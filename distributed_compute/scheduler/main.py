# scheduler/main.py (With Credit System & Bug Fix)

import logging
import asyncio
import json
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Gauge

import database
from database import SessionLocal

# --- Setup & Clients ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("scheduler")
database.create_db_and_tables()
app = FastAPI()
redis_client = redis.Redis.from_url("redis://redis:6379", decode_responses=True)

# --- Prometheus ---
Instrumentator().instrument(app).expose(app, endpoint="/metrics", should_gzip=True)
CONNECTED_AGENTS_GAUGE = Gauge("connected_agents", "Number of currently connected agents")
QUEUED_JOBS_GAUGE = Gauge("queued_jobs", "Number of jobs currently in the Redis queue")

# --- In-Memory State ---
connected_agents = {}

# --- DB Dependency ---
def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

# --- Models ---
class JobRequest(BaseModel):
    command: str
    image: str = "alpine:latest"

# --- API ---
@app.post("/jobs")
async def submit_job(job_req: JobRequest, db: Session = Depends(get_db)):
    job_id = f"job-{await redis_client.incr('job_counter')}"
    new_job = database.Job(job_id=job_id, image=job_req.image, command=job_req.command, status="queued")
    db.add(new_job)
    db.commit()
    await redis_client.lpush("job_queue", job_id)
    logger.info(f"Saved and queued job '{job_id}'")
    return {"job_id": job_id, "status": "Job successfully queued"}

# --- Background Workers ---
async def schedule_jobs_periodically():
    logger.info("Starting job scheduler worker...")
    db = SessionLocal()
    while True:
        try:
            _ , job_id = await redis_client.brpop("job_queue")
            logger.info(f"Popped job '{job_id}'. Looking for a fair agent.")
            job = db.query(database.Job).filter(database.Job.job_id == job_id).first()
            if not job:
                logger.error(f"Job '{job_id}' not in DB. Skipping.")
                continue

            # --- [FIXED] Fair Scheduling Logic ---
            idle_agents_ids = [
                agent_id for agent_id, agent_info in connected_agents.items()
                if agent_info["status"] == "idle"
            ]

            if not idle_agents_ids:
                logger.warning(f"No IDLE agents available. Re-queuing job '{job_id}'.")
                await redis_client.lpush("job_queue", job_id)
                await asyncio.sleep(2)
                continue

            agent_db_records = db.query(database.Agent).filter(database.Agent.agent_id.in_(idle_agents_ids)).all()
            
            agent_to_use = min(agent_db_records, key=lambda agent: agent.total_credits_earned, default=None)

            # This 'if' block now correctly handles the case where no agent is found
            if agent_to_use:
                assigned_agent_id = agent_to_use.agent_id
                agent_websocket = connected_agents[assigned_agent_id]["websocket"]
                
                task_data = { "type": "task_assignment", "task_id": job.job_id, "image": job.image, "command": job.command }
                await agent_websocket.send_json(task_data)
                
                connected_agents[assigned_agent_id]["status"] = "busy"
                job.status = "assigned"
                job.assigned_agent_id = assigned_agent_id
                db.commit()
                logger.info(f"Assigned task '{job_id}' to agent '{assigned_agent_id}' (Credits: {agent_to_use.total_credits_earned})")
            else:
                # This case handles if idle agents are connected but not yet in the DB.
                logger.error("Could not find a valid agent in the database. Re-queuing.")
                await redis_client.lpush("job_queue", job_id)
                await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Error in scheduler worker: {e}")
            db.rollback()
            await asyncio.sleep(5)

async def update_queue_metrics():
    while True:
        try:
            queue_length = await redis_client.llen("job_queue")
            QUEUED_JOBS_GAUGE.set(queue_length)
        except Exception as e: logger.error(f"Error updating queue metrics: {e}")
        await asyncio.sleep(10)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_jobs_periodically())
    asyncio.create_task(update_queue_metrics())

# --- WebSocket Endpoint ---
@app.websocket("/agents/connect")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    agent_id = None
    db = SessionLocal()
    try:
        registration_message = await websocket.receive_json()
        if registration_message.get("type") == "register":
            agent_id = registration_message.get("agent_id")
            agent_record = db.query(database.Agent).filter(database.Agent.agent_id == agent_id).first()
            if not agent_record:
                logger.info(f"First time seeing agent '{agent_id}'. Creating DB record.")
                agent_record = database.Agent(
                    agent_id=agent_id,
                    cpu_cores=registration_message.get("resources", {}).get("cpu_cores", 0)
                )
                db.add(agent_record)
                db.commit()

            connected_agents[agent_id] = {"websocket": websocket, "status": "idle"}
            CONNECTED_AGENTS_GAUGE.set(len(connected_agents))
            logger.info(f"Agent '{agent_id}' connected. Total: {len(connected_agents)}")
            await websocket.send_json({"status": "success"})
        else:
            await websocket.close()
            return
        
        while True:
            message = await websocket.receive_json()
            if message.get("type") == "task_result":
                task_id = message.get("task_id")
                job = db.query(database.Job).filter(database.Job.job_id == task_id).first()
                if job and job.assigned_agent_id:
                    job.status = "completed"
                    job.result = message.get("result", {}).get("logs", "")
                    credits_to_award = 10.0
                    agent_to_reward = db.query(database.Agent).filter(database.Agent.agent_id == job.assigned_agent_id).first()
                    agent_to_reward.total_credits_earned += credits_to_award
                    new_tx = database.CreditTransaction(agent_id=agent_to_reward.agent_id, job_id=job.job_id, credits_awarded=credits_to_award)
                    db.add(new_tx)
                    db.commit()
                    if job.assigned_agent_id in connected_agents:
                        connected_agents[job.assigned_agent_id]["status"] = "idle"
                    logger.info(f"Awarded {credits_to_award} credits to '{agent_to_reward.agent_id}' for job '{task_id}'.")
                else:
                    logger.warning(f"Received result for unknown/unassigned task '{task_id}'")

    except WebSocketDisconnect: pass
    finally:
        if agent_id and agent_id in connected_agents:
            del connected_agents[agent_id]
            CONNECTED_AGENTS_GAUGE.set(len(connected_agents))
            logger.info(f"Agent '{agent_id}' disconnected. Total: {len(connected_agents)}")
        db.close()