# scheduler/main.py (Final Corrected Version)

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

# --- Basic Setup & Global Clients ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("scheduler")

# This creates the tables if they don't exist
database.create_db_and_tables()

app = FastAPI()

# This line defines the Redis client at the global scope so all functions can see it.
redis_client = redis.Redis.from_url("redis://redis:6379", decode_responses=True)

# --- Prometheus Metrics Setup ---
Instrumentator().instrument(app).expose(app, endpoint="/metrics", should_gzip=True)
CONNECTED_AGENTS_GAUGE = Gauge("connected_agents", "Number of currently connected agents")
QUEUED_JOBS_GAUGE = Gauge("queued_jobs", "Number of jobs currently in the Redis queue")

# --- In-Memory State ---
connected_agents = {}

# --- Database Dependency ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Pydantic Models ---
class JobRequest(BaseModel):
    command: str
    image: str = "alpine:latest"

# --- API Endpoints ---
@app.post("/jobs")
async def submit_job(job_req: JobRequest, db: Session = Depends(get_db)):
    job_id = f"job-{await redis_client.incr('job_counter')}"
    new_job = database.Job(
        job_id=job_id, image=job_req.image, command=job_req.command, status="queued"
    )
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
            # This function can now see the global 'redis_client'
            _ , job_id = await redis_client.brpop("job_queue")
            logger.info(f"Popped job '{job_id}'. Looking for an IDLE agent.")
            job = db.query(database.Job).filter(database.Job.job_id == job_id).first()
            if not job:
                logger.error(f"Job '{job_id}' not in DB. Skipping.")
                continue
            assigned_agent_id = None
            for agent_id, agent_info in connected_agents.items():
                if agent_info["status"] == "idle":
                    agent_websocket = agent_info["websocket"]
                    task_data = { "type": "task_assignment", "task_id": job.job_id, "image": job.image, "command": job.command }
                    await agent_websocket.send_json(task_data)
                    agent_info["status"] = "busy"
                    job.status = "assigned"
                    db.commit()
                    assigned_agent_id = agent_id
                    logger.info(f"Assigned task '{job_id}' to agent '{assigned_agent_id}'")
                    break
            if not assigned_agent_id:
                logger.warning(f"No IDLE agents available. Re-queuing job '{job_id}'.")
                await redis_client.lpush("job_queue", job_id)
                await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Error in scheduler worker: {e}")
            db.rollback()
            await asyncio.sleep(5)

async def update_queue_metrics():
    while True:
        try:
            # This function can also see the global 'redis_client'
            queue_length = await redis_client.llen("job_queue")
            QUEUED_JOBS_GAUGE.set(queue_length)
        except Exception as e:
            logger.error(f"Error updating queue metrics: {e}")
        await asyncio.sleep(10)

# --- FastAPI Events ---
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
                result = message.get("result")
                job_to_update = db.query(database.Job).filter(database.Job.job_id == task_id).first()
                if job_to_update:
                    job_to_update.status = "completed"
                    job_to_update.result = result.get("logs", "")
                    db.commit()
                    if agent_id in connected_agents:
                        connected_agents[agent_id]["status"] = "idle"
                        logger.info(f"Agent '{agent_id}' finished task '{task_id}' and is now IDLE.")
                else:
                    logger.warning(f"Received result for unknown task '{task_id}'")
    except WebSocketDisconnect:
        pass
    finally:
        if agent_id and agent_id in connected_agents:
            del connected_agents[agent_id]
            CONNECTED_AGENTS_GAUGE.set(len(connected_agents))
            logger.info(f"Agent '{agent_id}' disconnected. Total: {len(connected_agents)}")
        db.close()