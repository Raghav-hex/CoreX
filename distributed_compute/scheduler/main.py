# scheduler/main.py

import logging
import asyncio
import json
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

import database
from database import SessionLocal

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("scheduler")
database.create_db_and_tables()
app = FastAPI()
redis_client = redis.Redis.from_url("redis://redis:6379", decode_responses=True)

# --- [UPDATED] Agent Tracking ---
# We now store more than just the websocket; we also store the agent's status.
connected_agents = {} # agent_id -> {"websocket": websocket, "status": "idle" | "busy"}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class JobRequest(BaseModel):
    command: str
    image: str = "alpine:latest"

@app.post("/jobs")
async def submit_job(job_req: JobRequest, db: Session = Depends(get_db)):
    job_id = f"job-{await redis_client.incr('job_counter')}"
    new_job = database.Job(
        job_id=job_id,
        image=job_req.image,
        command=job_req.command,
        status="queued"
    )
    db.add(new_job)
    db.commit()
    await redis_client.lpush("job_queue", job_id)
    logger.info(f"Saved and queued job '{job_id}'")
    return {"job_id": job_id, "status": "Job successfully queued"}

# --- [UPDATED] The Core Scheduling Logic ---
async def schedule_jobs_periodically():
    logger.info("Starting job scheduler worker...")
    db = SessionLocal()
    while True:
        try:
            _ , job_id = await redis_client.brpop("job_queue")
            logger.info(f"Popped job '{job_id}'. Looking for an IDLE agent.")

            job = db.query(database.Job).filter(database.Job.job_id == job_id).first()
            if not job:
                logger.error(f"Job '{job_id}' not in DB. Skipping.")
                continue

            # [NEW] Find an idle agent instead of just the first one
            assigned_agent_id = None
            for agent_id, agent_info in connected_agents.items():
                if agent_info["status"] == "idle":
                    agent_websocket = agent_info["websocket"]
                    task_data = { "type": "task_assignment", "task_id": job.job_id, "image": job.image, "command": job.command }
                    
                    await agent_websocket.send_json(task_data)
                    
                    # Mark the agent as busy and update the job status
                    agent_info["status"] = "busy"
                    job.status = "assigned"
                    db.commit()
                    
                    assigned_agent_id = agent_id
                    logger.info(f"Assigned task '{job_id}' to agent '{assigned_agent_id}'")
                    break # Exit the loop once we've assigned the job

            # [NEW] If no idle agents were found, re-queue the job
            if not assigned_agent_id:
                logger.warning(f"No IDLE agents available. Re-queuing job '{job_id}'.")
                await redis_client.lpush("job_queue", job_id)
                await asyncio.sleep(2) # Wait a bit before checking the queue again

        except Exception as e:
            logger.error(f"Error in scheduler worker: {e}")
            db.rollback()
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_jobs_periodically())

# --- [UPDATED] WebSocket Endpoint ---
@app.websocket("/agents/connect")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    agent_id = None
    db = SessionLocal()
    try:
        registration_message = await websocket.receive_json()
        if registration_message.get("type") == "register":
            agent_id = registration_message.get("agent_id")
            # [UPDATED] Store the agent with its initial 'idle' status
            connected_agents[agent_id] = {"websocket": websocket, "status": "idle"}
            logger.info(f"Agent '{agent_id}' connected. Status: idle.")
            await websocket.send_json({"status": "success"})
        else:
            await websocket.close()
            return
        
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")
            if msg_type == "task_result":
                task_id = message.get("task_id")
                result = message.get("result")
                
                # [UPDATED] When a task finishes, set the agent back to 'idle'
                job_to_update = db.query(database.Job).filter(database.Job.job_id == task_id).first()
                if job_to_update:
                    job_to_update.status = "completed"
                    job_to_update.result = result.get("logs", "")
                    db.commit()
                    
                    # Find which agent did the job and mark it as idle
                    for id, info in connected_agents.items():
                        if info["websocket"] == websocket:
                            info["status"] = "idle"
                            logger.info(f"Agent '{id}' finished task '{task_id}' and is now IDLE.")
                            break
                else:
                    logger.warning(f"Received result for unknown task '{task_id}'")

    except WebSocketDisconnect:
        pass
    finally:
        if agent_id and agent_id in connected_agents:
            del connected_agents[agent_id]
            logger.info(f"Agent '{agent_id}' disconnected.")
        db.close()