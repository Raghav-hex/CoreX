# scheduler/main.py

import logging
import asyncio
import json
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

# [NEW] Import our database definitions
import database
from database import SessionLocal

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("scheduler")

# [NEW] Create the database table on startup
database.create_db_and_tables()

app = FastAPI()
redis_client = redis.Redis.from_url("redis://redis:6379", decode_responses=True)
active_connections = {}

# --- [NEW] Dependency for getting a DB session ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Data Models ---
class JobRequest(BaseModel):
    command: str
    image: str = "alpine:latest"

# --- [UPDATED] Job Submission Endpoint ---
@app.post("/jobs")
async def submit_job(job_req: JobRequest, db: Session = Depends(get_db)):
    # 1. Create a new Job record in the database
    job_id = f"job-{await redis_client.incr('job_counter')}"
    new_job = database.Job(
        job_id=job_id,
        image=job_req.image,
        command=job_req.command,
        status="queued"
    )
    db.add(new_job)
    db.commit()

    # 2. Push the job_id to the Redis queue for the worker to pick up
    await redis_client.lpush("job_queue", job_id)
    
    logger.info(f"Saved and queued job '{job_id}'")
    return {"job_id": job_id, "status": "Job successfully queued"}

# --- [UPDATED] Background Scheduler ---
async def schedule_jobs_periodically():
    logger.info("Starting job scheduler worker...")
    db = SessionLocal() # Get a DB session for the background worker
    while True:
        try:
            # Pop a job_id from the queue
            _ , job_id = await redis_client.brpop("job_queue")
            logger.info(f"Popped job '{job_id}' from queue. Looking for an agent.")

            # Find the job details from the database
            job = db.query(database.Job).filter(database.Job.job_id == job_id).first()
            if not job:
                logger.error(f"Job '{job_id}' found in queue but not in database. Skipping.")
                continue

            if not active_connections:
                logger.warning(f"No agents available. Re-queuing job '{job_id}'.")
                await redis_client.lpush("job_queue", job_id)
                await asyncio.sleep(5)
                continue

            agent_id_to_use = next(iter(active_connections))
            agent_websocket = active_connections[agent_id_to_use]

            task_data = {
                "type": "task_assignment",
                "task_id": job.job_id,
                "image": job.image,
                "command": job.command
            }
            
            await agent_websocket.send_json(task_data)
            
            # Update the job status in the database
            job.status = "assigned"
            db.commit()
            logger.info(f"Assigned task '{job_id}' to agent '{agent_id_to_use}'")

        except Exception as e:
            logger.error(f"Error in scheduler worker: {e}")
            db.rollback() # Rollback any failed DB transactions
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_jobs_periodically())

# --- [UPDATED] WebSocket Endpoint to include DB logic for results ---
@app.websocket("/agents/connect")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    agent_id = None
    db = SessionLocal() # Get a DB session for this connection
    try:
        # (Registration logic is the same)
        registration_message = await websocket.receive_json()
        if registration_message.get("type") == "register":
            agent_id = registration_message.get("agent_id")
            active_connections[agent_id] = websocket
            logger.info(f"Agent '{agent_id}' connected.")
            await websocket.send_json({"status": "success"})
        else:
            await websocket.close()
            return
        
        # Listen for task results
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")
            if msg_type == "task_result":
                task_id = message.get("task_id")
                result = message.get("result")
                
                # Find the job in the DB and update it
                job_to_update = db.query(database.Job).filter(database.Job.job_id == task_id).first()
                if job_to_update:
                    job_to_update.status = "completed"
                    job_to_update.result = result.get("logs", "No logs found.")
                    db.commit()
                    logger.info(f"Updated job '{task_id}' to completed in database.")
                else:
                    logger.warning(f"Received result for unknown task '{task_id}'")

    except WebSocketDisconnect:
        pass # The 'finally' block will handle cleanup
    finally:
        if agent_id and agent_id in active_connections:
            del active_connections[agent_id]
            logger.info(f"Agent '{agent_id}' disconnected.")
        db.close()