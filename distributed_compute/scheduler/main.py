# scheduler/main.py

import logging
import asyncio
import json
import redis.asyncio as redis # [UPDATED] Use the async version of the redis library
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("scheduler")
app = FastAPI()

# --- Redis Connection ---
# [NEW] Connect to the Redis server. "redis" is the hostname we defined in docker-compose.yml.
redis_client = redis.Redis.from_url("redis://redis:6379", decode_responses=True)

# --- In-Memory State ---
active_connections = {}

# --- Data Models ---
class Job(BaseModel):
    command: str
    image: str = "alpine:latest"

# --- [UPDATED] Job Submission Endpoint ---
# This endpoint now just adds the job to the Redis queue.
@app.post("/jobs")
async def submit_job(job: Job):
    job_id = f"job-{await redis_client.incr('job_counter')}"
    job_data = {
        "task_id": job_id,
        "image": job.image,
        "command": job.command
    }
    
    # 'lpush' adds the job to the left side of a list (our queue)
    await redis_client.lpush("job_queue", json.dumps(job_data))
    
    logger.info(f"Queued job '{job_id}'")
    return {"job_id": job_id, "status": "Job successfully queued"}

# --- [NEW] Background Worker for Scheduling ---
async def schedule_jobs_periodically():
    logger.info("Starting job scheduler worker...")
    while True:
        try:
            # 'brpop' is a blocking pop. It waits until a job is available in the queue.
            # This is very efficient as it doesn't constantly poll.
            _ , job_data_str = await redis_client.brpop("job_queue")
            job_data = json.loads(job_data_str)
            job_id = job_data["task_id"]
            
            logger.info(f"Popped job '{job_id}' from queue. Looking for an agent.")

            if not active_connections:
                logger.warning(f"No agents available for job '{job_id}'. Re-queuing job.")
                # Re-queue the job if no agents are online
                await redis_client.lpush("job_queue", json.dumps(job_data))
                await asyncio.sleep(5) # Wait a bit before checking again
                continue

            # Simple scheduling: assign to the first available agent
            agent_id_to_use = next(iter(active_connections))
            agent_websocket = active_connections[agent_id_to_use]

            task_data = { "type": "task_assignment", **job_data }
            
            await agent_websocket.send_json(task_data)
            logger.info(f"Assigned task '{job_id}' to agent '{agent_id_to_use}'")

        except Exception as e:
            logger.error(f"Error in scheduler worker: {e}")
            await asyncio.sleep(5) # Avoid rapid-fire loops on persistent errors

# --- [NEW] FastAPI Startup Event ---
# This tells FastAPI to run our background worker when the application starts.
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(schedule_jobs_periodically())

# --- WebSocket Endpoint (no changes needed here) ---
@app.websocket("/agents/connect")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    agent_id = None
    try:
        registration_message = await websocket.receive_json()
        if registration_message.get("type") == "register":
            agent_id = registration_message.get("agent_id")
            if agent_id:
                active_connections[agent_id] = websocket
                logger.info(f"Agent '{agent_id}' connected. Total agents: {len(active_connections)}")
                await websocket.send_json({"status": "success"})
            else:
                await websocket.close(code=1008)
                return
        else:
            await websocket.close(code=1008)
            return
        
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")
            if msg_type == "task_result":
                task_id = message.get("task_id")
                result = message.get("result")
                logger.info(f"Received result for task '{task_id}': {result['logs']}")

    except WebSocketDisconnect:
        if agent_id and agent_id in active_connections:
            del active_connections[agent_id]
            logger.info(f"Agent '{agent_id}' disconnected. Total agents: {len(active_connections)}")
    except Exception as e:
        logger.error(f"An error occurred with agent '{agent_id}': {e}")
        if agent_id and agent_id in active_connections:
            del active_connections[agent_id]