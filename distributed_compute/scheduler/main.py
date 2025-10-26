# scheduler/main.py

import logging
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel # NEW: For data validation

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scheduler")
app = FastAPI()

# --- In-Memory State (for our simple MVP) ---
active_connections = {} # Stores agent_id -> WebSocket
job_database = {}       # NEW: A simple dict to act as our job database

# --- NEW: Data Models for API ---
# This defines what a "Job" looks like when a user submits it.
# FastAPI will automatically validate the incoming data against this model.
class Job(BaseModel):
    command: str
    image: str = "alpine:latest" # Default to a small, common Docker image

# --- NEW: REST API Endpoint for Job Submission ---
@app.post("/jobs")
async def submit_job(job: Job):
    if not active_connections:
        raise HTTPException(status_code=503, detail="No agents are available to accept jobs.")

    # Find the first available (idle) agent. This is a very simple scheduling strategy.
    agent_id_to_use = next(iter(active_connections))
    agent_websocket = active_connections[agent_id_to_use]

    # Create a unique ID for the job
    job_id = f"job-{len(job_database) + 1}"
    job_database[job_id] = {"status": "assigned", "agent_id": agent_id_to_use}

    # The task message we will send to the agent
    task_data = {
        "type": "task_assignment",
        "task_id": job_id,
        "image": job.image,
        "command": job.command
    }

    try:
        await agent_websocket.send_json(task_data)
        logger.info(f"Assigned task '{job_id}' to agent '{agent_id_to_use}'")
        return {"job_id": job_id, "status": "Job assigned successfully"}
    except Exception as e:
        logger.error(f"Failed to send task to agent '{agent_id_to_use}': {e}")
        job_database[job_id]["status"] = "failed_to_assign"
        raise HTTPException(status_code=500, detail="Failed to communicate with agent.")


# --- UPDATED: WebSocket Endpoint ---
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
                await websocket.send_json({"status": "success", "message": "Registration successful"})
            else:
                await websocket.close(code=1008, reason="Agent ID is required")
                return
        else:
            await websocket.close(code=1008, reason="First message must be a registration message.")
            return

        # UPDATED: Listen for more than just heartbeats
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")
            
            if msg_type == "heartbeat":
                logger.info(f"Received heartbeat from '{agent_id}'")
            
            elif msg_type == "task_result":
                task_id = message.get("task_id")
                result = message.get("result")
                logger.info(f"Received result for task '{task_id}': {result}")
                if task_id in job_database:
                    job_database[task_id]["status"] = "completed"
                    job_database[task_id]["result"] = result
            else:
                logger.warning(f"Received unknown message type from '{agent_id}': {msg_type}")

    except WebSocketDisconnect:
        if agent_id and agent_id in active_connections:
            del active_connections[agent_id]
            logger.info(f"Agent '{agent_id}' disconnected. Total agents: {len(active_connections)}")
    except Exception as e:
        logger.error(f"An error occurred with agent '{agent_id}': {e}")
        if agent_id and agent_id in active_connections:
            del active_connections[agent_id]