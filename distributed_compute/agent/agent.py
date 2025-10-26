# agent/agent.py (With Preemption Logic)

import asyncio
import websockets
import json
import psutil
import logging
import docker
import threading # [NEW] To run the Docker task in the background
import keyboard  # [NEW] To detect user activity

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("agent")

# --- Configuration ---
AGENT_ID = "agent-002" # Remember to change this to "agent-002" for your second agent
SCHEDULER_URL = "ws://localhost:8000/agents/connect"
docker_client = docker.from_env()

# --- [NEW] Global state to track the current task ---
current_task_container = None
current_task_id = None

# --- [UPDATED] Task Execution Logic ---
def execute_task(task_id: str, image: str, command: str, websocket):
    global current_task_container, current_task_id
    
    logger.info(f"Executing task '{task_id}': Running '{command}' in image '{image}'")
    try:
        current_task_id = task_id
        container = docker_client.containers.run(
            image=image,
            command=command,
            detach=True, # [UPDATED] Detach to run in the background
            remove=True
        )
        current_task_container = container
        
        # Wait for the container to finish
        result = container.wait()
        logs = container.logs().decode('utf-8')
        
        # If we reach here, the task completed normally (wasn't preempted)
        logger.info(f"Task '{task_id}' completed successfully.")
        
        # Send result back to scheduler in a thread-safe way
        asyncio.run(websocket.send(json.dumps({
            "type": "task_result", "task_id": task_id, "result": {"status": "success", "logs": logs.strip()}
        })))

    except docker.errors.NotFound:
        # This can happen if we preemptively stop the container
        logger.warning(f"Container for task '{task_id}' was stopped or removed before completion (likely preempted).")
    except Exception as e:
        logger.error(f"An unexpected error occurred during task '{task_id}': {e}")
        asyncio.run(websocket.send(json.dumps({
            "type": "task_result", "task_id": task_id, "result": {"status": "error", "logs": str(e)}
        })))
    finally:
        # Clear the global state once the task is done
        current_task_container = None
        current_task_id = None

# --- [NEW] Owner Activity Detection ---
def listen_for_preemption(websocket):
    logger.info("Listening for owner activity (press ANY key to preempt)...")
    keyboard.read_key(suppress=True) # This will block until a key is pressed
    
    global current_task_container, current_task_id
    if current_task_container:
        logger.warning(f"OWNER ACTIVITY DETECTED! Preempting task '{current_task_id}'.")
        try:
            current_task_container.stop()
            # Send a preemption message to the scheduler
            asyncio.run(websocket.send(json.dumps({
                "type": "task_preempted", "task_id": current_task_id
            })))
        except Exception as e:
            logger.error(f"Error while stopping container: {e}")
    else:
        logger.info("Owner activity detected, but no task is currently running.")

# --- [UPDATED] Main Agent Loop ---
async def agent_main_loop():
    while True:
        try:
            async with websockets.connect(SCHEDULER_URL) as websocket:
                logger.info("Connected to scheduler.")

                # Register with scheduler
                await websocket.send(json.dumps({
                    "type": "register", "agent_id": AGENT_ID,
                    "resources": {"cpu_cores": psutil.cpu_count()}
                }))
                await websocket.recv() # Wait for ack

                # Main loop to listen for task assignments
                async for message_str in websocket:
                    message = json.loads(message_str)
                    if message.get("type") == "task_assignment":
                        task_id = message.get("task_id")
                        logger.info(f"Received task assignment: {task_id}")
                        
                        # Start the Docker task in a separate thread
                        task_thread = threading.Thread(
                            target=execute_task,
                            args=(task_id, message["image"], message["command"], websocket)
                        )
                        task_thread.start()
                        
                        # Immediately start listening for preemption in another thread
                        preemption_thread = threading.Thread(
                            target=listen_for_preemption, args=(websocket,)
                        )
                        preemption_thread.start()
        
        except Exception as e:
            logger.error(f"An error occurred: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(agent_main_loop())