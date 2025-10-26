# agent/agent.py

import asyncio
import websockets
import json
import psutil
import logging
import docker # NEW: To interact with the Docker daemon

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("agent")

# --- Configuration ---
AGENT_ID = "agent-002"
SCHEDULER_URL = "ws://localhost:8000/agents/connect"
# NEW: Initialize the Docker client
docker_client = docker.from_env()

# --- NEW: Task Execution Logic ---
def execute_task(task_id: str, image: str, command: str):
    """Runs a command inside a new Docker container and returns the logs."""
    logger.info(f"Executing task '{task_id}': Running '{command}' in image '{image}'")
    try:
        # Pull the image to make sure it's available.
        # In a real system, you might handle this more gracefully.
        logger.info(f"Pulling Docker image: {image}...")
        docker_client.images.pull(image)
        
        # Run the container. This is the core of the execution.
        container = docker_client.containers.run(
            image=image,
            command=command,
            detach=False, # We set detach=False to wait for it to complete
            remove=True    # Automatically remove the container when it's done
        )

        # The result is the container's log output.
        # We decode it from bytes to a string.
        result_logs = container.decode('utf-8')
        logger.info(f"Task '{task_id}' completed successfully.")
        return {"status": "success", "logs": result_logs.strip()}
        
    except docker.errors.ContainerError as e:
        logger.error(f"Task '{task_id}' failed with a container error: {e}")
        return {"status": "error", "logs": str(e)}
    except docker.errors.ImageNotFound:
        logger.error(f"Task '{task_id}' failed because image '{image}' was not found.")
        return {"status": "error", "logs": f"Image not found: {image}"}
    except Exception as e:
        logger.error(f"An unexpected error occurred during task '{task_id}': {e}")
        return {"status": "error", "logs": str(e)}

# --- UPDATED: Main Agent Loop ---
async def agent_main_loop():
    while True:
        try:
            async with websockets.connect(SCHEDULER_URL) as websocket:
                logger.info(f"Connected to scheduler.")

                # Register with the scheduler
                await websocket.send(json.dumps({
                    "type": "register",
                    "agent_id": AGENT_ID,
                    "resources": {"cpu_cores": psutil.cpu_count()}
                }))
                response = await websocket.recv()
                logger.info(f"Scheduler response: {response}")

                # Listen for messages from the scheduler (tasks, etc.)
                async for message_str in websocket:
                    message = json.loads(message_str)
                    msg_type = message.get("type")

                    if msg_type == "task_assignment":
                        task_id = message.get("task_id")
                        logger.info(f"Received task assignment: {task_id}")
                        
                        # Execute the task
                        result = execute_task(task_id, message["image"], message["command"])
                        
                        # Send the result back to the scheduler
                        await websocket.send(json.dumps({
                            "type": "task_result",
                            "task_id": task_id,
                            "result": result
                        }))
                    else:
                        logger.warning(f"Received unknown message type: {msg_type}")
        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("Connection lost. Reconnecting in 5 seconds...")
        except Exception as e:
            logger.error(f"An error occurred: {e}. Reconnecting in 5 seconds...")
        
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(agent_main_loop())