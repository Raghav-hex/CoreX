# agent/agent.py (Professional, No-Admin-Required Version)

import asyncio
import websockets
import json
import psutil
import logging
import docker
import threading
import time
import platform
import subprocess

# --- Basic Setup & Config ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("agent")
AGENT_ID = "agent-003" # Change for each agent
SCHEDULER_URL = "ws://localhost:8000/agents/connect"
docker_client = docker.from_env()

# --- [NEW] Cross-Platform Idle Time Checker (No Admin Required) ---
def get_idle_time_seconds():
    """Gets the system idle time in seconds. Does not require admin rights."""
    system = platform.system()
    try:
        if system == "Windows":
            # Uses Windows-specific calls via a PowerShell command
            # This is a bit of a hack but avoids needing pywin32 directly in the logic
            from ctypes import Structure, windll, c_uint, sizeof, byref
            class LASTINPUTINFO(Structure):
                _fields_ = [('cbSize', c_uint), ('dwTime', c_uint)]
            lii = LASTINPUTINFO()
            lii.cbSize = sizeof(lii)
            windll.user32.GetLastInputInfo(byref(lii))
            millis = windll.kernel32.GetTickCount() - lii.dwTime
            return millis / 1000.0
        elif system == "Linux" or system == "Darwin": # Darwin is macOS
            # Uses the 'xprintidle' utility, which is the standard for X11 environments
            output = subprocess.check_output(['xprintidle']).strip()
            return int(output) / 1000.0
    except Exception as e:
        # If we can't get idle time, we default to a safe value (0, meaning "active")
        logger.warning(f"Could not get idle time on {system}: {e}. Defaulting to active.")
        return 0
    return 0

# --- [NEW] Hybrid Preemption State Machine (Simpler & More Reliable) ---
class ActivityMonitor:
    def __init__(self, websocket):
        self.websocket = websocket
        self.is_owner_active = False # The current state
        self.preemption_threshold_seconds = 300  # Preempt if owner is active for 5 minutes
        self.return_to_service_idle_seconds = 900 # Return to service after 15 minutes of inactivity

        self.current_task_container = None
        self.current_task_id = None
        
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)

    def start(self):
        self.monitor_thread.start()
        logger.info("Activity monitor started (using OS idle time).")

    def set_task(self, container, task_id):
        self.current_task_container = container
        self.current_task_id = task_id

    def clear_task(self):
        self.current_task_container = None
        self.current_task_id = None

    def _monitor_loop(self):
        while True:
            idle_time = get_idle_time_seconds()

            # --- State Logic ---
            # If the user has been idle for a long time, we are definitely available for work.
            if idle_time > self.return_to_service_idle_seconds and self.is_owner_active:
                logger.info(f"Machine has been idle for {idle_time:.0f}s. Returning to service.")
                self.is_owner_active = False

            # If the user has been active recently, we consider them active.
            elif idle_time < self.preemption_threshold_seconds and not self.is_owner_active:
                logger.warning(f"Owner has become active (idle time {idle_time:.0f}s). Machine is now considered in use.")
                self.is_owner_active = True

            # --- Action Logic ---
            # If we are in the "active" state and a task is running, preempt it.
            if self.is_owner_active and self.current_task_id:
                logger.warning(f"Preempting task '{self.current_task_id}' because owner is active.")
                try:
                    self.current_task_container.stop()
                    preempted_task_id = self.current_task_id
                    self.clear_task() # Clear state immediately
                    asyncio.run(self.websocket.send(json.dumps({
                        "type": "task_preempted", "task_id": preempted_task_id
                    })))
                except Exception as e:
                    logger.error(f"Error while stopping container for preemption: {e}")

            time.sleep(15) # Check the idle time every 15 seconds

# --- Task Execution & Main Loop (No changes from the previous 'pynput' version) ---
def execute_task(task_id: str, image: str, command: str, websocket, activity_monitor):
    # This function is identical to the last version.
    logger.info(f"Starting task '{task_id}'...")
    container = None
    try:
        container = docker_client.containers.run(image=image, command=command, detach=True, remove=True)
        activity_monitor.set_task(container, task_id)
        result = container.wait()
        logs = container.logs().decode('utf-8')
        logger.info(f"Task '{task_id}' completed normally.")
        asyncio.run(websocket.send(json.dumps({
            "type": "task_result", "task_id": task_id, "result": {"status": "success", "logs": logs.strip()}
        })))
    except docker.errors.NotFound:
        logger.warning(f"Container for task '{task_id}' stopped (preempted).")
    except Exception as e:
        logger.error(f"Error during task '{task_id}': {e}")
        asyncio.run(websocket.send(json.dumps({
            "type": "task_result", "task_id": task_id, "result": {"status": "error", "logs": str(e)}
        })))
    finally:
        activity_monitor.clear_task()

async def agent_main_loop():
    # This function is identical to the last version.
    activity_monitor = None
    while True:
        try:
            async with websockets.connect(SCHEDULER_URL) as websocket:
                logger.info("Connected to scheduler.")
                activity_monitor = ActivityMonitor(websocket)
                activity_monitor.start()
                await websocket.send(json.dumps({
                    "type": "register", "agent_id": AGENT_ID,
                    "resources": {"cpu_cores": psutil.cpu_count()}
                }))
                await websocket.recv()
                async for message_str in websocket:
                    message = json.loads(message_str)
                    if message.get("type") == "task_assignment":
                        if not activity_monitor.is_owner_active:
                            task_id = message.get("task_id")
                            logger.info(f"Accepted task: {task_id}")
                            task_thread = threading.Thread(
                                target=execute_task,
                                args=(task_id, message["image"], message["command"], websocket, activity_monitor)
                            )
                            task_thread.start()
                        else:
                            logger.warning(f"Owner is active. Rejecting task assignment.")
        except Exception as e:
            logger.error(f"Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    # No admin rights needed anymore!
    asyncio.run(agent_main_loop())