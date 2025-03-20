from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uuid
from typing import Dict, Optional
import logging
import sys
import os
import aiohttp
import asyncio
import redis
from dotenv import load_dotenv
import json


# Sketchy module
load_dotenv()

# Configuration
API_PORT = int(os.environ.get("API_PORT", 8000))
IO_WORKER_URL = os.environ.get("IO_WORKER_URL", "http://io-worker:8001")
PROCESS_WORKER_URL = os.environ.get("PROCESS_WORKER_URL", "http://process-worker:8002")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379)) # Use this port as default

# Logging setup
# The append-only file is not used due to storage limitations for logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

app = FastAPI()

# TODO - Change origins to the actual frontend URL for production code
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis Connection
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# Task Store using Redis
class RedisTaskStore:
    def __init__(self, redis_client):
        self.redis_client = redis_client

    def add_task(self, task_id: str, task_data: dict):
        self.redis_client.set(task_id, json.dumps(task_data))

    def update_task(self, task_id: str, updates: dict):
        task_data = self.get_task(task_id)
        if task_data:
            task_data.update(updates)
            self.redis_client.set(task_id, json.dumps(task_data))

    def get_task(self, task_id: str) -> Optional[dict]:
        task_data = self.redis_client.get(task_id)
        if task_data:
            return json.loads(task_data)
        return None

    def get_all_tasks(self) -> Dict[str, dict]:
        tasks = {}
        for key in self.redis_client.scan_iter():
            task_data = self.redis_client.get(key)
            if task_data:
                tasks[key] = json.loads(task_data)
        return tasks

task_store = RedisTaskStore(redis_client)

async def submit_task_to_worker(worker_url: str, task_data: dict):
    """Submits a task to a worker service using HTTP."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{worker_url}/process", json=task_data) as resp:
                resp.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                return await resp.json()
        except aiohttp.ClientError as e:
            logger.error(f"Error submitting task to {worker_url}: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/deepzoom/health")
async def health():
    return {"status": "I'm alive!"}


@app.post("/deepzoom", status_code=202)
async def deepzoom_endpoint(request: Request):
    try:
        data = await request.json()
        logger.info("Received deepzoom request")

        for param in ["path", "target_path", "token"]:
            if not data.get(param):
                logger.error(f"Missing parameter: {param}")
                raise HTTPException(
                    status_code=400, detail=f"Missing required parameter: {param}"
                )
            if not isinstance(data[param], str) or not data[param].strip():
                logger.error(f"Invalid parameter: {param}")
                raise HTTPException(
                    status_code=400, detail=f"{param} must be a non-empty string"
                )

        task_id = str(uuid.uuid4())
        task_data = {
            "task_id": task_id,
            "path": data["path"],
            "target_path": data["target_path"],
            "token": data["token"],
            "status": "pending",
            "current_step": "submitted",
            "progress": 0,
        }
        task_store.add_task(task_id, task_data)

        # Submit task to IO worker for download
        try:
            asyncio.create_task(submit_task_to_worker(IO_WORKER_URL, task_data))
            logger.info(f"Submitted task {task_id} to IO worker")
        except HTTPException as e:
            task_store.update_task(task_id, {"status": "failed", "error": str(e)})
            raise

        response = {
            "task_id": task_id,
            "status": "accepted",
            "status_endpoint": f"/deepzoom/status/{task_id}",
        }
        return response

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/deepzoom/status/{task_id}")
async def get_task_status(task_id: str):
    try:
        logger.debug(f"Checking status for task: {task_id}")
        task = task_store.get_task(task_id)
        if not task:
            logger.warning(f"Task not found: {task_id}")
            raise HTTPException(status_code=404, detail="Task not found")
        return task
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}", exc_info=True)
        raise

# Reporting related dashboard stuff
# Currently to provide internal access only
async def verify_ebrains_token(token: str) -> bool:
    """Verify token and check if email ends with @medisin.uio.no"""
    url = "https://iam.ebrains.eu/auth/realms/hbp/protocol/openid-connect/userinfo"
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url, headers={"Authorization": f"Bearer {token}"}
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("email", "").endswith("@medisin.uio.no")
    return False


@app.get("/deepzoom/tasks")
async def get_all_tasks(request: Request):
    try:
        # Get token from Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing or invalid token")

        token = auth_header.split(" ")[1]

        is_authorized = await verify_ebrains_token(token)
        if not is_authorized:
            raise HTTPException(status_code=403, detail="Unauthorized email domain")

        # task_manager.task_store.cleanup_old_tasks() # implement cleanup in task store

        return {
            "tasks": task_store.get_all_tasks(),
            "total": len(task_store.get_all_tasks()),
        }

    except Exception as e:
        logger.error(f"Error getting tasks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))