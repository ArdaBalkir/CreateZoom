import asyncio
import aiohttp
import aiofiles
import os
from fastapi import FastAPI, HTTPException
import logging
import sys
from aiofiles.os import makedirs
import redis
from dotenv import load_dotenv
import json

load_dotenv()
# TODO - Get everything from the environment variables

# Configuration
IO_PORT = int(os.environ.get("IO_PORT", 8001))
PROCESS_WORKER_URL = os.environ.get("PROCESS_WORKER_URL", "http://process-worker:8002")
API_URL = os.environ.get("API_URL", "http://main-api:8000")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
DATA_ROOT = "/data"
DOWNLOADS_DIR = os.path.join(DATA_ROOT, "downloads")
CHUNK_SIZE = 64 * 1024 * 1024
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(
    total=3600,  # 1 hour total timeout
    connect=60,
    sock_connect=60,
    sock_read=300,
)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Redis Connection
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

async def update_task_status(task_id: str, updates: dict):
    """Updates the task status in Redis."""
    try:
        task_data = redis_client.get(task_id)
        if task_data:
            task_data = json.loads(task_data)
            task_data.update(updates)
            redis_client.set(task_id, json.dumps(task_data))
        else:
            logger.warning(f"Task {task_id} not found in Redis")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        raise HTTPException(status_code=500, detail="Failed to connect to Redis")
    except Exception as e:
        logger.error(f"Failed to update task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update task: {e}")

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


async def get_download(path: str, token: str, task_id: str):
    """Downloads a file from EBrains."""
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{path}?redirect=false"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                url, headers=headers, timeout=DOWNLOAD_TIMEOUT
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    download_url = data.get("url")
                    if not download_url:
                        raise Exception("Download URL not provided in response")

                    await makedirs(DOWNLOADS_DIR, exist_ok=True)
                    filename = os.path.basename(path)
                    filepath = os.path.join(DOWNLOADS_DIR, filename)

                    async with session.get(
                        download_url, timeout=DOWNLOAD_TIMEOUT
                    ) as download_response:
                        if download_response.status == 200:
                            total_size = int(
                                download_response.headers.get("content-length", 0)
                            )
                            downloaded_size = 0
                            logger.info(
                                f"Starting download of {filename} ({total_size} bytes)"
                            )

                            async with aiofiles.open(filepath, "wb") as file:
                                try:
                                    async for (
                                        chunk
                                    ) in download_response.content.iter_chunked(
                                        CHUNK_SIZE
                                    ):
                                        await file.write(chunk)
                                        downloaded_size += len(chunk)
                                        progress = (downloaded_size / total_size) * 25
                                        await update_task_status(task_id, {"progress": progress})
                                        logger.debug(
                                            f"Downloaded {downloaded_size}/{total_size} bytes"
                                        )
                                except asyncio.TimeoutError:
                                    logger.error(
                                        f"Timeout while downloading chunks for task {task_id}"
                                    )
                                    raise

                            logger.info(f"Download completed for task {task_id}")
                            return filepath
                        else:
                            raise Exception(
                                f"Failed to download file. Status code: {download_response.status}"
                            )
                else:
                    raise Exception(
                        f"Failed to get download URL. Status code: {response.status}"
                    )
        except asyncio.TimeoutError:
            logger.error(f"Timeout during download operation for task {task_id}")
            raise
        except Exception as e:
            logger.error(f"Download failed for task {task_id}: {str(e)}")
            raise


@app.post("/process")
async def process_task(task_data: dict):
    task_id = task_data["task_id"]
    path = task_data["path"]
    token = task_data["token"]
    try:
        await update_task_status(task_id, {"status": "downloading", "current_step": "downloading", "progress": 0})
        download_path = await get_download(path, token, task_id)
        task_data["download_path"] = download_path
        await update_task_status(task_id, {"status": "processing", "current_step": "downloaded", "progress": 25})
         # Submit task to Processing worker
        try:
            asyncio.create_task(submit_task_to_worker(PROCESS_WORKER_URL, task_data))
            logger.info(f"Submitted task {task_id} to processing worker")
        except HTTPException as e:
            await update_task_status(task_id, {"status": "failed", "error": str(e)})
            raise
        return {"status": "downloaded", "download_path": download_path}

    except Exception as e:
        await update_task_status(task_id, {"status": "failed", "error": str(e)})
        raise HTTPException(status_code=500, detail=str(e))