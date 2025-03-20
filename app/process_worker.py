import asyncio
import pyvips
import os
import zipfile
from io import BytesIO
import shutil
from fastapi import FastAPI, HTTPException
import logging
import sys
from aiofiles.os import remove, path
import aiofiles
import aiohttp
import redis
from dotenv import load_dotenv
import json

load_dotenv()

# Configuration
PROCESS_PORT = int(os.environ.get("PROCESS_PORT", 8002))
API_URL = os.environ.get("API_URL", "http://main-api:8000")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
DATA_ROOT = "/data"
OUTPUTS_DIR = os.path.join(DATA_ROOT, "outputs")

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

async def upload_zip(upload_path: str, zip_path: str, token: str):
    """Uploads a zip file to EBrains."""
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{upload_path}"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        # Get the upload URL
        async with session.put(url, headers=headers) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status, detail="Failed to get upload URL"
                )
            data = await response.json()
            upload_url = data.get("url")
            if not upload_url:
                raise HTTPException(
                    status_code=400, detail="Upload URL not provided in response"
                )

            print(f"Uploading to {upload_url}")
            async with aiofiles.open(zip_path, "rb") as file:
                file_data = await file.read()
                async with session.put(upload_url, data=file_data) as upload_response:
                    if upload_response.status == 201:
                        print(f"Created in {upload_path}")
                        return f"Created in {upload_path}"
                    else:
                        raise HTTPException(
                            status_code=upload_response.status,
                            detail="Failed to upload file",
                        )


async def deepzoom(path: str):
    """Creates a DeepZoom pyramid from the image file."""

    def process_image():
        logger.info(f"Creating DeepZoom pyramid for {path}")
        image = pyvips.Image.new_from_file(path)
        os.makedirs(OUTPUTS_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUTS_DIR, os.path.basename(path))
        image.dzsave(output_path)
        return output_path + ".dzi"

    return await asyncio.to_thread(process_image)


async def zip_pyramid(path: str):
    """Zips the pyramid files with a .dzip extension."""

    def create_zip():
        dzi_file = path
        dzi_dir = os.path.splitext(dzi_file)[0] + "_files"
        strip_file_name = os.path.basename(os.path.splitext(dzi_file)[0])
        zip_path = f"{os.path.dirname(dzi_file)}/{strip_file_name}.dzip"

        zip_buffer = BytesIO()
        with zipfile.ZipFile(
            zip_buffer, "w", zipfile.ZIP_STORED
        ) as zipf:  # Changed here to ZIP_STORED as the compression leve is 0 and our use case is different
            zipf.write(dzi_file, os.path.basename(dzi_file))
            for root, _, files in os.walk(dzi_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, os.path.dirname(dzi_dir))
                    zipf.write(file_path, arcname)

        with open(zip_path, "wb") as f:
            f.write(zip_buffer.getvalue())
        return zip_path

    return await asyncio.to_thread(create_zip)


async def cleanup_files(download_path: str, dzi_path: str, zip_path: str):
    """Removes temporary files after processing."""
    try:
        if await path.exists(download_path):
            await remove(download_path)

        if dzi_path and await path.exists(dzi_path):
            await remove(dzi_path)
            dzi_dir = os.path.splitext(dzi_path)[0] + "_files"
            if await path.exists(dzi_dir):
                await asyncio.to_thread(shutil.rmtree, dzi_dir)

        if zip_path and await path.exists(zip_path):
            await remove(zip_path)

    except Exception as e:
        print(f"Cleanup error: {str(e)}")


@app.post("/process")
async def process_task(task_data: dict):
    task_id = task_data["task_id"]
    download_path = task_data["download_path"]
    target_path = task_data["target_path"]
    token = task_data["token"]
    try:
        await update_task_status(task_id, {"current_step": "creating_deepzoom", "progress": 50})
        dzi_path = await deepzoom(download_path)

        await update_task_status(task_id, {"current_step": "compressing", "progress": 75})
        zip_path = await zip_pyramid(dzi_path)

        await update_task_status(task_id, {"current_step": "uploading", "progress": 90})
        upload_filename = os.path.basename(zip_path)
        result = await upload_zip(
            f"{target_path}/{upload_filename}", zip_path, token
        )

        await update_task_status(
            task_id,
            {
                "status": "completed",
                "current_step": "completed",
                "result": result,
                "progress": 100,
            },
        )
        logger.info(f"Task {task_id} completed successfully")

        await cleanup_files(download_path, dzi_path, zip_path)

        return {"status": "completed", "result": result}

    except Exception as e:
        logger.error(f"Task {task_id} failed: {str(e)}", exc_info=True)
        await update_task_status(
            task_id,
            {
                "status": "failed",
                "current_step": "failed",
                "error": str(e),
                "progress": 0,
            },
        )
        await cleanup_files(download_path, dzi_path if "dzi_path" in locals() else None, zip_path if "zip_path" in locals() else None)
        raise HTTPException(status_code=500, detail=str(e))