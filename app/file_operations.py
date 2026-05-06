"""
File operations for downloading and uploading files to EBRAINS data proxy.
"""
import os
import asyncio
import logging
import aiohttp
import aiofiles
from aiofiles.os import makedirs, remove, path
import shutil

logger = logging.getLogger(__name__)

# Constants
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(
    total=3600,  # 1 hour total timeout
    connect=60,  # 60 seconds connect timeout
    sock_connect=60,  # 60 seconds to establish connection
    sock_read=300,  # 5 minutes socket read timeout
)
CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunks

DATA_ROOT = "/data"
DOWNLOADS_DIR = os.path.join(DATA_ROOT, "downloads")
OUTPUTS_DIR = os.path.join(DATA_ROOT, "outputs")


async def download_file(
    path: str,
    token: str,
    task_id: str,
    task_store,
    user_info: dict
) -> str:
    """
    Download file from EBRAINS hosted bucket.
    
    Args:
        path: The bucket path to download from
        token: Authorization token
        task_id: The task identifier
        task_store: TaskStore instance for progress updates
        user_info: Dictionary containing user information
        
    Returns:
        Local file path of the downloaded file
    """
    user_display = f"{user_info.get('name') or user_info.get('username') or 'Unknown'}"
    user_email = user_info.get('email') or 'no-email'
    
    logger.info(
        "[DOWNLOAD START] Task: %s | User: %s (%s) | Source: %s",
        task_id, user_display, user_email, path
    )
    
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{path}?redirect=false"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        try:
            logger.debug(
                "[DOWNLOAD] Task: %s | Requesting download URL from: %s",
                task_id, url
            )
            
            async with session.get(
                url, headers=headers, timeout=DOWNLOAD_TIMEOUT
            ) as response:
                logger.info(
                    "[DOWNLOAD] Task: %s | Initial response status: %d",
                    task_id, response.status
                )
                
                if response.status == 200:
                    data = await response.json()
                    download_url = data.get("url")
                    
                    if not download_url:
                        logger.error(
                            "[DOWNLOAD FAILED] Task: %s | User: %s | No download URL in response",
                            task_id, user_display
                        )
                        raise Exception("Download URL not provided in response")

                    logger.debug(
                        "[DOWNLOAD] Task: %s | Got redirect URL, starting actual download",
                        task_id
                    )

                    await makedirs(DOWNLOADS_DIR, exist_ok=True)
                    filename = os.path.basename(path)
                    filepath = os.path.join(DOWNLOADS_DIR, filename)

                    async with session.get(
                        download_url, timeout=DOWNLOAD_TIMEOUT
                    ) as download_response:
                        logger.info(
                            "[DOWNLOAD] Task: %s | Download response status: %d",
                            task_id, download_response.status
                        )
                        
                        if download_response.status == 200:
                            total_size = int(
                                download_response.headers.get("content-length", 0)
                            )
                            downloaded_size = 0
                            
                            logger.info(
                                "[DOWNLOAD] Task: %s | User: %s | File: %s | Size: %s bytes (%.2f MB)",
                                task_id, user_display, filename, total_size, total_size / (1024 * 1024)
                            )

                            async with aiofiles.open(filepath, "wb") as file:
                                chunk_count = 0
                                try:
                                    async for chunk in download_response.content.iter_chunked(CHUNK_SIZE):
                                        await file.write(chunk)
                                        downloaded_size += len(chunk)
                                        chunk_count += 1
                                        progress = (
                                            int((downloaded_size / total_size) * 25)
                                            if total_size
                                            else 0
                                        )
                                        task_store.update_task(task_id, {"progress": progress})
                                        
                                        # Log every 500 chunks or at completion
                                        if chunk_count % 500 == 0 or downloaded_size == total_size:
                                            logger.info(
                                                "[DOWNLOAD PROGRESS] Task: %s | %d/%d bytes (%.1f%%) | Chunks: %d",
                                                task_id, downloaded_size, total_size,
                                                (downloaded_size / total_size * 100) if total_size else 0,
                                                chunk_count
                                            )
                                            
                                except asyncio.TimeoutError:
                                    logger.error(
                                        "[DOWNLOAD TIMEOUT] Task: %s | User: %s | Downloaded: %d/%d bytes before timeout",
                                        task_id, user_display, downloaded_size, total_size
                                    )
                                    raise

                            logger.info(
                                "[DOWNLOAD COMPLETE] Task: %s | User: %s | File: %s | Total: %d bytes | Chunks: %d",
                                task_id, user_display, filename, downloaded_size, chunk_count
                            )
                            return os.path.relpath(filepath)
                        else:
                            logger.error(
                                "[DOWNLOAD FAILED] Task: %s | User: %s | Status: %d | File: %s",
                                task_id, user_display, download_response.status, filename
                            )
                            raise Exception(
                                f"Failed to download file. Status code: {download_response.status}"
                            )
                else:
                    logger.error(
                        "[DOWNLOAD FAILED] Task: %s | User: %s | Failed to get URL | Status: %d",
                        task_id, user_display, response.status
                    )
                    raise Exception(
                        f"Failed to get download URL. Status code: {response.status}"
                    )
                    
        except asyncio.TimeoutError:
            logger.error(
                "[DOWNLOAD TIMEOUT] Task: %s | User: %s | Connection or read timeout",
                task_id, user_display
            )
            raise
        except Exception as e:
            logger.error(
                "[DOWNLOAD ERROR] Task: %s | User: %s | Error: %s",
                task_id, user_display, str(e),
                exc_info=True
            )
            raise


async def upload_file(
    upload_path: str,
    zip_path: str,
    token: str,
    task_id: str,
    user_info: dict
) -> str:
    """
    Upload zip file to EBRAINS hosted bucket.
    
    Args:
        upload_path: The bucket path to upload to
        zip_path: Local path to the zip file
        token: Authorization token
        task_id: The task identifier
        user_info: Dictionary containing user information
        
    Returns:
        Success message string
    """
    user_display = f"{user_info.get('name') or user_info.get('username') or 'Unknown'}"
    user_email = user_info.get('email') or 'no-email'
    
    # Get file size for logging
    file_size = os.path.getsize(zip_path) if os.path.exists(zip_path) else 0
    
    logger.info(
        "[UPLOAD START] Task: %s | User: %s (%s) | Target: %s | Size: %d bytes (%.2f MB)",
        task_id, user_display, user_email, upload_path, file_size, file_size / (1024 * 1024)
    )
    
    url = f"https://data-proxy.ebrains.eu/api/v1/buckets/{upload_path}"
    headers = {"Authorization": f"Bearer {token}"}

    async with aiohttp.ClientSession() as session:
        try:
            logger.debug(
                "[UPLOAD] Task: %s | Requesting upload URL from: %s",
                task_id, url
            )
            
            # Get the upload URL
            async with session.put(url, headers=headers) as response:
                logger.info(
                    "[UPLOAD] Task: %s | Initial response status: %d",
                    task_id, response.status
                )
                
                if response.status != 200:
                    logger.error(
                        "[UPLOAD FAILED] Task: %s | User: %s | Failed to get upload URL | Status: %d",
                        task_id, user_display, response.status
                    )
                    raise Exception(f"Failed to get upload URL. Status: {response.status}")
                
                data = await response.json()
                upload_url = data.get("url")
                
                if not upload_url:
                    logger.error(
                        "[UPLOAD FAILED] Task: %s | User: %s | No upload URL in response",
                        task_id, user_display
                    )
                    raise Exception("Upload URL not provided in response")

                logger.info(
                    "[UPLOAD] Task: %s | Got upload URL, starting file upload",
                    task_id
                )
                
                async with aiofiles.open(zip_path, "rb") as file:
                    file_data = await file.read()
                    
                    logger.debug(
                        "[UPLOAD] Task: %s | Read %d bytes from local file, uploading...",
                        task_id, len(file_data)
                    )
                    
                    async with session.put(upload_url, data=file_data) as upload_response:
                        logger.info(
                            "[UPLOAD] Task: %s | Upload response status: %d",
                            task_id, upload_response.status
                        )
                        
                        if upload_response.status in (200, 201, 204):
                            logger.info(
                                "[UPLOAD COMPLETE] Task: %s | User: %s (%s) | Target: %s | Size: %d bytes",
                                task_id, user_display, user_email, upload_path, file_size
                            )
                            return f"Created in {upload_path}"
                        else:
                            logger.error(
                                "[UPLOAD FAILED] Task: %s | User: %s | Status: %d | Target: %s",
                                task_id, user_display, upload_response.status, upload_path
                            )
                            raise Exception(f"Failed to upload file. Status: {upload_response.status}")
                            
        except Exception as e:
            logger.error(
                "[UPLOAD ERROR] Task: %s | User: %s | Error: %s",
                task_id, user_display, str(e),
                exc_info=True
            )
            raise


async def cleanup_files(download_path: str, dzi_path: str, zip_path: str, task_id: str = "unknown"):
    """
    Asynchronously remove temporary files after processing.
    
    Args:
        download_path: Path to the downloaded file
        dzi_path: Path to the DZI file
        zip_path: Path to the zip file
        task_id: Task identifier for logging
    """
    logger.info("[CLEANUP START] Task: %s", task_id)
    
    try:
        download_path = os.path.join(DOWNLOADS_DIR, os.path.basename(download_path))
        if await path.exists(download_path):
            await remove(download_path)
            logger.debug("[CLEANUP] Task: %s | Removed download: %s", task_id, download_path)

        if dzi_path and await path.exists(dzi_path):
            await remove(dzi_path)
            logger.debug("[CLEANUP] Task: %s | Removed DZI: %s", task_id, dzi_path)
            
            dzi_dir = os.path.splitext(dzi_path)[0] + "_files"
            if await path.exists(dzi_dir):
                await asyncio.to_thread(shutil.rmtree, dzi_dir)
                logger.debug("[CLEANUP] Task: %s | Removed DZI dir: %s", task_id, dzi_dir)

        if zip_path and await path.exists(zip_path):
            await remove(zip_path)
            logger.debug("[CLEANUP] Task: %s | Removed zip: %s", task_id, zip_path)

        logger.info("[CLEANUP COMPLETE] Task: %s", task_id)
        
    except Exception as e:
        logger.error("[CLEANUP ERROR] Task: %s | Error: %s", task_id, str(e))
