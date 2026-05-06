"""
Task management for tracking and processing DeepZoom conversion tasks.
"""
import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from .jwt_utils import extract_user_info, get_user_display_string
from .file_operations import download_file, upload_file, cleanup_files, DOWNLOADS_DIR
from .image_processing import create_deepzoom, create_zip_archive

logger = logging.getLogger(__name__)


class TaskStore:
    """
    Manages tasks and their statuses.
    Default TTL is 72 hours.
    """

    def __init__(self, ttl_hours: int = 72):
        self.tasks: Dict[str, dict] = {}
        self.ttl = timedelta(hours=ttl_hours)
        logger.info("TaskStore initialized with TTL: %d hours", ttl_hours)

    def add_task(
        self,
        task_id: str,
        task_data: dict,
        user_info: Optional[dict] = None
    ):
        """
        Add a new task to the store.
        
        Args:
            task_id: Unique task identifier
            task_data: Dictionary containing path and target_path
            user_info: Optional user information from JWT
        """
        self.tasks[task_id] = {
            **task_data,
            "created_at": datetime.now(),
            "status": "pending",
            "current_step": "initialized",
            "step_details": None,
            "result": None,
            "error": None,
            "progress": 0,
            # User information
            "user_info": {
                "username": user_info.get("username") if user_info else None,
                "email": user_info.get("email") if user_info else None,
                "name": user_info.get("name") if user_info else None,
                "given_name": user_info.get("given_name") if user_info else None,
                "family_name": user_info.get("family_name") if user_info else None,
                "sub": user_info.get("sub") if user_info else None,
            },
            "submitted_by": get_user_display_string(user_info) if user_info else "Unknown",
        }
        
        logger.info(
            "Added new task: %s | Submitted by: %s | Path: %s | Target: %s",
            task_id,
            self.tasks[task_id]["submitted_by"],
            task_data.get("path"),
            task_data.get("target_path")
        )

    def update_task(self, task_id: str, updates: dict):
        """Update task with new values."""
        if task_id in self.tasks:
            self.tasks[task_id].update(updates)
            logger.debug("Updated task %s: %s", task_id, updates)

    def get_task(self, task_id: str) -> Optional[dict]:
        """Get task by ID."""
        return self.tasks.get(task_id)

    def cleanup_old_tasks(self):
        """Remove tasks older than TTL."""
        before_count = len(self.tasks)
        now = datetime.now()
        self.tasks = {
            task_id: task
            for task_id, task in self.tasks.items()
            if now - task["created_at"] < self.ttl
        }
        after_count = len(self.tasks)
        if before_count > after_count:
            logger.info("Cleaned up %d old tasks", before_count - after_count)


class TaskManager:
    """
    Manages task processing with semaphore-based concurrency control.
    """
    
    PROCESS_WORKERS = 12

    def __init__(self):
        self.semaphore = asyncio.Semaphore(self.PROCESS_WORKERS)
        self.task_store = TaskStore()
        logger.info("TaskManager initialized with %d workers", self.PROCESS_WORKERS)

    async def add_task(
        self,
        task_id: str,
        path: str,
        target_path: str,
        token: str
    ) -> str:
        """
        Add and start processing a new task.
        
        Args:
            task_id: Unique task identifier
            path: Source file path in bucket
            target_path: Destination path in bucket
            token: Authorization token
            
        Returns:
            The task_id
        """
        # Extract user info from JWT for logging and tracking
        user_info = extract_user_info(token)
        
        logger.info(
            "[TASK SUBMITTED] Task: %s | User: %s (%s) | Source: %s | Target: %s",
            task_id,
            user_info.get("name") or user_info.get("username") or "Unknown",
            user_info.get("email") or "no-email",
            path,
            target_path
        )
        
        self.task_store.add_task(
            task_id,
            {"path": path, "target_path": target_path},
            user_info
        )
        
        asyncio.create_task(
            self._process_task(task_id, path, target_path, token, user_info)
        )
        
        return task_id

    async def _process_task(
        self,
        task_id: str,
        path: str,
        target_path: str,
        token: str,
        user_info: dict
    ):
        """
        Process a single task through all stages.
        
        Args:
            task_id: Unique task identifier
            path: Source file path in bucket
            target_path: Destination path in bucket
            token: Authorization token
            user_info: User information from JWT
        """
        dzi_path = None
        zip_path = None
        user_display = get_user_display_string(user_info)
        
        async with self.semaphore:
            try:
                logger.info(
                    "[TASK PROCESSING] Task: %s | User: %s | Starting pipeline",
                    task_id, user_display
                )
                
                # Stage 1: Download
                self.task_store.update_task(
                    task_id,
                    {
                        "status": "processing",
                        "current_step": "downloading",
                        "progress": 0,
                    },
                )
                download_path = await download_file(
                    path, token, task_id, self.task_store, user_info
                )

                # Stage 2: DeepZoom
                self.task_store.update_task(
                    task_id,
                    {"current_step": "creating_deepzoom", "progress": 25}
                )
                dzi_path = await create_deepzoom(download_path, task_id)

                # Stage 3: Zip
                self.task_store.update_task(
                    task_id,
                    {"current_step": "compressing", "progress": 50}
                )
                zip_path = await create_zip_archive(dzi_path, task_id)

                # Stage 4: Upload
                self.task_store.update_task(
                    task_id,
                    {"current_step": "uploading", "progress": 75}
                )
                upload_filename = os.path.basename(zip_path)
                result = await upload_file(
                    f"{target_path}/{upload_filename}",
                    zip_path,
                    token,
                    task_id,
                    user_info
                )

                # Success
                self.task_store.update_task(
                    task_id,
                    {
                        "status": "completed",
                        "current_step": "completed",
                        "result": result,
                        "progress": 100,
                    },
                )
                
                logger.info(
                    "[TASK COMPLETED] Task: %s | User: %s | Result: %s",
                    task_id, user_display, result
                )

                # Cleanup
                await cleanup_files(
                    os.path.join(DOWNLOADS_DIR, os.path.basename(path)),
                    dzi_path,
                    zip_path,
                    task_id
                )

            except Exception as e:
                logger.error(
                    "[TASK FAILED] Task: %s | User: %s | Error: %s",
                    task_id, user_display, str(e),
                    exc_info=True
                )
                
                self.task_store.update_task(
                    task_id,
                    {
                        "status": "failed",
                        "current_step": "failed",
                        "error": str(e),
                        "progress": 0,
                    },
                )
                
                # Cleanup on failure
                await cleanup_files(
                    os.path.join(DOWNLOADS_DIR, os.path.basename(path)),
                    dzi_path,
                    zip_path,
                    task_id
                )
