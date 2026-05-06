from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import uuid
import logging
import sys

from .task_manager import TaskManager
from .jwt_utils import extract_user_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="CreateZoom API",
    description="DeepZoom pyramid creation service for EBRAINS",
    version="1.0.4"
)

# CORS configuration
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Initialize global task manager
task_manager = TaskManager()


@app.get("/")
async def root():
    """Root endpoint."""
    return {"message": "Hello World"}


@app.get("/deepzoom/health")
async def health():
    """Health check endpoint."""
    return {"status": "I'm alive!"}


@app.post("/deepzoom", status_code=202)
async def deepzoom_endpoint(request: Request):
    """
    Create a new DeepZoom conversion task.
    
    Expects JSON body with:
    - path: Source file path in bucket
    - target_path: Destination path in bucket
    - token: EBRAINS authorization token
    """
    try:
        data = await request.json()
        
        # Extract user info for logging
        token = data.get("token", "")
        user_info = extract_user_info(token) if token else {}
        user_display = f"{user_info.get('name') or user_info.get('username') or 'Unknown'}"
        user_email = user_info.get('email') or 'no-email'
        
        logger.info(
            "[REQUEST RECEIVED] User: %s (%s) | Path: %s | Target: %s",
            user_display,
            user_email,
            data.get("path", "N/A"),
            data.get("target_path", "N/A")
        )

        # Validate required parameters
        for param in ["path", "target_path", "token"]:
            if not data.get(param):
                logger.error(
                    "[REQUEST REJECTED] User: %s | Missing parameter: %s",
                    user_display, param
                )
                raise HTTPException(
                    status_code=400, detail=f"Missing required parameter: {param}"
                )
            if not isinstance(data[param], str) or not data[param].strip():
                logger.error(
                    "[REQUEST REJECTED] User: %s | Invalid parameter: %s",
                    user_display, param
                )
                raise HTTPException(
                    status_code=400, detail=f"{param} must be a non-empty string"
                )

        task_id = str(uuid.uuid4())
        
        logger.info(
            "[TASK CREATED] Task: %s | User: %s (%s) | Path: %s",
            task_id, user_display, user_email, data["path"]
        )

        await task_manager.add_task(
            task_id, data["path"], data["target_path"], data["token"]
        )

        response = {
            "task_id": task_id,
            "status": "accepted",
            "status_endpoint": f"/deepzoom/status/{task_id}",
        }
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[REQUEST ERROR] Error processing request: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/deepzoom/status/{task_id}")
async def get_task_status(task_id: str):
    """
    Get the status of a DeepZoom conversion task.
    
    Returns task details including:
    - status: pending, processing, completed, or failed
    - current_step: Current processing stage
    - progress: Percentage complete (0-100)
    - result: Result on completion
    - error: Error message on failure
    - submitted_by: User who submitted the task
    - user_info: Detailed user information
    """
    try:
        logger.debug("[STATUS CHECK] Task: %s", task_id)
        task = task_manager.task_store.get_task(task_id)
        
        if not task:
            logger.warning("[STATUS NOT FOUND] Task: %s", task_id)
            raise HTTPException(status_code=404, detail="Task not found")
        
        return task
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[STATUS ERROR] Task: %s | Error: %s", task_id, str(e), exc_info=True)
        raise


# Admin/Dashboard endpoints

async def verify_ebrains_token(token: str) -> bool:
    """
    Verify token and check if email ends with @medisin.uio.no.
    Currently provides internal access only.
    """
    url = "https://iam.ebrains.eu/auth/realms/hbp/protocol/openid-connect/userinfo"
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url, headers={"Authorization": f"Bearer {token}"}
        ) as response:
            if response.status == 200:
                data = await response.json()
                email = data.get("email", "")
                is_authorized = email.endswith("@medisin.uio.no")
                logger.info(
                    "[AUTH CHECK] Email: %s | Authorized: %s",
                    email, is_authorized
                )
                return is_authorized
    return False


@app.get("/deepzoom/tasks")
async def get_all_tasks(request: Request):
    """
    Get all tasks (admin endpoint).
    Requires valid EBRAINS token from @medisin.uio.no domain.
    """
    try:
        # Get token from Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            logger.warning("[ADMIN ACCESS DENIED] Missing or invalid token header")
            raise HTTPException(status_code=401, detail="Missing or invalid token")

        token = auth_header.split(" ")[1]
        
        # Extract user info for logging
        user_info = extract_user_info(token)
        user_display = f"{user_info.get('name') or user_info.get('username') or 'Unknown'}"
        
        logger.info("[ADMIN ACCESS ATTEMPT] User: %s", user_display)

        is_authorized = await verify_ebrains_token(token)
        if not is_authorized:
            logger.warning(
                "[ADMIN ACCESS DENIED] User: %s | Unauthorized email domain",
                user_display
            )
            raise HTTPException(status_code=403, detail="Unauthorized email domain")

        task_manager.task_store.cleanup_old_tasks()

        logger.info(
            "[ADMIN ACCESS GRANTED] User: %s | Tasks returned: %d",
            user_display, len(task_manager.task_store.tasks)
        )

        return {
            "tasks": task_manager.task_store.tasks,
            "total": len(task_manager.task_store.tasks),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("[ADMIN ERROR] Error getting tasks: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

