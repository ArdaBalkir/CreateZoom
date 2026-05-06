"""
Image processing utilities for DeepZoom pyramid creation and compression.
"""
import os
import asyncio
import logging
import zipfile
from io import BytesIO
import pyvips

# Suppress pyvips worker/thread messages
pyvips.cache_set_max(0)
logging.getLogger('pyvips').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

DATA_ROOT = "/data"
OUTPUTS_DIR = os.path.join(DATA_ROOT, "outputs")


async def create_deepzoom(path: str, task_id: str = "unknown") -> str:
    """
    Creates a DeepZoom pyramid from the image file specified.
    Runs in a thread pool since pyvips operations are CPU-bound.
    
    Args:
        path: Path to the source image file
        task_id: Task identifier for logging
        
    Returns:
        Path to the created DZI file
    """
    def process_image():
        logger.info(
            "[DEEPZOOM START] Task: %s | Source: %s",
            task_id, path
        )
        
        try:
            # Use access='sequential' to avoid keeping file handle open
            image = pyvips.Image.new_from_file(path, access='sequential')
            
            logger.info(
                "[DEEPZOOM] Task: %s | Image loaded | Size: %dx%d | Bands: %d",
                task_id, image.width, image.height, image.bands
            )
            
            os.makedirs(OUTPUTS_DIR, exist_ok=True)
            output_path = os.path.join(OUTPUTS_DIR, os.path.basename(path))
            
            image.dzsave(output_path)
            
            dzi_path = output_path + ".dzi"
            logger.info(
                "[DEEPZOOM COMPLETE] Task: %s | Output: %s",
                task_id, dzi_path
            )
            
            # Explicitly delete image to release file handle (important on Windows)
            del image
            
            return dzi_path
            
        except Exception as e:
            logger.error(
                "[DEEPZOOM ERROR] Task: %s | Error: %s",
                task_id, str(e),
                exc_info=True
            )
            raise

    return await asyncio.to_thread(process_image)


async def create_zip_archive(path: str, task_id: str = "unknown") -> str:
    """
    Zips the pyramid files with a .dzip extension.
    Runs in a thread pool since compression is CPU-bound.
    
    Args:
        path: Path to the DZI file
        task_id: Task identifier for logging
        
    Returns:
        Path to the created zip file
    """
    def create_zip():
        dzi_file = path
        dzi_dir = os.path.splitext(dzi_file)[0] + "_files"
        strip_file_name = os.path.basename(os.path.splitext(dzi_file)[0])
        zip_path = f"{os.path.dirname(dzi_file)}/{strip_file_name}.dzip"

        logger.info(
            "[ZIP START] Task: %s | DZI: %s | Dir: %s",
            task_id, dzi_file, dzi_dir
        )
        
        try:
            # Count files for logging
            file_count = 0
            total_size = 0
            
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_STORED) as zipf:
                zipf.write(dzi_file, os.path.basename(dzi_file))
                file_count += 1
                
                for root, _, files in os.walk(dzi_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, os.path.dirname(dzi_dir))
                        zipf.write(file_path, arcname)
                        file_count += 1
                        total_size += os.path.getsize(file_path)

            # Write the buffer to disk
            with open(zip_path, "wb") as f:
                f.write(zip_buffer.getvalue())
            
            final_size = os.path.getsize(zip_path)
            
            logger.info(
                "[ZIP COMPLETE] Task: %s | Files: %d | Original: %.2f MB | Zipped: %.2f MB | Output: %s",
                task_id, file_count, total_size / (1024 * 1024), final_size / (1024 * 1024), zip_path
            )
            
            return zip_path
            
        except Exception as e:
            logger.error(
                "[ZIP ERROR] Task: %s | Error: %s",
                task_id, str(e),
                exc_info=True
            )
            raise

    return await asyncio.to_thread(create_zip)
