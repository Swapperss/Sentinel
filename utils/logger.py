import logging
import os
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

def get_logger(module_name: str, log_file: str = "sentinel_platform.log"):
    """
    Standardized logger with Daily Rotation for the Auto-Sentinel platform.
    Falls back to console logging if file logging fails (e.g., permission errors).
    """
    # 1. Path Management
    log_dir = Path(__file__).parent.parent / "logs"
    
    # Try to create logs directory, but don't fail if we can't
    log_path = None
    try:
        log_dir.mkdir(exist_ok=True)
        log_path = log_dir / log_file
    except PermissionError:
        # If we can't create the logs directory, use current directory or /tmp
        import tempfile
        log_path = Path(tempfile.gettempdir()) / log_file

    logger = logging.getLogger(module_name)
    
    # Avoid adding multiple handlers if get_logger is called again in the same session
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # 2. Formatter
        log_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 3. Daily Rotation Handler
        # 'when="midnight"' creates a new file every day
        # 'backupCount=7' keeps the last 7 days of logs (saves your disk space!)
        try:
            rotation_handler = TimedRotatingFileHandler(
                log_path, 
                when="midnight", 
                interval=1, 
                backupCount=7,
                encoding="utf-8"
            )
            rotation_handler.setFormatter(log_format)
            logger.addHandler(rotation_handler)
        except (PermissionError, OSError):
            # If we can't create file handler, continue with stream handler only
            pass

        # 4. Stream Handler (Console)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(log_format)

        logger.addHandler(stream_handler)

    return logger