from loguru import logger
import sys
from config.settings import settings

def setup_logging():
    """Configure Loguru logging"""
    logger.remove()
    logger.add(sys.stdout,
               level=getattr(settings, 'log_level', 'INFO'),
               format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
               enqueue=True)

