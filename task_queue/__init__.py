"""Task Queue.
"""

from task_queue.logger import logger
from .config.config import get_task_queue_settings
from .config import config


__all__ = [
    "get_task_queue_settings",
    "config",
    "logger",
]

__version__ = "1.7.6"
