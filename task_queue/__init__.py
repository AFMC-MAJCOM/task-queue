"""Task Queue.
"""

from .config.config import get_task_queue_settings
from .config import config

__version__ = "1.4.0"

__all__ = [
    "get_task_queue_settings",
    "config",
]
