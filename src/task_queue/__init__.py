"""Task Queue."""

from .config.config import get_task_queue_settings
from .config import config
try:
    from ._version import __version__, __version_tuple__
except ModuleNotFoundError
    raise ValueError('`task_queue` must be `pip install`ed!') from None

__all__ = [
    "get_task_queue_settings",
    "config",
]

