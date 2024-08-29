"""Task Queue."""

# import order matters here! If logger is not imported first, other modules
# running `from task_queue import logger` will find the module before the
# package attribute.
from task_queue.logger import logger

from task_queue.config import config
from task_queue.config.config import get_task_queue_settings
try:
    from ._version import __version__, __version_tuple__
except ModuleNotFoundError:
    raise ValueError('`task_queue` must be `pip install`ed!') from None

__all__ = [
    "get_task_queue_settings",
    "config",
    "logger",
    "__version__",
    "__version_tuple__",
]
