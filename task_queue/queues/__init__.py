"""
Types of queues that store task statuses in the task queue.
"""

import warnings

try:
    from .sql_queue import json_sql_queue
except ModuleNotFoundError:
    warnings.warn(
        "Task queue must be installed with optional dependencies "
        "`task_queue[sql]` to use `json_sql_queue`."
    )

    def json_sql_queue():
        """
        Empty placeholder function to satisfy type hints and the like.
        """

try:
    from .s3_queue import json_s3_queue
except ModuleNotFoundError:
    warnings.warn(
        "Task queue must be installed with optional dependencies "
        "`task_queue[sqls3]` to use `json_s3_queue`."
    )

    def json_s3_queue():
        """
        Empty placeholder function to satisfy type hints and the like.
        """


from .in_memory_queue import in_memory_queue as memory_queue
from .queue_with_events import queue_with_events as event_queue
from .queue_base import QueueBase, QueueItemStage

__all__ = (
    "json_sql_queue",
    "json_s3_queue",
    "memory_queue",
    "event_queue",
    "QueueBase",
    "QueueItemStage"
)
