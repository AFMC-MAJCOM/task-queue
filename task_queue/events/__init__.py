"""
Event store implementations to hold task queue event types.
"""

import warnings

try:
    from .sql_event_store import SqlEventStore
except ModuleNotFoundError:
    warnings.warn(
        "Task queue must be installed with optional dependencies "
        "`task_queue[sql]` to use `SqlEventStore`."
    )

    class SqlEventStore:
        """
        Empty placeholder class to satisfy type hints and the like.
        """

from .in_memory_event_store import InMemoryEventStore

__all__ = (
    "InMemoryEventStore",
    "SqlEventStore"
)
