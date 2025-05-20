import warnings

try:
    from .sql_event_store import SqlEventStore
except ModuleNotFoundError:
    warnings.warn(
        "Task queue must be installed with optional dependencies "
        "`task_queue[sql]` to use `SqlEventStore`."
    )
    SqlEventStore = None

from .in_memory_event_store import InMemoryEventStore
