"""Different options for how to automatically start jobs in the task queue.
"""
from .release_all import ReleaseAll as ReleaseAll
from .job_release_strategy_base import JobReleaseStrategyBase as JobReleaseStrategyBase
from .processing_limit import ProcessingLimit as ProcessingLimit
from .resource_limit import ResourceLimit as ResourceLimit
