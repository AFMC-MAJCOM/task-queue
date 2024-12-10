"""Releasess jobs if resources are available.
"""

from .job_release_strategy_base import JobReleaseStrategyBase

class ResourceLimit(JobReleaseStrategyBase):
    """
    Releases new jobs if all defined resources are above zero. Resource usage
    is defined by queue items.
    """

    def __init__(self, resource_limits:dict[str, int]):
        self.resource_limits = resource_limits.copy()

    def release_next_jobs(self, work_queue):
        return super().release_next_jobs(work_queue)
