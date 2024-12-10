"""Releases all jobs as soon as they are added.
"""

from .job_release_strategy_base import JobReleaseStrategyBase
from ..queues.queue_base import QueueItemStage

class ReleaseAll(JobReleaseStrategyBase):
    """
    Simplest possible job release strategy: Releases all jobs as soon as
    they are added.
    """

    def release_next_jobs(self, work_queue):
        all_waiting_jobs = work_queue.get_queue_size(QueueItemStage.WAITING)
        work_queue.push_next_jobs(all_waiting_jobs)
