"""
Releases jobs to try to keep a certain number of tasks in the `processing`
state.
"""

from .job_release_strategy_base import JobReleaseStrategyBase
from ..queues.queue_base import QueueItemStage
from ..logger import logger

# pylint: disable-next=too-few-public-methods
class ProcessingLimit(JobReleaseStrategyBase):
    """
    Releases jobs to try to keep a certain number of tasks in the `processing`
    state.
    """

    def __init__(self, max_num_processing:int):
        """
        Parameters:
        -----------
        max_num_processing:
            The job release strategy will push new jobs to the work queue as
            long as there are less than this many queue items in the
            PROCESSING stage.
        """

        self.max_num_processing = max_num_processing


    def release_next_jobs(self, work_queue):
        n_processing = work_queue.get_queue_size(QueueItemStage.PROCESSING)

        # make sure there is never a negative number here. There should never
        # be, but I don't want to find out what happens if there is.
        to_start = max(0, self.max_num_processing - n_processing)

        started_jobs = work_queue.push_next_jobs(to_start)

        logger.info(
            "ProcessingLimit.release_next_jobs: Started %s jobs",
            started_jobs
        )
