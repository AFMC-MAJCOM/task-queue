"""Abstract Base Class for Job Release Strategies.
"""

from abc import ABC, abstractmethod

from task_queue import logger
from ..workers.work_queue import WorkQueue

class JobReleaseStrategyBase(ABC):
    """Abstract Base Class for Job Release Strategies.
    """

    @abstractmethod
    def release_next_jobs(self, work_queue : WorkQueue):
        """Pushes new jobs to the work queue.
        """
        pass
