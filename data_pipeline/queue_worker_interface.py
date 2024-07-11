"""Wherein is contained the Abstract Class for QueueWorkerInterface.
"""
from abc import ABC, abstractmethod

from data_pipeline.queue_base import QueueItemStage


class QueueWorkerInterface(ABC):
    """Abstract Queue Worker Interface Class.
    """

    @abstractmethod
    def send_job(self, item_id, queue_item_body):
        """Starts a job from queue item.

        Parameters:
        -----------
        item_id: str
            Item ID
        queue_item_body: List
            Contents of Queue Item body.
        """
        pass

    @abstractmethod
    def poll_all_status(self):
        """Requests status from workflows.

        Returns:
        -----------
        Returns Dict[Any, QueueItemStage]
        """
        pass


class DummyWorkerInterface(QueueWorkerInterface):
    """Dummy Worker Interface Class

    Jobs are stored as a dictionary in memory, and are manually marked as
    Success or Fail. Only useful for testing.
    """

    def __init__(self):
        """Initializes DummyWorkerInterface.
        """
        self._job_status = {}

    def send_job(self, item_id, queue_item_body):
        """Starts a job from queue item.

        Parameters:
        -----------
        item_id: str
            Item ID
        queue_item_body: List
            Contents of Queue Item body.
        """
        self._job_status[item_id] = QueueItemStage.PROCESSING

    def poll_all_status(self):
        """Requests status from workflows.

        Returns:
        -----------
        Returns Dict[Any, QueueItemStage] of job statuses.
        """
        return self._job_status

    def mock_success(self, item_id):
        """Forces Item to SUCCESS stage.

        Parameters:
        -----------
        item_id: str
            ID of Item.
        """
        self._job_status[item_id] = QueueItemStage.SUCCESS

    def mock_fail(self, item_id):
        """Forces Item to FAIL stage.

        Parameters:
        -----------
        item_id: str
            ID of Item.
        """
        self._job_status[item_id] = QueueItemStage.FAIL
