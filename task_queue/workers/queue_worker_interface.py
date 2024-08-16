"""Wherein is contained the Abstract Class for QueueWorkerInterface.
"""
from abc import ABC, abstractmethod

from ..queues.queue_base import QueueItemStage


class QueueWorkerInterface(ABC):
    """Abstract Queue Worker Interface Class.
    """

    @abstractmethod
    def send_job(self, item_id, queue_item_body):
        """Starts a job from queue item.

        Parameters:
        -----------
        item_id: str
            Queue Item ID
        queue_item_body: dict
            Dictionary that must contain a key:value pair where the key is
            'submit_body' and the value is a dictionary with the format
            matching the submit_body schema.
        """
        raise NotImplementedError("Please Implement this method")

    @abstractmethod
    def delete_job(self, queue_item_id):
        """Sends a delete request to argo workflows to delete a specific
        completed workflow.

        Parameters:
        -----------
        queue_item_id: str
            Queue Item ID
        """
        raise NotImplementedError("Please Implement this method")


    @abstractmethod
    def poll_all_status(self):
        """Poll status of all jobs sent by the worker interface.

        Returns:
        -----------
        Returns Dict[Any, QueueItemStage]
        """
        raise NotImplementedError("Please Implement this method")


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
            Queue Item ID
        queue_item_body: dict
            Dictionary that must contain a key:value pair where the key is
            'submit_body' and the value is a dictionary with the format
            matching the submit_body schema.
        """
        self._job_status[item_id] = QueueItemStage.PROCESSING

    def delete_job(self, queue_item_id):
        """Sends a delete request to argo workflows to delete a specific
        completed workflow.

        Parameters:
        -----------
        queue_item_id: str
            Queue Item ID
        """
        self._job_status[queue_item_id] = None

    def poll_all_status(self):
        """Poll status of all jobs sent by the worker interface.

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
            Queue ID of Item.
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
