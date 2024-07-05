"""Blank dockstring for file
"""
from abc import ABC, abstractmethod
from .queue_base import QueueItemStage
from typing import Dict, Any

class QueueWorkerInterface(ABC):
    """Docstring
    """

    @abstractmethod
    def send_job(self, item_id, queue_item_body):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        pass
   
    @abstractmethod
    def poll_all_status(self) -> Dict[Any, QueueItemStage]:
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        pass


class DummyWorkerInterface(QueueWorkerInterface):
    """Docstring
        """

    def __init__(self):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        self._job_status = {}

    def send_job(self, item_id, queue_item_body):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        self._job_status[item_id] = QueueItemStage.PROCESSING

    def poll_all_status(self) -> Dict[Any, QueueItemStage]:
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        return self._job_status
    
    def mock_success(self, item_id):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        self._job_status[item_id] = QueueItemStage.SUCCESS

    def mock_fail(self, item_id):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        self._job_status[item_id] = QueueItemStage.FAIL
