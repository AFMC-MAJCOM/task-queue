from abc import ABC, abstractmethod
from data_pipeline.queue_base import QueueItemStage
from typing import Dict, Any

class QueueWorkerInterface(ABC):

    @abstractmethod
    def send_job(self, item_id, queue_item_body):
        pass
   
    @abstractmethod
    def poll_all_status(self) -> Dict[Any, QueueItemStage]:
        pass


class DummyWorkerInterface(QueueWorkerInterface):

    def __init__(self):
        self._job_status = {}

    def send_job(self, item_id, queue_item_body):
        self._job_status[item_id] = QueueItemStage.PROCESSING

    def poll_all_status(self) -> Dict[Any, QueueItemStage]:
        return self._job_status
    
    def mock_success(self, item_id):
        self._job_status[item_id] = QueueItemStage.SUCCESS

    def mock_fail(self, item_id):
        self._job_status[item_id] = QueueItemStage.FAIL
