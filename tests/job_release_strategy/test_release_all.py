import pytest

from task_queue.job_release_strategy import ReleaseAll
from task_queue.queues.queue_base import QueueItemStage
from tests.test_work_queue import default_work_queue as work_queue

@pytest.mark.unit
def test_release_all(work_queue):
    """
    Test that the ReleaseAll strategy moves all jobs in WAITING to PROCESSING.
    """
    release_all_strategy = ReleaseAll()

    # make sure some preconditions are good.
    num_waiting = work_queue.get_queue_size(QueueItemStage.WAITING)
    assert num_waiting > 0
    assert work_queue.get_queue_size(QueueItemStage.PROCESSING) == 0

    release_all_strategy.release_next_jobs(work_queue)

    # check the postconditions
    assert work_queue.get_queue_size(QueueItemStage.WAITING) == 0
    processing = work_queue.get_queue_size(QueueItemStage.PROCESSING)
    assert processing == num_waiting


