import pytest

from task_queue.job_release_strategy import ReleaseAll
from task_queue.queues.queue_base import QueueItemStage
from tests.test_work_queue import default_work_queue

@pytest.mark.unit
def test_release_all(default_work_queue):
    """
    Test that the ReleaseAll strategy moves all jobs in WAITING to PROCESSING.
    """
    release_all_strategy = ReleaseAll()

    # make sure some preconditions are good.
    num_waiting = default_work_queue.get_queue_size(QueueItemStage.WAITING)
    assert num_waiting > 0
    assert default_work_queue.get_queue_size(QueueItemStage.PROCESSING) == 0

    release_all_strategy.release_next_jobs(default_work_queue)

    # check the postconditions
    assert default_work_queue.get_queue_size(QueueItemStage.WAITING) == 0
    assert default_work_queue.get_queue_size(QueueItemStage.PROCESSING) == num_waiting


