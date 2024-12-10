import pytest

from task_queue.job_release_strategy import ProcessingLimit
from task_queue.queues.queue_base import QueueItemStage
from tests.test_work_queue import default_work_queue as work_queue

@pytest.mark.unit
def test_processing_limit(work_queue):
    """
    Test that the ProcessingLimit strategy starts jobs as expected
    """

    NUM_PROCESSING_LIMIT = 2
    processing_limit_strategy = ProcessingLimit(NUM_PROCESSING_LIMIT)

    # Make sure some preconditions are good.
    num_waiting = work_queue.get_queue_size(QueueItemStage.WAITING)
    assert num_waiting > 0
    assert work_queue.get_queue_size(QueueItemStage.PROCESSING) == 0

    # First release - make sure jobs get started.
    processing_limit_strategy.release_next_jobs(work_queue)
    waiting = work_queue.get_queue_size(QueueItemStage.WAITING)
    assert waiting == num_waiting - NUM_PROCESSING_LIMIT
    processing = work_queue.get_queue_size(QueueItemStage.PROCESSING)
    assert processing == NUM_PROCESSING_LIMIT

    # Second release - make sure no new jobs get started, as we already have
    # enough jobs processing.
    processing_limit_strategy.release_next_jobs(work_queue)
    waiting = work_queue.get_queue_size(QueueItemStage.WAITING)
    assert waiting == num_waiting - NUM_PROCESSING_LIMIT
    processing = work_queue.get_queue_size(QueueItemStage.PROCESSING)
    assert processing == NUM_PROCESSING_LIMIT

    # Finish just one job and make sure it fills back up to the limit again.
    item_to_succeed = (
        work_queue
        ._queue
        .lookup_state(QueueItemStage.PROCESSING)[0]
    )
    work_queue._interface.mock_success(item_to_succeed)
    work_queue.update_job_status()
    processing_limit_strategy.release_next_jobs(work_queue)
    processing = work_queue.get_queue_size(QueueItemStage.PROCESSING)
    assert processing == NUM_PROCESSING_LIMIT

