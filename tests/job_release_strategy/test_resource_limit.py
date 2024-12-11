import pytest

from task_queue.job_release_strategy import ResourceLimit
from task_queue.queues.queue_base import QueueItemStage

@pytest.mark.unit
def test_resource_limit(default_work_queue):
    """
    Test that the ProcessingLimit strategy starts jobs as expected
    """

    RESOURCE_A_LIMIT = 10
    RESOURCE_B_LIMIT = 20
    RESOURCE_C_LIMIT = 10

    processing_limit_strategy = ResourceLimit(
        {
            "resource_a": RESOURCE_A_LIMIT,
            "resource_b": RESOURCE_B_LIMIT,
            "resource_c": RESOURCE_C_LIMIT
        }
    )

    # We should see 2 jobs get started, because the resource B limit is 20, and
    # each job uses 10 of resource B
    processing_limit_strategy.release_next_jobs(default_work_queue)
    processing = default_work_queue.get_queue_size(QueueItemStage.PROCESSING)
    assert processing == 2

    # Second release - make sure no new jobs get started, as we already have
    # enough jobs processing.
    processing_limit_strategy.release_next_jobs(default_work_queue)
    assert default_work_queue.get_queue_size(QueueItemStage.PROCESSING) == 2

    # Finish just one job and make sure it fills back up to the limit again.
    item_to_succeed = (
        default_work_queue
        ._queue
        .lookup_state(QueueItemStage.PROCESSING)[0]
    )
    default_work_queue._interface.mock_success(item_to_succeed)
    default_work_queue.update_job_status()
    processing_limit_strategy.release_next_jobs(default_work_queue)
    assert default_work_queue.get_queue_size(QueueItemStage.PROCESSING) == 2

@pytest.mark.unit
def test_resource_limit_missing_resources(default_work_queue):
    """
    Test that the resource limit still works even with jobs that have resource
    types that are not set in the ResourceLimit and vice versa
    """

    RESOURCE_A_LIMIT = 10
    RESOURCE_B_LIMIT = 20
    RESOURCE_D_LIMIT = 5

    processing_limit_strategy = ResourceLimit(
        {
            "resource_a": RESOURCE_A_LIMIT,
            "resource_b": RESOURCE_B_LIMIT,
            "resource_d": RESOURCE_D_LIMIT
        }
    )

    # We should see 2 jobs get started, because the resource B limit is 20, and
    # each job uses 10 of resource B
    processing_limit_strategy.release_next_jobs(default_work_queue)
    processing = default_work_queue.get_queue_size(QueueItemStage.PROCESSING)
    assert processing == 2


@pytest.mark.unit
def test_resource_limit_peek_batch_size(default_work_queue):
    """
    Test that the resource limit will start more than `peek_batch_size` jobs
    at once.
    """

    RESOURCE_A_LIMIT = 10
    RESOURCE_B_LIMIT = 20
    RESOURCE_C_LIMIT = 10

    processing_limit_strategy = ResourceLimit(
        {
            "resource_a": RESOURCE_A_LIMIT,
            "resource_b": RESOURCE_B_LIMIT,
            "resource_c": RESOURCE_C_LIMIT
        },
        peek_batch_size=1
    )

    # We should see 2 jobs get started, because the resource B limit is 20, and
    # each job uses 10 of resource B, even though the peek batch size is only
    # 1.
    processing_limit_strategy.release_next_jobs(default_work_queue)
    processing = default_work_queue.get_queue_size(QueueItemStage.PROCESSING)
    assert processing == 2