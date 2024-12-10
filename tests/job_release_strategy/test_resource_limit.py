import pytest

from task_queue.job_release_strategy import ResourceLimit
from task_queue.queues.queue_base import QueueItemStage
from tests.test_work_queue import default_work_queue

@pytest.mark.unit
def test_resource_limit(default_work_queue):
    """
    Test that the ProcessingLimit strategy starts jobs as expected
    """

    RESOURCE_A_LIMIT = 10
    RESOURCE_B_LIMIT = 20
    processing_limit_strategy = ResourceLimit(
        {
            "resource_a": RESOURCE_A_LIMIT,
            "resource_b": RESOURCE_B_LIMIT
        }
    )

    # Make sure some preconditions are good.
    num_waiting = default_work_queue.get_queue_size(QueueItemStage.WAITING)
    assert num_waiting > 0
    assert default_work_queue.get_queue_size(QueueItemStage.PROCESSING) == 0
