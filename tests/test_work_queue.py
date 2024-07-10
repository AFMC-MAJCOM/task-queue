import pytest

from data_pipeline import work_queue
import data_pipeline.in_memory_queue as mq
from data_pipeline.queue_worker_interface import DummyWorkerInterface
from data_pipeline.queue_base import QueueItemStage
from tests.common_queue import default_items


@pytest.fixture
def default_work_queue() -> work_queue.WorkQueue:
    queue = mq.InMemoryQueue()
    queue.put(default_items)
    interface = DummyWorkerInterface()
    return work_queue.WorkQueue(queue, interface)

def test_push_job(default_work_queue):
    default_work_queue.push_next_jobs()

    # make sure we have something in the worker interface that is in processing
    n_processing_first = \
        default_work_queue._queue.size(QueueItemStage.PROCESSING)

    assert n_processing_first == 1

    # push more jobs
    n_jobs = 3
    default_work_queue.push_next_jobs(3)
    n_processing_second = \
        default_work_queue._queue.size(QueueItemStage.PROCESSING)

    # assert there are `n_jobs` more items in `processing`
    assert n_processing_second - n_processing_first == n_jobs

def test_job_success(default_work_queue):
    n_jobs = 5

    # push something to the worker
    pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    success_id, _ = pushed_jobs[0]

    # force a success
    default_work_queue._interface.mock_success(success_id)
    default_work_queue.update_job_status()
    n_success = default_work_queue._queue.size(QueueItemStage.SUCCESS)
    assert n_success == 1

    # check that it was actually moved
    n_processing = default_work_queue._queue.size(QueueItemStage.PROCESSING)
    assert n_processing == n_jobs - n_success

def test_job_fail(default_work_queue):
    n_jobs = 5

    # push something to the worker
    pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    success_id, _ = pushed_jobs[0]

    # force a failure
    default_work_queue._interface.mock_fail(success_id)
    default_work_queue.update_job_status()
    n_fail = default_work_queue._queue.size(QueueItemStage.FAIL)
    assert n_fail == 1

    # check that it was actually moved
    n_processing = default_work_queue._queue.size(QueueItemStage.PROCESSING)
    assert n_processing == n_jobs - n_fail

def test_idempotent_update(default_work_queue):
    n_jobs = 5

    pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    success_id, _ = pushed_jobs[0]

    default_work_queue._interface.mock_success(success_id)
    default_work_queue.update_job_status()
    default_work_queue.update_job_status()

def test_push_multiple(default_work_queue):
    n_jobs = 3

    first_pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    second_pushed_jobs = default_work_queue.push_next_jobs(n_jobs)

    # assert that all IDs are different - no item was grabbed twice
    first_ids = [ i for i, _ in first_pushed_jobs ]
    second_ids = [ i for i, _ in second_pushed_jobs ]

    assert len(set(first_ids).intersection((set(second_ids)))) == 0
