"""Pytests for the work_queue functionality.
"""
import pytest

from task_queue.workers import work_queue
import task_queue.queues.in_memory_queue as mq
from task_queue.workers.queue_worker_interface import DummyWorkerInterface
from task_queue.queues.queue_base import QueueItemStage
from tests.common_queue import default_items


@pytest.fixture
def default_work_queue() -> work_queue.WorkQueue:
    """This is a fixture to create a work_queue for testing.

    Returns:
    -----------
    A default work_queue to be used for pytests.

    """
    queue = mq.in_memory_queue()
    queue.put(default_items)
    interface = DummyWorkerInterface()
    return work_queue.WorkQueue(queue, interface)

def test_push_job(default_work_queue):
    """Test pushing jobs to the work queue pushes the correct number of jobs.
    """
    default_work_queue.push_next_jobs()

    # Make sure we have something in the worker interface that is in processing
    n_processing_first = \
        default_work_queue._queue.size(QueueItemStage.PROCESSING)

    assert n_processing_first == 1

    # Push more jobs
    n_jobs = 3
    default_work_queue.push_next_jobs(3)
    n_processing_second = \
        default_work_queue._queue.size(QueueItemStage.PROCESSING)

    # Assert there are `n_jobs` more items in `processing`
    assert n_processing_second - n_processing_first == n_jobs

def test_job_success(default_work_queue):
    """Test that jobs are moved after succeeding.
    """
    n_jobs = 5

    # Push something to the worker
    pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    success_id, _ = pushed_jobs[0]

    # Force a success
    default_work_queue._interface.mock_success(success_id)
    default_work_queue.update_job_status()
    n_success = default_work_queue._queue.size(QueueItemStage.SUCCESS)
    assert n_success == 1

    # Check that it was actually moved
    n_processing = default_work_queue._queue.size(QueueItemStage.PROCESSING)
    assert n_processing == n_jobs - n_success

def test_job_fail(default_work_queue):
    """Test that job is moved after failing.
    """
    n_jobs = 5

    # Push something to the worker
    pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    success_id, _ = pushed_jobs[0]

    # Force a failure
    default_work_queue._interface.mock_fail(success_id)
    default_work_queue.update_job_status()
    n_fail = default_work_queue._queue.size(QueueItemStage.FAIL)
    assert n_fail == 1

    # Check that it was actually moved
    n_processing = default_work_queue._queue.size(QueueItemStage.PROCESSING)
    assert n_processing == n_jobs - n_fail

def test_idempotent_update(default_work_queue):
    """Test the idempotent is updated.
    """
    n_jobs = 5

    pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    success_id, _ = pushed_jobs[0]

    default_work_queue._interface.mock_success(success_id)
    default_work_queue.update_job_status()
    default_work_queue.update_job_status()

def test_push_multiple(default_work_queue):
    """Test that no job is added to the work queue twice.
    """
    n_jobs = 3

    first_pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    second_pushed_jobs = default_work_queue.push_next_jobs(n_jobs)

    # Assert that all IDs are different meaning no item was grabbed twice
    first_ids = [ i for i, _ in first_pushed_jobs ]
    second_ids = [ i for i, _ in second_pushed_jobs ]

    assert len(set(first_ids).intersection((set(second_ids)))) == 0

def test_delete_jobs(default_work_queue):
    """Tests that the work queue is deleting jobs when they terminate.
    """
    n_jobs = 3

    pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    success_id, _ = pushed_jobs[0]
    fail_id, _ = pushed_jobs[1]
    
    assert default_work_queue._queue.size(QueueItemStage.PROCESSING) == n_jobs


    # Terminate two of the jobs
    default_work_queue._interface.mock_success(success_id)
    default_work_queue._interface.mock_success(fail_id)

    default_work_queue.update_job_status()

    assert default_work_queue._queue.size(QueueItemStage.PROCESSING) == 1


