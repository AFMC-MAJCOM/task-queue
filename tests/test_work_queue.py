"""Pytests for the work_queue functionality.
"""
import pytest

from task_queue.queues import QueueItemStage

@pytest.mark.unit
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

@pytest.mark.unit
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

@pytest.mark.unit
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

@pytest.mark.unit
def test_idempotent_update(default_work_queue):
    """Test the idempotent is updated.
    """
    n_jobs = 5

    pushed_jobs = default_work_queue.push_next_jobs(n_jobs)
    success_id, _ = pushed_jobs[0]

    default_work_queue._interface.mock_success(success_id)
    default_work_queue.update_job_status()
    default_work_queue.update_job_status()

@pytest.mark.unit
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

@pytest.mark.unit
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

    statuses = default_work_queue._interface.poll_all_status()

    # Dummy workflow can't delete workflows, instead they are just assinged the
    # status of None
    assert success_id not in statuses
    assert fail_id not in statuses

@pytest.mark.unit
def test_deleted_job(default_work_queue):
    """
    Test that a job which is deleted from the worker is moved to FAIL instead
    of being stuck in PROCESSING.
    """
    pushed_job = default_work_queue.push_next_jobs(1)[0]

    pushed_job_id, _ = pushed_job

    default_work_queue._interface.delete_job(pushed_job_id)

    default_work_queue.update_job_status()

    new_status = default_work_queue._queue.lookup_status(pushed_job_id)
    assert new_status == QueueItemStage.FAIL
