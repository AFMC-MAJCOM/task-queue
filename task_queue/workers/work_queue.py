"""Wherein is contained the WorkQueue class.
"""
from task_queue.queues.queue_base import QueueItemStage
from task_queue import logger


class WorkQueue():
    """Class for the WorkQueue initialization and supporting functions.
    """
    def __init__(self, queue, interface):
        """Initializes Work Queue.

        Parameters:
        -----------
        queue: QueueBase
        interface: QueueWorkerInterface
        """
        self._queue = queue
        self._interface = interface
        self._cached_statuses = {}

    def get_queue_size(self, queue_item_stage):
        """Gets the queue size for the given QueueItemStage stage.

        Parameters:
        -----------
        queue_item_stage: enum
               The requested enum from QueueItemStage

        Returns:
        -----------
        The queue size from WorkQueue for the given stage.
        """
        queue_size = self._queue.size(queue_item_stage)
        return queue_size

    # Pylint disabled because any except is used to call the queue fail
    # pylint: disable=broad-exception-caught
    def push_next_jobs(self, n_jobs=None):
        """Sends jobs from Queue.

        Parameters:
        -----------
        n_jobs: int (default=None)
            Number of jobs to send.

        Returns:
        -----------
        Returns the jobs selected from Queue.
        """
        if n_jobs is None:
            n_jobs = 1

        next_items = self._queue.get(n_jobs)

        for queue_item_id, queue_item_body in next_items:
            try:
                self._interface.send_job(queue_item_id, queue_item_body)
            except Exception as e:
                # Error in submission -> fail
                logger.warn(f"Item {queue_item_id} failed on submission")
                logger.warn(f"Moving {queue_item_id} to failed")
                self._queue.fail(queue_item_id)

        return next_items


    def update_job_status(self):
        """Updates job statuses in Queue.

        Returns:
        -----------
        Returns dictionary of all statuses as Dict[Any, QueueItemStage]
        """
        statuses = self._interface.poll_all_status()

        logger.info("Processing new statuses from worker interface")
        for queue_item_id, status in statuses.items():
            # Not in processing -> don't care
            if self._queue.lookup_status(queue_item_id) != \
                QueueItemStage.PROCESSING:
                continue

            if status == QueueItemStage.SUCCESS:
                self._queue.success(queue_item_id)
                self._interface.delete_job(queue_item_id)
            elif status == QueueItemStage.FAIL:
                self._queue.fail(queue_item_id)
                self._interface.delete_job(queue_item_id)

        return statuses
