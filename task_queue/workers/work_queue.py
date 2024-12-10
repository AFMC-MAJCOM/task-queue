"""Wherein is contained the WorkQueue class.
"""
from task_queue.queues.queue_base import QueueItemStage, QueueBase
from task_queue import logger


class WorkQueue():
    """Class for the WorkQueue initialization and supporting functions.
    """
    def __init__(self, queue:QueueBase, interface):
        """Initializes Work Queue.

        Parameters:
        -----------
        queue: QueueBase
        interface: QueueWorkerInterface
        """
        self._queue = queue
        self._interface = interface
        self._cached_statuses = {}

    @property
    def queue(self):
        return self._queue

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
            except Exception:
                # Error in submission -> fail
                logger.warning("Item %s failed on submission", queue_item_id)
                logger.warning("Moving %s to failed", queue_item_id)
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

        processing_items = self._queue.lookup_state(QueueItemStage.PROCESSING)

        # update all items in processing
        for queue_item_id in processing_items:
            # default to None if the item id is not in the list of statuses
            # returned by the queue worker. This prevents jobs that were
            # deleted externally from getting stuck in `PROCESSING` eternally.
            status = statuses.get(queue_item_id, None)

            if status is None:
                # no need to delete here, because this case is only reached
                # when the item has already been deleted.
                self._queue.fail(queue_item_id)
            elif status == QueueItemStage.SUCCESS:
                self._queue.success(queue_item_id)
                self._interface.delete_job(queue_item_id)
            elif status == QueueItemStage.FAIL:
                self._queue.fail(queue_item_id)
                self._interface.delete_job(queue_item_id)

        return statuses
