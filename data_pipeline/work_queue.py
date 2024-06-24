from .queue_worker_interface import QueueWorkerInterface
from .queue_base import QueueBase, QueueItemStage

class WorkQueue():
    def __init__(
        self, 
        queue:QueueBase, 
        interface:QueueWorkerInterface
    ):
        self._queue = queue
        self._interface = interface
        self._cached_statuses = {}


    def push_next_jobs(self, n_jobs=None):
        if n_jobs is None:
            n_jobs = 1

        next_items = self._queue.get(n_jobs)

        for queue_item_id, queue_item_body in next_items:
            try:
                self._interface.send_job(queue_item_id, queue_item_body)
            except:
                # Error in submission -> fail
                self._queue.fail(queue_item_id)

        return next_items


    def update_job_status(self):
        statuses = self._interface.poll_all_status()

        # new_statuses = {
        #     k : v
        #     for k,v in statuses.items()
        #     if k not in self._cached_statuses or self._cached_statuses[k] != v
        # }

        print("Processing new statuses from worker interface")
        for queue_item_id, status in statuses.items():
            # Not in processing -> don't care
            if self._queue.lookup_status(queue_item_id) != QueueItemStage.PROCESSING:
                continue

            if status == QueueItemStage.SUCCESS:
                self._queue.success(queue_item_id)
            elif status == QueueItemStage.FAIL:
                self._queue.fail(queue_item_id)

        # self._cached_statuses = new_statuses

        return statuses
