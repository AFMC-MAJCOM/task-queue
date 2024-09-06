"""Wherein is contained the class for the Process Queue Worker.
"""
from multiprocessing import Process

from task_queue.workers.queue_worker_interface import QueueWorkerInterface
from task_queue.queues.queue_base import QueueItemStage
from task_queue import logger


class ProcessWorkerInterface(QueueWorkerInterface):
    """Process Worker Interface Class

    Jobs are completed using processes, steps to be completed for each job are
    stored in python scripts outside of the task-queue package.
    """

    def __init__(self, path_to_scripts):
        """Initializes ProcessWorkerInterface.
        """
        self.path_to_scripts = path_to_scripts
        self._active_processes = {}

    def start_job(self, item_id, queue_item_body):
        """Target function to run python script found in queue item body.

        ADD MORE
        """
        filepath = f"{self.path_to_scripts}/{queue_item_body['file_name']}"
        try:
            with open(filepath, 'r') as f:
                file_code = f.read()
            logger.info(f"Executing script {filepath}..")
            exec(file_code, {}, {"input_data":queue_item_body["parameters"]}) # security concerns? add safe_globals and locals
        except FileNotFoundError as e:
            logger.info(f"{filepath} not found.")
            logger.error(e)
            raise e
        except Exception as e:
            logger.info(f"An error occured while running the script: {e}")
            logger.error(e)
            raise e

    def send_job(self, item_id, queue_item_body):
        """Starts a job from queue item.

        Parameters:
        -----------
        item_id: str
            Queue Item ID
        queue_item_body: dict
            Dictionary that must contain a key:value pair where INSERT HERE
        """
        p = Process(target=self.start_job, args=(item_id,queue_item_body,))
        self._active_processes[item_id] = p
        p.start()

    def delete_job(self, queue_item_id):
        """Clears up any remaining resources being used by that process.

        Parameters:
        -----------
        queue_item_id: str
            Queue Item ID
        """
        # p = queue_item_id.pop('process')
        # p.close()

    def poll_all_status(self):
        """Poll status of all jobs sent by the worker interface.

        Returns:
        -----------
        Returns Dict[Any, QueueItemStage] of job statuses.
        """
        statuses = {}
        for i in self._active_processes:
            p = self._active_processes[i]
            if p.exitcode is None:
                statuses[i] = QueueItemStage.PROCESSING
            elif p.exitcode == 0:
                statuses[i] = QueueItemStage.SUCCESS
            else:
                statuses[i] = QueueItemStage.FAIL
        return statuses

