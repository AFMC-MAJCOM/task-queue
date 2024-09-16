"""Wherein is contained the class for the Process Queue Worker.
"""
from multiprocessing import Process
from subprocess import run
from pydantic import validate_call

from task_queue.workers.queue_worker_interface import QueueWorkerInterface
from task_queue.queues.queue_base import QueueItemStage
from task_queue import logger
from task_queue.queue_pydantic_models import ProcessWorkerModel


class ProcessQueueWorker(QueueWorkerInterface):
    """Process Queue Worker Class

    Jobs are completed using processes, steps to be completed for each job are
    stored in python scripts outside of the task-queue package.
    """

    def __init__(self, path_to_scripts):
        """Initializes ProcessQueueWorker.
        """
        self.path_to_scripts = path_to_scripts
        self._active_processes = {}

    def start_job(self, item_id, queue_item_body):
        """Target function to run python script specified in queue item body.

        Parameters:
        -----------
        item_id:
            ID of Queue Item
        queue_item_body: dict
            Dictionary with contents required to run python scipts. Follows the
            ProcessWorkerModel schema:
            {
                "file_name": 'name_of_script.py'
                "args": ['list','of','args'] or None
            }
        """
        filepath = f"{self.path_to_scripts}/{queue_item_body.file_name}"
        try:
            # Run python script found at filepath with 0+ args
            command = ['python3', filepath]
            if queue_item_body.args:
                command += (queue_item_body.args)
            result = run(
                command,
                capture_output=True,
                text=True,
                check=False
            )
            logger.info(result.stdout)
            # Catch error that occured when running script
            if len(result.stderr) > 0:
                logger.error(result.stderr)
                raise RuntimeError(
                    f"Error occured while running {filepath} for queue item: "
                    f"{item_id}\nError: {result.stderr}"
                )
        except Exception as e:
            logger.error(e)
            raise e

    @validate_call
    def send_job(self, item_id, queue_item_body:ProcessWorkerModel):
        """Starts a job from queue item.

        Parameters:
        -----------
        item_id: str
            Queue Item ID
        queue_item_body: dict
            Dictionary that must match the ProcessWorkerModel schema:
            {
                "file_name": 'name_of_script.py'
                "args": ['list','of','args'] or None
            }
        """
        p = Process(target=self.start_job, args=(item_id,queue_item_body,))
        self._active_processes[item_id] = p
        p.start()

    def delete_job(self, queue_item_id):
        """Clears up any remaining resources being used by that process.

        Parameters:
        -----------
        queue_item_id: str
            Queue Item ID to delete from dictionary of active processes
        """
        p = self._active_processes.pop(queue_item_id)
        p.close()

    def poll_all_status(self):
        """Poll status of all jobs sent by the worker interface.

        Returns:
        -----------
        Returns Dict[item_id, QueueItemStage] of job statuses.
        """
        statuses = {}
        for id_,p in self._active_processes.items():
            if p.exitcode is None:
                statuses[id_] = QueueItemStage.PROCESSING
            elif p.exitcode == 0:
                statuses[id_] = QueueItemStage.SUCCESS
            else:
                statuses[id_] = QueueItemStage.FAIL
        return statuses
