"""Wherein is contained the functions concerning the Work Queue Service CLI.
"""
import time

from sqlalchemy import create_engine

from task_queue.logger import logger, set_logger_level
from task_queue.config import config
from task_queue.workers.work_queue import WorkQueue
from task_queue.workers.process_queue_worker import ProcessQueueWorker
from task_queue.workers.argo_workflows_queue_worker import (
                                                    ArgoWorkflowsQueueWorker)
from task_queue.queues.s3_queue import json_s3_queue
from task_queue.queues.sql_queue import json_sql_queue
from task_queue.queues.queue_base import QueueItemStage
from task_queue.queues.in_memory_queue import InMemoryQueue

from task_queue.events.sql_event_store import SqlEventStore
from task_queue.queues.queue_with_events import queue_with_events
from task_queue.job_release_strategy import (
    ProcessingLimit,
    ResourceLimit,
    ReleaseAll
)

def validate_required_args_groups(cli_args, required_args, field, value):
    if not all(cli_args[i] is not None for i in required_args):
        errors_found += f"{required_args} arguments required when " \
                        f"{field} is set to " \
                        f"{value}\n"
        return False, errors_found
    return True, ""

# Pylint does not like how many if/elif branches we have in this function
# pylint: disable=R0912
def validate_args(cli_args):
    """Validates input arguments given to the CLI.

    Parameters:
    -----------
    cli_args: dict
        Python dictionary representing the arguments passed to the CLI.
        (parser.parse_args().__dict__)

    Returns:
    -----------
    validation_success: bool
        True if the arguments met the requirements, False otherwise.
    errors_found: str
        Error/s found if any. Empty string if validation_success = True.
    """
    errors = []
    validation_success = []

    worker_interface_choice = cli_args['worker_interface']

    if worker_interface_choice \
        == config.WorkerInterfaceChoices.ARGO_WORKFLOWS:
        required_args = ['worker_interface_id', 'endpoint', 'namespace']
        error, valid = validate_required_args_groups(
            cli_args,
            required_args,
            'worker_interface',
            config.WorkerInterfaceChoices.ARGO_WORKFLOWS
        )
        errors.append(error)
        validation_success.append(valid)

    elif worker_interface_choice \
        == config.WorkerInterfaceChoices.PROCESS:
        required_args = ['path_to_scripts']
        error, valid = validate_required_args_groups(
            cli_args,
            required_args,
            'worker_interface',
            config.WorkerInterfaceChoices.PROCESS
        )
        errors.append(error)
        validation_success.append(valid)

    queue_implementation_choice = cli_args['queue_implementation']

    if queue_implementation_choice \
        == config.QueueImplementations.SQL_JSON:
        required_args = ['connection_string', 'queue_name']
        error, valid = validate_required_args_groups(
            cli_args,
            required_args,
            'queue_implementation',
            config.QueueImplementations.SQL_JSON
        )
        errors.append(error)
        validation_success.append(valid)

    elif queue_implementation_choice \
        == config.QueueImplementations.S3_JSON:
        required_args = ['s3_base_path']
        error, valid = validate_required_args_groups(
            cli_args,
            required_args,
            'queue_implementation',
            config.QueueImplementations.S3_JSON
        )
        errors.append(error)
        validation_success.append(valid)

    if cli_args['event_store_implementation'] \
        != config.EventStoreChoices.NO_EVENTS:
        required_args = ['add_to_queue_event_name', 'move_queue_event_name']
        error, valid = validate_required_args_groups(
            cli_args,
            required_args,
            'event_sore_implementation',
            config.EventStoreChoices.SQL_JSON
        )
        errors.append(error)
        validation_success.append(valid)

    if cli_args['with_queue_events']:
        if cli_args['event_store_implementation'] \
                                != config.EventStoreChoices.SQL_JSON:

            errors_found += f"If with_queue_events is specificied, " \
                             "event_store_implementation must be set to " \
                            f"{config.EventStoreChoices.SQL_JSON.value}"
            validation_success = False

    all_valid = all(validation_success)
    error_string = "\n".join([ e for e in errors_found if e ])

    return all_valid, error_string

def handle_worker_interface_choice(cli_settings):
    """Handles the worker interface choice.

    Parameters:
    -----------
    cli_settings: TaskQueueCliSettings
        Configuration object for the CLI

    Returns:
    -----------
    Selects the worker interface from the arguments. Currently only the
    ArgoWorkflowsQueueWorker is implemented.
    """
    if cli_settings.worker_interface \
        == config.WorkerInterfaceChoices.ARGO_WORKFLOWS:
        return ArgoWorkflowsQueueWorker(
            cli_settings.worker_interface_id,
            cli_settings.endpoint,
            cli_settings.namespace
        )
    if cli_settings.worker_interface \
        == config.WorkerInterfaceChoices.PROCESS:
        return ProcessQueueWorker(cli_settings.path_to_scripts)
    return None

def handle_queue_implementation_choice(cli_settings):
    """Handles the queue implementation choice.

    Parameters:
    -----------
    cli_settings: TaskQueueCliSettings
        Configuration object for the CLI

    Returns:
    -----------
    Constructs the queue implementation from the arguments.
    """
    if cli_settings.queue_implementation \
        == config.QueueImplementations.S3_JSON:
        s3_settings = config.get_task_queue_settings(
            setting_class = config.TaskQueueS3Settings
        )
        s3_settings.log_settings()
        queue = json_s3_queue(cli_settings.s3_base_path)
    elif cli_settings.queue_implementation \
        == config.QueueImplementations.SQL_JSON:
        sql_settings = config.get_task_queue_settings(
            setting_class = config.TaskQueueSqlSettings
        )
        sql_settings.log_settings()
        queue = json_sql_queue(
            create_engine(cli_settings.connection_string),
            cli_settings.queue_name
        )
    elif cli_settings.queue_implementation \
         == config.QueueImplementations.IN_MEMORY:
        queue = InMemoryQueue()

    if cli_settings.with_queue_events:
        store = None
        if cli_settings.event_store_implementation \
            == config.EventStoreChoices.SQL_JSON:
            store = SqlEventStore(
                create_engine(cli_settings.connection_string)
            )
        else:
            raise AttributeError("SQL_JSON is the only implemented event store"
                                  " that works with with_queue_events")

        queue = queue_with_events(
            queue,
            store,
            add_event_name=cli_settings.add_to_queue_event_name,
            move_event_name=cli_settings.move_queue_event_name
        )

    return queue


def handle_job_release_strategy_choice(cli_settings):
    """Handles the job release strategy choice.

    Parameters:
    -----------
    cli_settings: TaskQueueCliSettings
        Configuration object for the CLI

    Returns:
    -----------
    Constructs the job release strategy from the arguments.
    """

    if cli_settings.resource_limits:
        job_release_strategy = ResourceLimit(
            cli_settings.resource_limits,
            cli_settings.resource_key
        )
    elif cli_settings.processing_limit:
        job_release_strategy = ProcessingLimit(
            cli_settings.processing_limit
        )
    else:
        job_release_strategy = ReleaseAll()

    return job_release_strategy


def start_jobs_with_processing_limit(max_processing_limit,
                                     work_queue
                                    ):
    """Pushes jobs without exceed the processing limit.

    Parameters:
    -----------
    max_processing_limit: str
        Max processing limit.
    queue: QueueBase
        Used to get add, use, and get the status of items in the queue.
    work_queue: WorkQueue
        Work Queue to orchestrate jobs.
    """
    n_processing = work_queue.get_queue_size(QueueItemStage.PROCESSING)
    to_start = max_processing_limit - n_processing

    to_start = max(to_start, 0)

    started_jobs = work_queue.push_next_jobs(to_start)
    logger.info("start_jobs_with_processing_limit: Started %s jobs", \
                len(started_jobs))


def main(job_release_strategy, work_queue, period_sec=10):
    """Main function, runs functions periodically with a set time to wait.

    Parameters:
    -----------
    periodic_functions: List[Callable[[], None]]
        Functions to run periodically.
    work_queue: WorkQueue
        Work Queue
    period_sec: int (default=10)
        Number of seconds to wait between running periodic functions.
    """
    while True:
        logger.info("Updating job statuses")
        work_queue.update_job_status()

        logger.info("Releasing new jobs")
        job_release_strategy.release_next_jobs(work_queue)

        time.sleep(period_sec)


if __name__ == "__main__":


    settings = config.get_task_queue_settings(
        setting_class=config.TaskQueueCliSettings
    )
    set_logger_level(settings.logger_level)
    settings.log_settings()

    # Check if dependent arguments were provided
    # This validation could be reworked into the settings model
    success, error_string = validate_args(settings.model_dump())

    if not success:
        logger.error(error_string)
        raise ValueError("\n" + error_string)

    unique_worker_interface = handle_worker_interface_choice(
        settings
    )

    unique_queue = handle_queue_implementation_choice(
        settings,
    )

    unique_work_queue = WorkQueue(
        unique_queue,
        unique_worker_interface
    )

    unique_job_release_strategy = handle_job_release_strategy_choice(
        settings
    )
    main(unique_job_release_strategy,
         unique_work_queue,
         settings.periodic_seconds)
