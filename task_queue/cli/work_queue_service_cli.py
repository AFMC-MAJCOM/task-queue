"""Wherein is contained the functions concerning the Work Queue Service CLI.
"""
import time

from sqlalchemy import create_engine

from task_queue.logger import logger, set_logger_level
from task_queue.config import config
from task_queue.workers.work_queue import WorkQueue
from task_queue.workers.argo_workflows_queue_worker import (
                                                    ArgoWorkflowsQueueWorker)
from task_queue.queues.s3_queue import json_s3_queue
from task_queue.queues.sql_queue import json_sql_queue
from task_queue.queues.queue_base import QueueItemStage
from task_queue.events.sql_event_store import SqlEventStore
from task_queue.queues.queue_with_events import queue_with_events


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
    errors_found = ""
    validation_success = True
    if cli_args['worker_interface'] \
        == config.WorkerInterfaceChoices.ARGO_WORKFLOWS:
        required_args = ['worker_interface_id', 'endpoint', 'namespace']
        if not all(cli_args[i] is not None for i in required_args):
            value = config.WorkerInterfaceChoices.ARGO_WORKFLOWS.value
            errors_found += f"{required_args} arguments required when " \
                             "worker-interface is set to " \
                            f"{value}\n"
            validation_success = False

    elif cli_args['worker_interface'] \
        == config.WorkerInterfaceChoices.PROCESS:
        required_args = ['path_to_scripts']
        if not all(cli_args[i] is not None for i in required_args):
            value = config.WorkerInterfaceChoices.PROCESS.value
            errors_found += f"{required_args} arguments required when " \
                             "worker-interface is set to " \
                            f"{value}\n"
            validation_success = False

    if cli_args['queue_implementation'] \
        == config.QueueImplementations.SQL_JSON:
        required_args = ['connection_string', 'queue_name']
        if not all(cli_args[i] is not None for i in required_args):
            errors_found += f"{required_args} arguments required when " \
                             "queue-implementation is set to " \
                            f"{config.QueueImplementations.SQL_JSON.value}\n"
            validation_success = False

    elif cli_args['queue_implementation'] \
        == config.QueueImplementations.S3_JSON:
        required_args = ['s3_base_path']
        if not all(cli_args[i] is not None for i in required_args):
            errors_found += f"{required_args} arguments required when " \
                             "queue-implementation is set to " \
                            f"{config.QueueImplementations.S3_JSON.value}\n"
            validation_success = False

    if cli_args['event_store_implementation'] \
        != config.EventStoreChoices.NO_EVENTS:
        required_args = ['add_to_queue_event_name', 'move_queue_event_name']
        if not all(cli_args[i] is not None for i in required_args):
            errors_found += f"{required_args} arguments required when " \
                             "event-store-implementation is not " \
                            f"{config.EventStoreChoices.NO_EVENTS.value}\n"
            validation_success = False

    if cli_args['with_queue_events']:
        if cli_args['event_store_implementation'] \
                                != config.EventStoreChoices.SQL_JSON:

            errors_found += f"If with_queue_events is specificied, " \
                             "event_store_implementation must be set to " \
                            f"{config.EventStoreChoices.SQL_JSON.value}"
            validation_success = False

    if cli_args['logger_level']:
        log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if cli_args['logger_level'] not in log_levels:
            errors_found = f"logger_level must be {log_levels}"
            validation_success = False

    return validation_success, errors_found

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
    elif cli_settings.worker_interface \
        == config.WorkerInterfaceChoices.PROCESS:
        return ProcessWorkerInterface(cli_settings.path_to_scripts)
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


def main(periodic_functions, work_queue, period_sec=10):
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

        logger.info("Running periodic functions")
        for fn in periodic_functions:
            fn()

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

    unique_periodic_functions = [
        lambda: start_jobs_with_processing_limit(settings.processing_limit,
                                                 unique_work_queue)
    ]

    main(unique_periodic_functions,
         unique_work_queue,
         settings.periodic_seconds)
