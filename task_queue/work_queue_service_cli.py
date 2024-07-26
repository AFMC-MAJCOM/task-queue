"""Wherein is contained the functions concerning the Work Queue Service CLI.
"""
import argparse
import time

from sqlalchemy import create_engine

from task_queue.work_queue import WorkQueue
from task_queue.argo_workflows_queue_worker import ArgoWorkflowsQueueWorker
from task_queue.s3_queue import json_s3_queue
from task_queue.sql_queue import json_sql_queue
from task_queue.queue_base import QueueItemStage
from task_queue.events.sql_event_store import SqlEventStore
from task_queue.queue_with_events import queue_with_events


ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE = "argo-workflows"

JSON_S3_QUEUE_CLI_CHOICE = "s3-json"
JSON_SQL_QUEUE_CLI_CHOICE = "sql-json"

NO_EVENT_STORE_CLI_CHOICE = "none"
SQL_EVENT_STORE_CLI_CHOICE = "sql-json"

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
    if cli_args['worker_interface'] == ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE:
        required_args = ['worker_interface_id', 'endpoint', 'namespace']
        if not all(cli_args[i] is not None for i in required_args):
            errors_found += f"{required_args} arguments required when " \
                             "worker-interface is set to " \
                            f"{ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}\n"
            validation_success = False

    if cli_args['queue_implementation'] == JSON_SQL_QUEUE_CLI_CHOICE:
        required_args = ['connection_string', 'queue_name']
        if not all(cli_args[i] is not None for i in required_args):
            errors_found += f"{required_args} arguments required when " \
                             "queue-implementation is set to " \
                            f"{JSON_SQL_QUEUE_CLI_CHOICE}\n"
            validation_success = False

    elif cli_args['queue_implementation'] == JSON_S3_QUEUE_CLI_CHOICE:
        required_args = ['s3_base_path']
        if not all(cli_args[i] is not None for i in required_args):
            errors_found += f"{required_args} arguments required when " \
                             "queue-implementation is set to " \
                            f"{JSON_S3_QUEUE_CLI_CHOICE}\n"
            validation_success = False

    if cli_args['event_store_implementation'] != NO_EVENT_STORE_CLI_CHOICE:
        required_args = ['add_to_queue_event_name', 'move_queue_event_name']
        if not all(cli_args[i] is not None for i in required_args):
            errors_found += f"{required_args} arguments required when " \
                             "event-store-implementation is not " \
                            f"{NO_EVENT_STORE_CLI_CHOICE}\n"
            validation_success = False

    if cli_args['with_queue_events']:
        if cli_args['event_store_implementation'] \
                                != SQL_EVENT_STORE_CLI_CHOICE:

            errors_found += f"If with_queue_events is specificied, " \
                             "event_store_implementation must be set to " \
                            f"{SQL_EVENT_STORE_CLI_CHOICE}"
            validation_success = False

    return validation_success, errors_found

def handle_worker_interface_choice(choice, args):
    """Handles the worker interface choice.

    Parameters:
    -----------
    choice: str
        Choice provided by the worker_interface argument passed to the CLI.

    Returns:
    -----------
    Selects the worker interface from the arguments. Currently only the
    ArgoWorkflowsQueueWorker is implemented.
    """
    if choice == ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE:
        return ArgoWorkflowsQueueWorker(
            args.worker_interface_id,
            args.endpoint,
            args.namespace
        )
    return None

def handle_queue_implementation_choice(choice, args):
    """Handles the queue implementation choice.

    Parameters:
    -----------
    choice: str
        Choice provided by the queue_implementation argument passed to the CLI.

    Returns:
    -----------
    Constructs the queue implementation from the arguments.
    """
    if choice == JSON_S3_QUEUE_CLI_CHOICE:
        queue = json_s3_queue(args.s3_base_path)
    elif choice == JSON_SQL_QUEUE_CLI_CHOICE:
        queue = json_sql_queue(
            create_engine(args.connection_string),
            args.queue_name
        )

    if args.with_queue_events:
        store = None
        if args.event_store_implementation == SQL_EVENT_STORE_CLI_CHOICE:
            store = SqlEventStore(
                create_engine(args.connection_string)
            )

        queue = queue_with_events(
            queue,
            store,
            add_event_name=args.add_to_queue_event_name,
            move_event_name=args.move_queue_event_name
        )

    return queue

def start_jobs_with_processing_limit(max_processing_limit,
                                     queue,
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
    n_processing = queue.size(QueueItemStage.PROCESSING)
    to_start = max_processing_limit - n_processing

    to_start = max(to_start, 0)

    started_jobs = work_queue.push_next_jobs(to_start)
    print(f"start_jobs_with_processing_limit: Started \
        {len(started_jobs)} jobs")

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
        print("Updating job statuses")
        work_queue.update_job_status()

        print("Running periodic functions")
        for fn in periodic_functions:
            fn()

        time.sleep(period_sec)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "worker_interface",
        choices=[ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE],
        help="worker-interface: Service used to run jobs."
    )

    parser.add_argument(
        "queue_implementation",
        choices=[
            JSON_S3_QUEUE_CLI_CHOICE,
            JSON_SQL_QUEUE_CLI_CHOICE
        ],
        help="queue-implementation: Service used to store the queue."
    )

    parser.add_argument(
        "event_store_implementation",
        choices=[
            NO_EVENT_STORE_CLI_CHOICE,
            SQL_EVENT_STORE_CLI_CHOICE
        ],
        default=NO_EVENT_STORE_CLI_CHOICE,
        help="event-store-implementation: "
             "Service used to store logs of queue state changes."
    )

    parser.add_argument(
        "--with-queue-events",
        type=bool,
        default=False,
        help="Flag to signify that logs should be stored on "
             "queue state changes. The 'event_store_implementation' "
             "argument should be set to 'sql-json' "
             "when including this flag."
    )

    parser.add_argument(
        "--processing-limit",
        default=10,
        type=int,
        help="Number of jobs to be run concurrently."
    )
    parser.add_argument(
        "--periodic-seconds",
        default=10,
        help="Number of seconds to wait before checking if "
             "additional jobs can be submitted."
    )

    parser.add_argument("--worker-interface-id",
                        help="User defined ID for the worker interface "
                             "used to submit jobs. Can be any unique "
                             "string. Required when worker-interface is set "
                             f" to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}")

    parser.add_argument("--endpoint",
                        help="Endpoint URL used to point to the ARGO "
                             "Workflows API. Required when worker-interface "
                            f"is set to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}")

    parser.add_argument("--namespace",
                        help="Kubernetes namespace where ARGO Workflows is "
                             "running. Required when worker-interface is set "
                            f"to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}")

    parser.add_argument("--connection-string",
                        help="Connection string associated with an external "
                             "SQL server. Required when queue-implementation "
                             f"is set to {JSON_SQL_QUEUE_CLI_CHOICE}")

    parser.add_argument("--queue-name",
                        help="User defined queue name. Can be any unique "
                             "string. Required when queue-implementation "
                            f"is set to {JSON_SQL_QUEUE_CLI_CHOICE}")

    parser.add_argument("--s3-base-path",
                        help="S3 path where the queue will be stored. "
                             "Required when queue-implementation is set to "
                            f"{JSON_S3_QUEUE_CLI_CHOICE}")

    parser.add_argument("--add-to-queue-event-name",
                        help="User defined event name used in the logs "
                             "when queue items are added. Required when "
                             "event-store-implementation is not set to "
                            f"{NO_EVENT_STORE_CLI_CHOICE}")

    parser.add_argument("--move-queue-event-name",
                        help="User defined event name used in the logs "
                             "when queue items are moved. Required when "
                             "event-store-implementation is not set to "
                            f"{NO_EVENT_STORE_CLI_CHOICE}")

    unique_args = parser.parse_args()

    # Check if dependent arguments were provided
    args_dict = unique_args.__dict__
    print(args_dict['with_queue_events'])
    success, error_string = validate_args(args_dict)

    if not success:
        parser.error(error_string)

    unique_worker_interface = handle_worker_interface_choice(
        unique_args.worker_interface,
        unique_args
    )

    unique_queue = handle_queue_implementation_choice(
        unique_args.queue_implementation,
        unique_args
    )

    unique_work_queue = WorkQueue(
        unique_queue,
        unique_worker_interface
    )

    unique_periodic_functions = [
        lambda: start_jobs_with_processing_limit(unique_args.processing_limit,
                                                 unique_queue,
                                                 unique_work_queue)
    ]

    main(unique_periodic_functions,
         unique_work_queue,
         unique_args.periodic_seconds)
