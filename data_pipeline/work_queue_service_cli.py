"""Wherein is contained the functions concerning the Work Queue Service CLI.
"""
import argparse
import time

from sqlalchemy import create_engine

from data_pipeline.work_queue import WorkQueue
from data_pipeline.argo_workflows_queue_worker import ArgoWorkflowsQueueWorker
from data_pipeline.s3_queue import json_s3_queue
from data_pipeline.sql_queue import json_sql_queue
from data_pipeline.queue_base import QueueItemStage
from data_pipeline.events.sql_event_store import SqlEventStore
from data_pipeline.queue_with_events import queue_with_events


ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE = "argo-workflows"

JSON_S3_QUEUE_CLI_CHOICE = "s3-json"
JSON_SQL_QUEUE_CLI_CHOICE = "sql-json"

NO_EVENT_STORE_CLI_CHOICE = "none"
SQL_EVENT_STORE_CLI_CHOICE = "sql-json"

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
        choices=[
            ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE
        ]
    )
    parser.add_argument(
        "queue_implementation",
        choices=[
            JSON_S3_QUEUE_CLI_CHOICE,
            JSON_SQL_QUEUE_CLI_CHOICE
        ]
    )
    parser.add_argument(
        "event_store_implementation",
        choices=[
            NO_EVENT_STORE_CLI_CHOICE,
            SQL_EVENT_STORE_CLI_CHOICE
        ],
        default=NO_EVENT_STORE_CLI_CHOICE
    )
    parser.add_argument(
        "--with-queue-events",
        type=bool,
        default=False
    )

    # Extra stuff
    parser.add_argument(
        "--processing-limit",
        default=10,
        type=int
    )
    parser.add_argument(
        "--periodic-seconds",
        default=10
    )

    known_args, _ = parser.parse_known_args()

    # Dynamically add extra arguments based on worker/queue choice
    if known_args.worker_interface == ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE:
        parser.add_argument("--worker-interface-id", required=True)
        parser.add_argument("--endpoint", required=True)
        parser.add_argument("--namespace", required=True)

    if known_args.queue_implementation == JSON_SQL_QUEUE_CLI_CHOICE:
        parser.add_argument("--connection-string", required=True)
        parser.add_argument("--queue-name", required=True)
    elif known_args.queue_implementation == JSON_S3_QUEUE_CLI_CHOICE:
        parser.add_argument("--s3-base-path", required=True)

    if known_args.event_store_implementation != NO_EVENT_STORE_CLI_CHOICE:
        parser.add_argument("--add-to-queue-event-name", required=True)
        parser.add_argument("--move-queue-event-name", required=True)

    unique_args = parser.parse_args()

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
