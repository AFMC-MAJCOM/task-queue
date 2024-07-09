from data_pipeline.work_queue import WorkQueue
from data_pipeline.argo_workflows_queue_worker import ArgoWorkflowsQueueWorker
from data_pipeline.s3_queue import JsonS3Queue
from data_pipeline.sql_queue import JsonSQLQueue
from data_pipeline.queue_base import QueueBase, QueueItemStage
from data_pipeline.queue_worker_interface import QueueWorkerInterface
from data_pipeline.events.sql_event_store import SqlEventStore
from data_pipeline.queue_with_events import QueueWithEvents

from typing import Callable, List
import argparse
import time
from sqlalchemy import create_engine

ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE = "argo-workflows"

JSON_S3_QUEUE_CLI_CHOICE = "s3-json"
JSON_SQL_QUEUE_CLI_CHOICE = "sql-json"

NO_EVENT_STORE_CLI_CHOICE = "none"
SQL_EVENT_STORE_CLI_CHOICE = "sql-json"

def handle_worker_interface_choice(choice:str, args):
    if choice == ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE:
        return ArgoWorkflowsQueueWorker(
            args.worker_interface_id,
            args.endpoint,
            args.namespace
        )

def handle_queue_implementation_choice(choice:str, args):
    if choice == JSON_S3_QUEUE_CLI_CHOICE:
        queue = JsonS3Queue(args.s3_base_path)
    elif choice == JSON_SQL_QUEUE_CLI_CHOICE:
        queue = JsonSQLQueue(
            create_engine(args.connection_string),
            args.queue_name
        )

    if args.with_queue_events:
        if args.event_store_implementation == SQL_EVENT_STORE_CLI_CHOICE:
            store = SqlEventStore(
                create_engine(args.connection_string)
            )

        queue = QueueWithEvents(
            queue,
            store,
            add_event_name=args.add_to_queue_event_name,
            move_event_name=args.move_queue_event_name
        )

    return queue

def start_jobs_with_processing_limit(
    max_processing_limit:int,
    queue:QueueBase,
    work_queue:WorkQueue,
):
    n_processing = queue.size(QueueItemStage.PROCESSING)
    to_start = max_processing_limit - n_processing

    # this case should never happen but I've been wrong before
    if to_start < 0:
        to_start = 0

    started_jobs = work_queue.push_next_jobs(to_start)
    print(f"start_jobs_with_processing_limit: Started {len(started_jobs)} jobs")

def main(
    periodic_functions : List[Callable[[], None]],
    work_queue:WorkQueue,
    period_sec:int=10
):
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

    # extra stuff
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

    # dynamically add extra arguments based on worker/queue choice
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

    args = parser.parse_args()

    worker_interface = handle_worker_interface_choice(
        args.worker_interface,
        args
    )

    queue = handle_queue_implementation_choice(
        args.queue_implementation,
        args
    )

    work_queue = WorkQueue(
        queue,
        worker_interface
    )

    periodic_functions = [
        lambda: start_jobs_with_processing_limit(args.processing_limit,
                                                 queue,
                                                 work_queue)
    ]

    main(periodic_functions, work_queue, args.periodic_seconds)
