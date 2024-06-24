# Queue

For each queue implementation, there are the following methods:
- put :: queue_item_id, queue_item_body -> ()
    - Adds a new item to the queue in the WAITING stage
- get :: int -> List [(queue_item_id, queue_item_body)]
    - Gets the next `n` items from the queue, moving them to PROCESSING
- success :: queue_item_id -> ()
    - Moves a queue item from PROCESSING to SUCCESS
- fail :: queue_item_id -> ()
    - Moves a queue item from PROCESSING to FAIL
- size :: queue_item_stage -> int
    - How many items are in some stage of the queue (PROCESSING, FAIL, etc)
- lookup_status :: queue_item_id -> queue_item_stage
    - Lookup which stage a queue item is currently in

## Implementations

- `s3`
    - Queue items are S3 JSON objects with different prefixes to describe their current stage
- `sql`
    - Queue items are rows in a SQL table
- `in_memory`
    - Queue items are objects in a python dictionary

# Work Queue

The work queue is designed to be capable of pushing queue items to a worker and monitoring the progress of those jobs over time, automatically moving the queue items to success/fail based on the job status. To support this, there's a `QueueWorkerInterface` abstract class with two methods:

- send_job :: queue_item_id, queue_item_body -> ()
    - Starts a job from a queue item
- poll_all_status :: () -> Dict [(queue_item_id, QueueItemStage)]
    - Gets the status of all of the jobs and associated queue item IDs

## Queue Worker Implementations

### Dummy Worker

Jobs are stored as a dictionary in memory, and are manually marked as Success or Fail. Only useful for testing. 

### Argo Workflows Queue Worker

[Swagger documentation for argo workflows](https://argo-workflows.readthedocs.io/en/stable/swagger/)

Jobs are created as workflows via the submit API endpoint, and are monitored from the workflows API endpoint. Details are listed below. 

#### Important endpoints

- `POST /api/v1/workflows/{namespace}/submit`
  - Creates a workflow from a workflow manifest or workflow template with additional submit options. *Parameters and labels are the important options*. 
- `GET /api/v1/workflows/{namespace}`
  - Gets workflows from the workflow server. *Can filter by labels*. 
- `PUT /api/v1/workflows/{namespace}/{name}/(resubmit|retry|stop|terminate)`
  - Resubmit, retry, stop, terminate a workflow.

#### Implementation Details

1. `POST` a new workflow to a template when a job is submitted. Add a label for the queue item ID and the name of this queue. 
2. `GET` the status of the workflows by filtering on the label with the name of the interface, checking the label for the queue item ID, and looking at the status in the JSON response.
    - Workflow status is in `labels: workflows.argoproj.io/phase`

# Work Queue Service

The `work_queue_service_cli.py` file will run a persistent service that periodically starts new jobs from a queue's `WAITING` stage with a queue worker. It's currently configured to try to keep no more than some amount of jobs in the `PROCESSING` stage, but it should be rather easy to change. 

# Running tests

1. Get AWS CLI credentials
2. Make sure the test workflow template is deployed to argo
    - `kubectl apply -n pivot ./resources/test_workflow_template.yaml`
3. Port-forward the argo workflows server pod
    - `shift+f` on K9s or `kubectl port-forward -n pivot <pod-name> <port>:<port>`
4. Start the local postgresql and minio server
    - `docker compose -f data_pipelines/resources/docker-compose.test.yaml up`
5. pip install the `data_pipeline` package
6. `python -m pytest data_pipeline`
