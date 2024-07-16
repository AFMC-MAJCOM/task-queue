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

### Running Tests in AWS

1. Get AWS CLI credentials:
2. Make sure the test workflow template is deployed to argo
    - `kubectl apply -n pivot task-queue/resources/test_workflow_template.yaml`
3. Port-forward the argo workflows server pod
    - `shift+f` on K9s or `kubectl port-forward -n pivot <pod-name> <port>:<port>`
4. Start the local postgresql and minio server
    - `docker compose -f task-queue/resources/docker-compose.test.yaml up`
5. pip install the `task-queue` package
6. `python -m pytest task-queue`

### Running Argo Workflow Tests in minikube

1. Spin up the pods:
    - `minikube start`
    - `kubectl create namespace pivot`
    - `kubectl apply -n pivot -f task-queue/resources/quick-start-minimal.yaml`
2. Make sure the test workflow template is deployed to argo
    - `kubectl apply -n pivot task-queue/resources/test_workflow_template.yaml`
    - If you have the Argo CLI installed, you can use:
    - `argo -n pivot template create resources/test_workflow_template.yaml`
3. Port-forward the argo workflows server pod
    - `shift+f` on K9s or `kubectl port-forward -n pivot service/argo-server 2746:2746`
5. pip install the `task-queue` package (you will likely need to run this in a python virtual environment)
6. `python -m pytest task-queue/tests/test_argo_workflows_worker_interface.py`


### S3 Connection
An external MinIO or S3 service is required for the tests to run and `AWS_ACCESS_KEY_ID`
`AWS_SECRET_ACCESS_KEY` and `FSSPEC_S3_ENDPOINT_URL` environment variables must be set.

# Starting Work Queue Server and CLI

You will need docker installed on you machine to use the Task Queue.

## Downloading Docker Image

To download the docker image run the following command.

```
docker pull ghcr.io/afmc-majcom/task-queue/task-queue:latest
```

You should see the task queue image listed out by running the following command.

```
docker image ls
```

## Running the Docker Image

### (Optional) Starting a Local Postgres Server

This step is necessary for testing or if you want a local postgres server running. To run a postgres server locally run the following command.

```
docker pull postgres 
docker run --name my-postgres -e POSTGRES_PASSWORD=my_password -d -p 5432:5432 postgres
```

You can modify the ports, passwords, and name to what you would like them to be but make sure the next steps reflect those changes.

### Running the Server

Before you can do this step you need to have a postgres server running and access to the server. Once you have that you can create the env.list file. The env.list file requires the following values. These values are based on running postgres from a docker container with the docker run command above.

```
QUEUE_IMPLEMENTATION=sql-json
SQL_QUEUE_CONNECTION_STRING=postgresql://postgres:my_password@host.docker.internal:5432/postgres
SQL_QUEUE_NAME=queue_name
```

The container can then be spun up using the below command.

```
docker run --rm -p 8001:80 --env-file ./env.list ghcr.io/afmc-majcom/task-queue/task-queue:latest server 
```

(Optional) If you started your own local postgres server and are using a linux machine run this command instead.

```
docker run --add-host=host.docker.internal:host-gateway --rm -p 8001:80 --env-file ./env.list ghcr.io/afmc-majcom/task-queue/task-queue:latest server 
```

### Running the CLI

Run the following command to output help from CLI

```
docker run --rm ghcr.io/afmc-majcom/task-queue/task-queue:latest controller --help
```

