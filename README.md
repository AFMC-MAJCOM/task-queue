# Task Queue

## Task Queue Architecture

<h1 align="center">
<img src="./task_queue_architecture.png" width="750">
</h1>

# Queue

For each queue implementation, there are the following methods:
- put :: items -> ()
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
- lookup_state :: queue_item_stage -> List[queue_item_id]
    - Lookup all the item ids in the given gueue item stage
- lookup_item :: item_id -> (item_id, status, item_body)
    - Lookup an item in the queue
- description :: () -> dict
    - Provides a brief description of the queue.
- requeue :: ([item_id]) -> [Requed_Ids]
    - requeue move an item from FAIL to WAITING

## Implementations

- `s3`
    - Queue items are S3 JSON objects with different prefixes to describe their current stage
- `sql`
    - Queue items are rows in a SQL table
- `in_memory`
    - Queue items are objects in a python dictionary
- `with_events`
    - Queue items are stored in the queue implementation of your choice and item movement is tracked as events in an event store

# Work Queue

The work queue is designed to be capable of pushing queue items to a worker and monitoring the progress of those jobs over time, automatically moving the queue items to success/fail based on the job status. To support this, there's a `QueueWorkerInterface` abstract class with two methods:

- send_job :: queue_item_id, queue_item_body -> ()
    - Starts a job from a queue item
- poll_all_status :: () -> Dict [(queue_item_id, QueueItemStage)]
    - Gets the status of all of the jobs and associated queue item IDs
- delete_job :: queue_item_id -> ()
    - Deletes a job from the specific workflow/process after the job completes and the queue is updated.

## Queue Worker Implementations

### Dummy Worker

Jobs are stored as a dictionary in memory, and are manually marked as Success or Fail. Only useful for testing.

### Argo Workflows Queue Worker

[Swagger documentation for argo workflows](https://argo-workflows.readthedocs.io/en/stable/swagger/)

Jobs are created as workflows via the submit API endpoint, and are monitored from the workflows API endpoint. Details are listed below.

### Process Worker

Jobs are created in Python Processes and each process will run a python script provided by the user.

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

# Configuring the Task Queue

The Task Queue library uses [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) to handle configuration and parameterization. This library handles reading in parameters from disparate sources, validating inputs, and automatically casting values to the correct data types. The order of priority for parameters is as follows:

1. If cli_parse_args is enabled, arguments passed in at the CLI.
2. Arguments passed to the Settings class initialiser.
3. Environment variables, e.g. my_prefix_special_function as described above.
4. Variables loaded from a dotenv (.env) file.
5. Variables loaded from the secrets directory.
6. The default field values for the Settings model.

As shown by priority (4), the library accepts a dotenv (.env) file for parameterization. The configuration file path is set using the `TASK_QUEUE_CONFIG_PATH` environment variable. If this feature is used when launching a Task Queue container, then the path must be mounted when the container is launched.

The configuration models for the Task Queue are found in [config.py](.task_queue/config/config.py), and include:

TaskQueueBaseSettings: No parameters, but gives shared logic for logging and loading in .env

TaskQueueSqlSettings: SQL Parameters
- SQL_QUEUE_NAME
- SQL_QUEUE_POSTGRES_DATABASE
- SQL_QUEUE_POSTGRES_HOSTNAME
- SQL_QUEUE_POSTGRES_PASSWORD
- SQL_QUEUE_POSTGRES_USER
- SQL_QUEUE_POSTGRES_PORT
- SQL_QUEUE_CONNECTION_STRING

TaskQueueS3Settings: S3 Parameters
- S3_QUEUE_BASE_PATH
- FSSPEC_S3_ENDPOINT_URL

TaskQueueApiSettings: Settings for launching the REST API
- QUEUE_IMPLEMENTATION

TaskQueueCliSettings: Settings for launching the CLI
- worker_interface
- queue_implementation
- event_store_implementation
- with_queue_events
- processing_limit
- periodic_seconds
- worker_interface_id
- endpoint
- namespace
- path_to_scripts
- connection_string
- queue_name
- s3_base_path
- add_to_queue_event_name
- move_queue_event_name

# Work Queue Api Client

There is a work queue api client designed to allow users to access the api endpoints easily in their python code. It wraps all of the api endpoints into callable methods.
To use the client simply construct it with the url to the api. For example if your api is hosted locally on port 8080.

```
client = ApiClient("localhost:8080")
```

# Work Queue Service

The `work_queue_service_cli.py` file will run a persistent service that periodically starts new jobs from a queue's `WAITING` stage with a queue worker. It's currently configured to try to keep no more than some amount of jobs in the `PROCESSING` stage, but it should be rather easy to change.

# Starting Work Queue Server and CLI

You will need docker installed on you machine to use the Task Queue.

## Downloading Docker Image

To download the docker image, go to the registry and select the desired image tag (https://github.com/AFMC-MAJCOM/task-queue/pkgs/container/task-queue%2Ftask-queue). The image tags mirror the version associated with the task-queue python package installed inside the image. For this example, the `latest` image will be used.


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

The container can then be spun up using the below command. `<volume_name>` in the command is whatever you would like to call the volume where the logs will be stored. The most likely path of where the logs are actually written to on your machine is `/var/lib/docker/volumes/<volume_name>/_data`, however, you can check by running ```docker volume inspect <volume_name>```. Typically on windows machines the path will be here `\\wsl$\docker-desktop\mnt\docker-desktop-disk\data\docker\volumes\<volume_name>\_data`.

```
docker run -ti --rm -p 8001:8001 --env-file ./env.list -v <volume_name>:/home/default/logs ghcr.io/afmc-majcom/task-queue/task-queue:latest server
```

(Optional) If you started your own local postgres server and are using a linux machine run this command instead.

```
docker run -ti --add-host=host.docker.internal:host-gateway --rm -p 8001:8001 --env-file ./env.list -v <volume_name>:/home/default/logs ghcr.io/afmc-majcom/task-queue/task-queue:latest server
```

### Running the CLI

Run the following command to output help from CLI

```
docker run --rm -v <volume_name>:/home/default/logs ghcr.io/afmc-majcom/task-queue/task-queue:latest controller --help
```

# Running tests

### S3 Connection
An external MinIO or S3 service is required for the tests to run and the following environment variables must be set.
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `FSSPEC_S3_ENDPOINT_URL` (If using a service outside Amazon S3 Services. e.g. MinIO)

### SQL Connection
An external postgreSQL service is required for the tests to run and the following environment variables must be set.
- `SQL_QUEUE_POSTGRES_HOSTNAME`
- `SQL_QUEUE_POSTGRES_PASSWORD`
- `SQL_QUEUE_POSTGRES_PORT`
- `SQL_QUEUE_POSTGRES_USER`

All pytests other than tests that use Argo Workflows can be run without any additional setup. Running argo tests with AWS or minikube is detailed below. A python virtual enviroment may be needed to use pip and pytest depending on your local setup.

By default, the argo tests are skipped. To run the argo tests, you can set an environment variable as follows:
`export RUN_ARGO_TESTS=True`

### Running Argo Workflow Tests in AWS

1. Get AWS CLI credentials
2. Make sure the test workflow template is deployed to argo
    - `kubectl apply -n pivot task-queue/resources/test_workflow_template.yaml`
3. Port-forward the argo workflows server pod
    - `shift+f` on K9s or `kubectl port-forward -n pivot <pod-name> <port>:<port>`
4. Start the local postgresql and minio server
    - `docker compose -f task-queue/resources/docker-compose.test.yaml up`
5. pip install the `task-queue` package
6. `python -m pytest task-queue/tests/test_argo_workflows_worker_interface.py`

### Running Argo Workflow Tests in minikube

1. Spin up the pods:
    - start minikube
    - `kubectl create namespace pivot`
    - `kubectl apply -n pivot -f task-queue/resources/quick-start-minimal.yaml`
2. Make sure the test workflow template is deployed to argo
    - `kubectl apply -n pivot -f task-queue/resources/test_workflow_template.yaml`
    - If you have the Argo CLI installed, you can use:
    - `argo -n pivot template create resources/test_workflow_template.yaml`
3. Port-forward the argo workflows server pod
    - `shift+f` on K9s or `kubectl port-forward -n pivot service/argo-server 2746:2746`
4. pip install the `task-queue` package
5. `python -m pytest task-queue/tests/test_argo_workflows_worker_interface.py`
