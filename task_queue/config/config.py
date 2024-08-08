"""Contains functions and classes concerning configuration management.
"""

import logging
import os
from enum import Enum
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import (
    Field,
    field_validator,
)


logger = logging.getLogger(__name__)


def get_config_file_path() -> str:
    """Form the configuration file path.

    Prefer the environment variable to "TASK_QUEUE_CONFIG_PATH".

    Input file paths from the environment variables can be relative to the
    script, or absolute.

    Returns:
    -----------
    The .env configuration file path
    """
    return os.environ.get("TASK_QUEUE_CONFIG_PATH",None)


class QueueImplementations(str, Enum):
    """Enum options for the available Queue Implementations."""
    S3_JSON = 's3-json'
    SQL_JSON = 'sql-json'
    IN_MEMORY = 'in-memory'


class EventStoreChoices(str, Enum):
    """Enum options for the available event logging."""
    NO_EVENTS = 'none'
    SQL_JSON = 'sql-json'


class WorkerInterfaceChoices(str, Enum):
    """Enum options for the available worker interfaces choices."""
    ARGO_WORKFLOWS = 'argo-workflows'


class TaskQueueBaseSetting(BaseSettings):
    """Core settings logic to add config_path and logging."""
    model_config = SettingsConfigDict(
        env_file=get_config_file_path(),
        env_file_encoding='utf-8',
        extra='ignore'
    )

    def log_settings(self):
        """Log configuration parameters"""
        class_name = type(self).__name__.rsplit('.',maxsplit=1)[-1]
        logger.info("Loaded %s parameters:", class_name)
        for k, v in self.model_dump().items():
            logger.info("%s: %s", str(k), str(v))



class TaskQueueSqlSettings(TaskQueueBaseSetting):
    """SQL Settings for the Task Queue."""
    SQL_QUEUE_NAME: str
    SQL_QUEUE_POSTGRES_DATABASE: Optional[str] = None
    SQL_QUEUE_POSTGRES_HOSTNAME: Optional[str] = None
    SQL_QUEUE_POSTGRES_PASSWORD: Optional[str] = None
    SQL_QUEUE_POSTGRES_USER: Optional[str] = None
    SQL_QUEUE_POSTGRES_PORT: int = 5432
    SQL_QUEUE_CONNECTION_STRING: Optional[str] = None

    @field_validator('SQL_QUEUE_CONNECTION_STRING')
    @classmethod
    def validate_s3_path(cls, v: str, values) -> str:
        """Verify either the connection string is present"""
        if v is not None:
            return v
        for key, value in values.data.items():
            if value is None:
                raise ValueError(
                    f"SQL Queue parameter {key} must be supplied when "
                    "SQL_QUEUE_CONNECTION_STRING is None."
                )
        return v


class TaskQueueS3Settings(TaskQueueBaseSetting):
    """S3 Settings for the Task Queue."""
    S3_QUEUE_BASE_PATH: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    FSSPEC_S3_ENDPOINT_URL: Optional[str] = None

    @field_validator('S3_QUEUE_BASE_PATH')
    @classmethod
    def validate_s3_path(cls, v: str) -> str:
        """Validate the s3 path begins with s3://"""
        if not v.startswith("s3://"):
            raise ValueError("S3_QUEUE_BASE_PATH must start with s3://")
        return v


class TaskQueueApiSettings(TaskQueueBaseSetting):
    """Base settings for the task queue library REST API.

    Parameters preference is as follows:
    1) Environment Vars
    2) Configuration file
    3) Defaults given here
    """
    QUEUE_IMPLEMENTATION: QueueImplementations = QueueImplementations.SQL_JSON


class TaskQueueCliSettings(TaskQueueBaseSetting,
                           cli_parse_args=True,
                           cli_hide_none_type=True):
    """Base settings for the task queue library CLI.

    Parameters preference is as follows:
    1) Environment Vars
    2) Configuration file
    3) Defaults given here
    """
    worker_interface: WorkerInterfaceChoices = Field(
        description = "worker-interface: Service used to run jobs."
    )
    queue_implementation: QueueImplementations = Field(
        description="queue-implementation: Service used to store the queue."
    )
    event_store_implementation: EventStoreChoices = Field(
        default=EventStoreChoices.NO_EVENTS,
        description="event-store-implementation: "
                    "Service used to store logs of queue state changes."
    )
    with_queue_events : bool = Field(
        default=False,
        alias='with-queue-events',
        description="Flag to signify that logs should be stored on "
                    "queue state changes. The 'event_store_implementation' "
                    "argument should be set to 'sql-json' "
                    "when including this flag."
    )
    processing_limit : int = Field(
        default=10,
        alias='processing-limit',
        description="Number of jobs to be run concurrently."
    )
    periodic_seconds : int = Field(
        default=10,
        alias='periodic-seconds',
        description="Number of seconds to wait before checking if "
                    "additional jobs can be submitted."
    )
    worker_interface_id : Optional[str] = Field(
        default=None,
        alias='worker-interface-id',
        description="User defined ID for the worker interface "
                    "used to submit jobs. Can be any unique "
                    "string. Required when worker-interface is set "
                    f" to {WorkerInterfaceChoices.ARGO_WORKFLOWS.value}"
    )
    endpoint : Optional[str] = Field(
        default=None,
        description="Endpoint URL used to point to the ARGO "
                    "Workflows API. Required when worker-interface "
                    f"is set to {WorkerInterfaceChoices.ARGO_WORKFLOWS.value}"
    )
    namespace : Optional[str] = Field(
        default=None,
        description="Kubernetes namespace where ARGO Workflows is "
                    "running. Required when worker-interface is set "
                    f"to {WorkerInterfaceChoices.ARGO_WORKFLOWS.value}"
    )
    connection_string : Optional[str] = Field(
        default=None,
        alias='connection-string',
        description="Connection string associated with an external "
                    "SQL server. Required when queue-implementation "
                    f"is set to {QueueImplementations.SQL_JSON.value}"
    )
    queue_name : Optional[str] = Field(
        default=None,
        alias='queue-name',
        description="User defined queue name. Can be any unique "
                    "string. Required when queue-implementation "
                    f"is set to {QueueImplementations.SQL_JSON.value}"
    )
    s3_base_path : Optional[str] = Field(
        default=None,
        alias='s3-base-path',
        description="S3 path where the queue will be stored. "
                    "Required when queue-implementation is set to "
                    f"{QueueImplementations.S3_JSON.value}"
    )
    add_to_queue_event_name : Optional[str] = Field(
        default=None,
        alias='add-to-queue-event-name',
        description="User defined event name used in the logs "
                    "when queue items are added. Required when "
                    "event-store-implementation is not set to "
                    f"{EventStoreChoices.NO_EVENTS.value}"
    )
    move_queue_event_name : Optional[str] = Field(
        default=None,
        alias='move-queue-event-name',
        description="User defined event name used in the logs "
                    "when queue items are moved. Required when "
                    "event-store-implementation is not set to "
                    f"{EventStoreChoices.NO_EVENTS.value}"
    )


def get_task_queue_settings(setting_class, config_path=None):
    """Wrapper for returning the TaskQueueSettings object.

    This function enables dynamically setting the configuration path.

    Parameters:
    -----------
    setting_class: TaskQueueSettings (option)
        The setting class to instantiate.
    config_path: str (optional)
        The path (relative or absolute) to a .env configuration file.

    Returns:
    -----------
    A TaskQueueSettings object.
    """
    if config_path is None:
        config_path = get_config_file_path()

    return setting_class(_env_file=config_path)
