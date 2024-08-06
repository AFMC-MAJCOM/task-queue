"""Contains functions and classes concerning configuration management.
"""

import logging
import os
from enum import StrEnum
from typing import Optional
from typing_extensions import Annotated

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import (
    BaseModel,
    ValidationError,
    ValidationInfo,
    BeforeValidator,
    field_validator,
)


logger = logging.getLogger(__name__)


def get_config_file_path() -> str:
    """Form the configuration file path.

    Prefer the environment variable to "TASK_QUEUE_CONFIG_PATH".
    If that does not exist, read the default .env in the local folder.

    Input file paths from the environment variables can be relative to the
    script, or absolute.

    Returns:
    -----------
    The .env configuration file path
    """
    if "TASK_QUEUE_CONFIG_PATH" in os.environ:
        return os.environ["TASK_QUEUE_CONFIG_PATH"]
    return os.path.join(os.path.dirname(__file__),'config.env')


class QueueImplementations(StrEnum):
    """Enum options for the available Queue Implementations."""
    S3_JSON = 's3-json'
    SQL_JSON = 'sql-json'
    IN_MEMORY = 'in-memory'

class EventStoreChoices(StrEnum):
    """Enum options for the available event logging."""
    NO_EVENTS = 'none'
    SQL_JSON = 'sql-json'

class ArgoInterfaceChoices(StrEnum):
    """Enum options for the available argo choices."""
    ARGO_WORKFLOWS = 'argo-workflows'



class TaskQueueBaseSetting(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=get_config_file_path(),
        env_file_encoding='utf-8',
        extra='ignore'
    )

    def log_settings(self):
        class_name = type(self).__name__.split('.')[-1]
        logger.info(f"Loaded {class_name} parameters:")
        for k, v in self.model_dump().items():
            logger.info(f"{k}: {v}")



class TaskQueueSqlSettings(TaskQueueBaseSetting):
    """SQL Settings for the Task Queue."""
    SQL_QUEUE_NAME: str
    SQL_QUEUE_POSTGRES_DATABASE: str = "postgres"
    SQL_QUEUE_POSTGRES_HOSTNAME: str = "postgres"
    SQL_QUEUE_POSTGRES_PASSWORD: str = "postgres"
    SQL_QUEUE_POSTGRES_USER: str = "postgres"
    SQL_QUEUE_POSTGRES_PORT: int = 5432
    SQL_QUEUE_CONNECTION_STRING: Optional[str] = None


class TaskQueueS3Settings(TaskQueueBaseSetting):
    """S3 Settings for the Task Queue."""
    S3_QUEUE_BASE_PATH: str
    FSSPEC_S3_ENDPOINT_URL: Optional[str] = None

    @field_validator('S3_QUEUE_BASE_PATH')
    @classmethod
    def name_must_contain_space(cls, v: str) -> str:
        if not v.startswith("s3://"):
            raise ValueError("S3_QUEUE_BASE_PATH must start with s3://")
        return v


class TaskQueueApiSettings(TaskQueueBaseSetting):
    """Base settings for the task queue library.

    Parameters preference is as follows:
    1) Environment Vars
    2) Configuration file
    3) Defaults given here

    The parameters include SQL connection information, queue selection, and
    S3 connection Information
    """
    QUEUE_IMPLEMENTATION: QueueImplementations = QueueImplementations.SQL_JSON


class TaskQueueTestSettings(TaskQueueApiSettings):
    """Extra settings for testing the task queue library."""
    # Testing configuration parameters
    TASK_QUEUE_ENV_TEST: bool = False
    run_argo_tests: bool = False
    UNIT_TEST_QUEUE_BASE: str = "s3://unit-tests/queue/queue_"


def get_task_queue_settings(path=None, test=False, setting_class=None):
    """Wrapper for returning the TaskQueueSettings object.

    This function enables dynamically setting the configuration path.

    Parameters:
    -----------
    path: str (optional)
        The path (relative or absolute) to a .env configuration file.
    test: bool (default=False)
        Flag to return the testing settings if desired.

    Returns:
    -----------
    A TaskQueueSettings object.
    """
    if test:
        setting_class = TaskQueueTestSettings
    elif setting_class is None:
        setting_class = TaskQueueApiSettings

    if path is None:
        path = get_config_file_path()

    return setting_class(_env_file=path)

"""
TODO:
Verify container still works
"""
