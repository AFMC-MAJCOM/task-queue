import os
from enum import StrEnum
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class QueueImplementations(StrEnum):
    S3Json = 's3-json'
    SQLJson = 'sql-json'


def get_config_file_path() -> str:
    if "TASK_QUEUE_CONFIG_PATH" in os.environ:
        return os.environ["TASK_QUEUE_CONFIG_PATH"]
    return os.path.join(os.path.dirname(__file__),'config.env')


class TaskQueueSettings(BaseSettings):
    # Load in the file from task_queue/config/config.env, unless a config file
    # path exists
    # Prefix all variables with "TASK_QUEUE"
    model_config = SettingsConfigDict(
        env_prefix='TASK_QUEUE_',
        env_file=get_config_file_path(),
        env_file_encoding='utf-8'
    )

    # Required base parameters
    SQL_HOST: str = "postgres"
    SQL_PASSWORD: str = "postgres"
    SQL_PORT: int = 5432
    SQL_USERNAME: str = "postgres"
    FSSPEC_S3_ENDPOINT_URL: str

    # Testing configuration parameters
    run_argo_tests: bool = False
    UNIT_TEST_QUEUE_BASE: str = "s3://unit-tests/queue/queue_"

    # API parameters
    QUEUE_IMPLEMENTATION: QueueImplementations = QueueImplementations.S3Json
    S3_QUEUE_BASE_PATH: Optional[str] = None
    SQL_QUEUE_NAME: Optional[str] = None
    SQL_QUEUE_POSTGRES_DATABASE: Optional[str] = None
    SQL_QUEUE_POSTGRES_HOSTNAME: Optional[str] = None
    SQL_QUEUE_POSTGRES_PASSWORD: Optional[str] = None
    SQL_QUEUE_POSTGRES_USER: Optional[str] = None
    SQL_QUEUE_CONNECTION_STRING: Optional[str] = None


def get_task_queue_settings(path=None):
    if path is None:
        path = get_config_file_path()
    return TaskQueueSettings(_env_file=path)
