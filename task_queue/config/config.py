import os

from pydantic_settings import BaseSettings, SettingsConfigDict

class TaskQueueSettings(BaseSettings):
    # Load in the file from task_queue/config/config.env, unless a config file
    # path exists
    # Prefix all variables with "TASK_QUEUE"
    model_config = SettingsConfigDict(
        env_prefix='TASK_QUEUE_',
        env_file=os.environ.get(
                    "TASK_QUEUE_CONFIG_PATH",
                    os.path.join(os.path.dirname(__file__),'config.env'),
            ),
        env_file_encoding='utf-8'
    )

    SQL_HOST: str
    SQL_PASSWORD: str
    SQL_PORT: int
    SQL_USERNAME: str
    FSSPEC_S3_ENDPOINT_URL: str