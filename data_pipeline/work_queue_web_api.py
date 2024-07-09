from fastapi import FastAPI

from dataclasses import dataclass, asdict
import os

from data_pipeline.queue_base import QueueBase, QueueItemStage
from data_pipeline.s3_queue import JsonS3Queue
from data_pipeline.sql_queue import JsonSQLQueue

app = FastAPI()

@dataclass
class QueueSettings():
    def from_env(env_dict:dict):
        return QueueSettings()

    def make_queue(self) -> QueueBase:
        pass


@dataclass
class S3QueueSettings(QueueSettings):
    s3_base_path : str

    def from_env(env_dict:dict):
        return S3QueueSettings(
            env_dict['S3_QUEUE_BASE_PATH']
        )

    def make_queue(self):
        return JsonS3Queue(self.s3_base_path)


@dataclass
class SqlQueueSettings(QueueSettings):
    connection_string : str
    queue_name : str

    def from_env(env_dict:dict):
        if "SQL_QUEUE_CONNECTION_STRING" in env_dict:
            conn_str = env_dict["SQL_QUEUE_CONNECTION_STRING"]
        else:
            user = env_dict['SQL_QUEUE_POSTGRES_USER']
            password = env_dict['SQL_QUEUE_POSTGRES_PASSWORD']
            host = env_dict['SQL_QUEUE_POSTGRES_HOSTNAME']
            database = env_dict['SQL_QUEUE_POSTGRES_DATABASE']

            conn_str = \
                f"postgresql+psycopg2://{user}:{password}@{host}/{database}"

        return SqlQueueSettings(
            conn_str,
            env_dict['SQL_QUEUE_NAME'],
        )

    def make_queue(self):
        from sqlalchemy import create_engine
        return JsonSQLQueue(
            create_engine(self.connection_string),
            self.queue_name
        )


def queue_settings_from_env(env_dict) -> QueueSettings:
    impl = env_dict['QUEUE_IMPLEMENTATION']
    if impl == "s3-json":
        return S3QueueSettings.from_env(env_dict)
    elif impl == "sql-json":
        return SqlQueueSettings.from_env(env_dict)

queue_settings = queue_settings_from_env(os.environ)
queue = queue_settings.make_queue()

@app.get("/api/v1/queue/sizes")
async def get_queue_sizes():
    return {
        s.name : queue.size(s)
        for s in QueueItemStage
    }

@app.get("/api/v1/queue/status/{item_id}")
async def lookup_queue_item_status(item_id:str):
    return queue.lookup_status(item_id)

@app.get("/api/v1/queue/describe")
async def describe_queue():
    return {
        "implementation": queue.__class__.__name__,
        "arguments": asdict(queue_settings)
    }
