"""Blank dockstring for file
"""
from fastapi import FastAPI

from dataclasses import dataclass, asdict
import os

from .queue_base import QueueBase, QueueItemStage
from .s3_queue import JsonS3Queue
from .sql_queue import JsonSQLQueue

app = FastAPI()

@dataclass
class QueueSettings():
    """Docstring
    """
    def from_env(env_dict:dict):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        return QueueSettings()
    
    def make_queue(self) -> QueueBase:
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        pass


@dataclass
class S3QueueSettings(QueueSettings):
    """Docstring
    """
    s3_base_path : str

    def from_env(env_dict:dict):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        return S3QueueSettings(
            env_dict['S3_QUEUE_BASE_PATH']
        )

    def make_queue(self):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        return JsonS3Queue(self.s3_base_path)


@dataclass
class SqlQueueSettings(QueueSettings):
    """Docstring
    """
    connection_string : str
    queue_name : str

    def from_env(env_dict:dict):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        if "SQL_QUEUE_CONNECTION_STRING" in env_dict:
            conn_str = env_dict["SQL_QUEUE_CONNECTION_STRING"]
        else:
            user = env_dict['SQL_QUEUE_POSTGRES_USER']
            password = env_dict['SQL_QUEUE_POSTGRES_PASSWORD']
            host = env_dict['SQL_QUEUE_POSTGRES_HOSTNAME']
            database = env_dict['SQL_QUEUE_POSTGRES_DATABASE']

            conn_str = f"postgresql+psycopg2://{user}:{password}@{host}/{database}"

        return SqlQueueSettings(
            conn_str,
            env_dict['SQL_QUEUE_NAME'],
        )
    
    def make_queue(self):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        from sqlalchemy import create_engine
        return JsonSQLQueue(
            create_engine(self.connection_string),
            self.queue_name
        )


def queue_settings_from_env(env_dict) -> QueueSettings:
    """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
    impl = env_dict['QUEUE_IMPLEMENTATION']
    if impl == "s3-json":
        return S3QueueSettings.from_env(env_dict)
    elif impl == "sql-json":
        return SqlQueueSettings.from_env(env_dict)
    
queue_settings = queue_settings_from_env(os.environ)
queue = queue_settings.make_queue()

@app.get("/api/v1/queue/sizes")
async def get_queue_sizes():
    """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
    return {
        s.name : queue.size(s)
        for s in QueueItemStage
    }

@app.get("/api/v1/queue/status/{item_id}")
async def lookup_queue_item_status(item_id:str):
    """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
    return queue.lookup_status(item_id)

@app.get("/api/v1/queue/describe")
async def describe_queue():
    """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
    return {
        "implementation": queue.__class__.__name__,
        "arguments": asdict(queue_settings)
    }
