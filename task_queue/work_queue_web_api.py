"""Wherein is contained the functions and classes concering the Work Queue Web
API.
"""
from dataclasses import dataclass, asdict
import os

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine

from task_queue.queue_base import QueueItemStage
from task_queue.s3_queue import json_s3_queue
from task_queue.sql_queue import json_sql_queue
from task_queue.in_memory_queue import in_memory_queue

app = FastAPI()

@dataclass
class QueueSettings():
    """Class concerning the Queue Settings.
    """
    # Disabled because this method is inherited
    # and uses env_dict as a param later
    # pylint: disable=unused-argument
    @staticmethod
    def from_env(env_dict):
        """Returns instance of QueueSettings

        Parameters:
        -----------
        env_dict: dict
            Dictionary of environment variables.
        """
        return QueueSettings()

    def make_queue(self):
        """Returns QueueBase object.
        """
        raise NotImplementedError("make_queue not yet implemented.")


@dataclass
class S3QueueSettings(QueueSettings):
    """Class concerning the s3 Queue settings.
    """
    s3_base_path : str

    @staticmethod
    def from_env(env_dict):
        """Returns an S3QueueSettings object given an s3 Queue Base path.

        Parameters:
        -----------
        env_dict: dict
            Dictionary of environment variables.
        """
        return S3QueueSettings(
            env_dict['S3_QUEUE_BASE_PATH']
        )

    def make_queue(self):
        """Creates and returns a JsonS3Queue.
        """
        return json_s3_queue(self.s3_base_path)


@dataclass
class SqlQueueSettings(QueueSettings):
    """Class concerning the SQL queue settings.
    """
    connection_string : str
    queue_name : str

    @staticmethod
    def from_env(env_dict):
        """Creates and returns an instance of SqlQueueSettings based on the
        given env_dict.

        Parameters:
        -----------
        env_dict: dict
            Dictionary of environment variables.

        Returns:
        -----------
        Returns an instance of SQLQueueSettings.
        """
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
        """Creates and returns a JSONSQLQueue.
        """
        return json_sql_queue(
            create_engine(self.connection_string),
            self.queue_name
        )

@dataclass
class InMemoryQueueSettings(QueueSettings):
    """Class concerning the In Memory Queue settings.

    The only implementation of this class so far is for testing.
    """
    @staticmethod
    def from_env(env_dict):
        """Returns instance of QueueSettings

        Parameters:
        -----------
        env_dict: dict
            Dictionary of environment variables.
        """
        return InMemoryQueueSettings()

    def make_queue(self):
        """Returns QueueBase object.
        """
        return in_memory_queue()

def queue_settings_from_env(env_dict):
    """Creates an instance of QueueSettings from an environment dictionary.

    Parameters:
    -----------
    env_dict: dict
        Dictionary of environment variables.

    Returns:
    -----------
    Returns QueueSettings.
    """
    impl = env_dict['QUEUE_IMPLEMENTATION']
    if impl == "s3-json":
        return S3QueueSettings.from_env(env_dict)
    if impl == "sql-json":
        return SqlQueueSettings.from_env(env_dict)
    if impl == "in-memory":
        return InMemoryQueueSettings.from_env(env_dict)
    return None

queue_settings = queue_settings_from_env(os.environ)
queue = queue_settings.make_queue()

@app.get("/api/v1/queue/sizes")
async def get_queue_sizes() -> dict:
    """API endpoint to get the number of jobs in each stage.

    Returns:
    ----------
    Returns the number of jobs in each stage of the queue.
    """
    return {
        s.name : queue.size(s)
        for s in QueueItemStage
    }

@app.get("/api/v1/queue/status/{item_id}")
async def lookup_queue_item_status(item_id:str) -> QueueItemStage:
    """API endpoint to look up the status of a specific item in queue.

    Parameters:
    -----------
    item_id: str
        ID of Item.

    Returns:
    -----------
    Returns the status of Item passed in.
    """
    try:
        return queue.lookup_status(item_id)
    except KeyError as exc:
        raise HTTPException(status_code=400,
                            detail=f"{item_id} not in Queue") from exc

@app.get("/api/v1/queue/describe")
async def describe_queue() -> dict:
    """API endpoint to descibe the Queue.

    Returns:
    ----------
    Returns a dictionary description of the Queue.
    """
    return {
        "implementation": queue.__class__.__name__,
        "arguments": asdict(queue_settings)
    }
