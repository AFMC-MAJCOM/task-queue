"""Created SQL engine for pytests.
"""
import sqlalchemy as sqla

from task_queue import config

class PytestSqlEngine():
    """Class to create a SQL engine used for testing."""

    def __init__(self):
        """Initializing PytestSqlEngine object from envrironemnt variables."""
        settings = config.TaskQueueSqlSettings()

        self.test_sql_engine = sqla.create_engine(sqla.engine.url.URL(
            drivername="postgresql",
            username=settings.SQL_QUEUE_POSTGRES_USER,
            password=settings.SQL_QUEUE_POSTGRES_PASSWORD,
            host=settings.SQL_QUEUE_POSTGRES_HOSTNAME,
            database=settings.SQL_QUEUE_POSTGRES_DATABASE,
            query={},
            port=settings.SQL_QUEUE_POSTGRES_PORT,

        ), echo=True)
