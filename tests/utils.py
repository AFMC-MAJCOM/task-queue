"""Created SQL engine for pytests.
"""
import sqlalchemy as sqla

from task_queue import config

class PytestSqlEngine():
    """Class to create a SQL engine used for testing."""

    def __init__():
        """Initializing PytestSqlEngine object from envrironemnt variables."""
        settings = config.TaskQueueSqlSettings()

        self.drivername = "postgresql"
        self.username = settings.SQL_QUEUE_POSTGRES_USER
        self.password = settings.SQL_QUEUE_POSTGRES_PASSWORD
        self.host = settings.SQL_QUEUE_POSTGRES_HOSTNAME
        self.database = settings.SQL_QUEUE_POSTGRES_DATABASE
        self.query = {}
        self.port = settings.SQL_QUEUE_POSTGRES_PORT

    def create_test_engine():
        """Create and return SQL engine to use for testing."""
        test_sql_engine = sqla.create_engine(sqla.engine.url.URL(
                    self.drivername,
                    self.username,
                    self.password,
                    self.host,
                    self.database,
                    self.query,
                    self.port,
                ), echo=True)
        return test_sql_engine
