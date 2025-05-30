"""Wherein is contained the functions for implementing the SQL Queue.
"""
from typing import Optional, Any
import json

from sqlmodel import Field, Session, SQLModel, select, func, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert, JSONB
from sqlalchemy import Engine, Column

from task_queue import logger
from .queue_base import QueueBase, QueueItemStage

def new_sql_queue_table(tablename:str, constraint_name:str):
    """Creates a SQL table for the SQL Queue based on the table name provided.

    Parameters:
    -----------
    tablename: str (default "sqlqueue")
        Name for the table used for th SQL Queue

    Returns:
    -----------
    Returns an instance of the self.sql_queue object for the specific table
    """
    class SqlQueueTable(SQLModel, table=True):
        """Class to define the schema for a row in the SQL table.
        """
        __tablename__ = tablename
        # No queue may have duplicate `index_key`s
        __table_args__ = (
            UniqueConstraint("queue_name", "index_key",
                             name=constraint_name),
            {'extend_existing':True},
        )

        id: Optional[int] = Field(default=None, primary_key=True)
        queue_item_stage: Optional[int] = QueueItemStage.WAITING.value
        json_data: dict[Any, Any] = Field(sa_column=Column(JSONB))
        index_key: str
        queue_name: str
    return SqlQueueTable

class SQLQueue(QueueBase):
    """Creates the SQL Queue.
    """
    def __init__(self,
                 engine,
                 queue_name,
                 tablename="sqlqueue",
                 constraint_name="_queue_name_index_key_uc"):
        """Initializes the QueueBase class.
        """
        self.sql_queue = new_sql_queue_table(tablename, constraint_name)
        SQLModel.metadata.create_all(engine)
        self.queue_name = queue_name
        self.engine = engine

    # Disabled pylint because BaseException is used to record
    # and keep the program running correctly until the raise
    # BaseException is used to report on the error
    # pylint: disable=broad-exception-caught
    # pylint: disable=broad-exception-raised
    def put(self, items):
        """Adds a new Item to the Queue in the WAITING stage.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key=index_key, and value is the json expected when the
            job is submitted for processing. The item ID must be a string and
            the item body must be serializable.

        Returns:
        -----------
        Returns the number of items successfully added to the sql queue.
        """
        items = self._put(items)

        if len(items) == 0:
            # Nothing to add.
            return 0

        fail_items = []

        db_items = []
        for k, v in items.items():
            try:
                db_items.append(
                    self.sql_queue(
                        json_data=json.dumps(v),
                        index_key=str(k),
                        queue_name=self.queue_name
                    ).model_dump(exclude_unset=True)
                )
            except Exception as e:
                logger.warning(e)


        with Session(self.engine) as session:
            statement = (insert(self.sql_queue).values(db_items) \
                         .on_conflict_do_nothing())
            session.exec(statement)

            session.commit()

        if len(db_items) != len(items):
            logger.error("Error writing at least one queue object to SQL: %s",\
                        fail_items)
            raise ValueError(
                "Error writing at least one queue object to SQL:",
                fail_items)

        return len(db_items)

    def get(self, n_items=1):
        """Gets the next n items from the queue, moving them to PROCESSING.

        Parameters:
        -----------
        n_items: int
            Number of items to retrieve from queue.

        Returns:
        ------------
        Returns a list of n_items from the queue, as
        List[(queue_item_id, queue_item_body)]
        """
        with Session(self.engine) as session:
            stmt = select(self.sql_queue).where(
                (self.queue_name == self.sql_queue.queue_name) &
                (self.sql_queue.queue_item_stage==QueueItemStage.WAITING.value)
            ).limit(n_items)
            results = session.exec(stmt)

            outputs = []
            for queue_item in results:
                outputs.append((queue_item.index_key,
                                json.loads(queue_item.json_data)))
                update_stage(self.engine,
                             self.queue_name,
                             QueueItemStage.PROCESSING,
                             queue_item.index_key,
                             self.sql_queue)

            return outputs

    def peek(self, n_items=1):
        with Session(self.engine) as session:
            stmt = select(self.sql_queue).where(
                (self.queue_name == self.sql_queue.queue_name) &
                (self.sql_queue.queue_item_stage==QueueItemStage.WAITING.value)
            ).limit(n_items)
            results = session.exec(stmt)

            outputs = []
            for queue_item in results:
                outputs.append((queue_item.index_key,
                                json.loads(queue_item.json_data)))

            return outputs


    def success(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to SUCCESS.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        update_stage(
            self.engine,
            self.queue_name,
            QueueItemStage.SUCCESS,
            queue_item_id,
            self.sql_queue
        )
        logger.info("Job %s successfully completed", queue_item_id)

    def fail(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to FAIL.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        update_stage(
            self.engine,
            self.queue_name,
            QueueItemStage.FAIL,
            queue_item_id,
            self.sql_queue
        )
        logger.info("Job %s failed", queue_item_id)

    # Pylint cannot correctly tell that func has a count method
    # This raises an error that can be ignored
    # because func.count is a method that is callable
    # pylint: disable=not-callable
    def size(self, queue_item_stage):
        """Determines how many Items are in some stage of the Queue.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage object
            The specific stage of the Queue (PROCESSING, FAIL, etc.).

        Returns:
        ------------
        Returns the number of Items in that stage of the Queue as an integer.
        """
        with Session(self.engine) as session:
            statement = select(func.count(self.sql_queue.id)).filter(
                self.sql_queue.queue_name == self.queue_name,
                self.sql_queue.queue_item_stage == queue_item_stage.value
            )

            return session.exec(statement).first()

    def lookup_status(self, queue_item_id):
        """Lookup which stage in the Queue Item is currently in.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item

        Returns:
        ------------
        Returns the current stage of the Item as a QueueItemStage object, will
        raise an error if Item is not in Queue.
        """
        with Session(self.engine) as session:
            statement = (
                select(self.sql_queue.queue_item_stage)
                .where((self.queue_name == self.sql_queue.queue_name) & \
                       (str(queue_item_id) == self.sql_queue.index_key))
            )

            item = session.exec(statement).first()

            if item is None:
                logger.error("Item does not exist %s", queue_item_id)
                raise KeyError(queue_item_id)

            return QueueItemStage(item)

    def lookup_state(self, queue_item_stage):
        """Lookup which item ids are in the current Queue stage.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage
            stage of Queue Item

        Returns:
        ------------
        Returns a list of all item ids in the current queue stage.
        """
        with Session(self.engine) as session:
            statement = (
                select(self.sql_queue.index_key)
                .where(
                    (self.queue_name == self.sql_queue.queue_name) &
                    (queue_item_stage.value == self.sql_queue.queue_item_stage)
                )
            )

            result = session.exec(statement).all()
            return result

    def lookup_item(self, queue_item_id):
        """Lookup an Item currently in the Queue.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item

        Returns:
        ------------
        Returns a dictionary with the Queue Item ID, the status of that Item,
        and the body, or it will raise an error if Item is not in Queue.
        """
        # Get item stage
        item_stage = self.lookup_status(queue_item_id)
        # Get item body
        item_body = []
        with Session(self.engine) as session:
            stmt = select(self.sql_queue).where(
                (self.queue_name == self.sql_queue.queue_name)
                & (self.sql_queue.index_key == queue_item_id))
            results = session.exec(stmt)

            for queue_item in results:
                item_body = json.loads(queue_item.json_data)

        return {
            'item_id':queue_item_id,
            'status':item_stage,
            'item_body':item_body
        }

    def description(self):
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """
        desc = {
            "implementation": "sql",
            "engine_url": str(self.engine.url)
        }
        return desc

    def requeue(self, item_ids):
        """Move input queue items from FAILED to WAITING.

        Parameters:
        -----------
        item_ids: [str]
            ID of Queue Item
        """
        for item in self._requeue(item_ids):
            update_stage(self.engine,
                         self.queue_name,
                         QueueItemStage.WAITING,
                         item,
                         self.sql_queue)


def update_stage(engine, queue_name, new_stage, item_key, sql_queue):
    """Updates the stage of an Item.

    Parameters:
    -----------
    engine: Engine
    queue_name: str
        Name of Queue.
    new_stage: QueueItemStage.[STAGE]
        New stage for the Item to get moved to.
    item_key: str
        Key of the Item to be staged.
    """
    with Session(engine) as session:
        statement = select(sql_queue).where(
            (sql_queue.index_key == item_key) & \
                (sql_queue.queue_name == queue_name)
        )
        results = session.exec(statement)

        item = results.one()

        item.queue_item_stage = new_stage.value

        session.add(item)
        session.commit()

def json_sql_queue(
    engine:Engine,
    queue_name,
    table_name="sqlqueue",
    constraint_name="_queue_name_index_key_uc"
):
    """Creates and returns the SQL Queue.
    """
    return SQLQueue(
        engine,
        queue_name,
        tablename=table_name,
        constraint_name=constraint_name
    )
