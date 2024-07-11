"""Wherein is contained the functions for implementing the SQL Queue.
"""
from typing import Optional
from functools import partial
import json

from sqlmodel import Field, Session, SQLModel, select, func, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Engine

from data_pipeline.queue_base import QueueBase, QueueItemStage


class SqlQueue(SQLModel, table=True):
    """Class to define the SQL queue table.
    """
    # No queue may have duplicate `index_key`s
    __table_args__ = (
        UniqueConstraint("queue_name", "index_key",
                         name="_queue_name_index_key_uc"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    queue_item_stage: Optional[int] = QueueItemStage.WAITING.value
    json_data: str
    index_key: str
    queue_name: str


def add_json_to_sql_queue(engine, queue_name, items):
    """Adds Items to the SQL Queue.

    Parameters:
    -----------
    engine: Engine
    queue_name: str
        Name of Queue.
    items: dict
        Dictionary of Items to add to Queue.

    Returns:
    -----------
    Returns 0 if it was successful or else raises exception.
    """
    success = 0

    if len(items) == 0:
        return success

    fail_items = []

    db_items = []
    for k, v in items.items():
        try:
            db_items.append(
                SqlQueue(
                    json_data=json.dumps(v),
                    index_key=str(k),
                    queue_name=queue_name
                ).model_dump(exclude_unset=True)
            )
        except BaseException as e:
            print(e)

    with Session(engine) as session:
        statement = insert(SqlQueue).values(db_items).on_conflict_do_nothing()
        session.exec(statement)

        session.commit()

    if len(db_items) != len(items):
        raise BaseException("Error writing at least one queue object to S3:",
                            fail_items)

    return success


def get_json_from_sql_queue(engine, queue_name, n_items=1):
    """Grabs Items from SQL Queue.

    Parameters:
    -----------
    engine: Engine
    queue_name: str
        Name of Queue.
    n_items: int (default=1)
        Number of Items to get from Queue.

    Returns:
    -----------
    Returns list of item keys and data from the Queue.
    """
    with Session(engine) as session:
        stmt = select(SqlQueue).where(
            (queue_name == SqlQueue.queue_name)
            & (SqlQueue.queue_item_stage == QueueItemStage.WAITING.value)
        ).limit(n_items)
        results = session.exec(stmt)

        outputs = []
        for queue_item in results:
            outputs.append((queue_item.index_key,
                            json.loads(queue_item.json_data)))
            update_stage(engine,
                         queue_name,
                         QueueItemStage.PROCESSING,
                         queue_item.index_key)

        return outputs


def update_stage(engine, queue_name, new_stage, item_key):
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
        statement = select(SqlQueue).where(
            (SqlQueue.index_key == item_key) & \
                (SqlQueue.queue_name == queue_name)
        )
        results = session.exec(statement)

        item = results.one()

        item.queue_item_stage = new_stage.value

        session.add(item)
        session.commit()


def queue_size(engine, queue_name, stage):
    """Gets the number of items in a certain stage.

    Parameters:
    -----------
    engine: Engine
    queue_name: str
        Name of Queue.
    stage: QueueItemStage.[STAGE]
        Desired Stage to return size of.
    Returns:
    -----------
    Returns the number of items in the Queue at that current stage.
    """
    with Session(engine) as session:
        statement = select(func.count(SqlQueue.id)).filter(
            SqlQueue.queue_name == queue_name,
            SqlQueue.queue_item_stage == stage.value
        )

        return session.exec(statement).first()


def lookup_status(engine, queue_name, item_id):
    """Looks up what the current stage of a certain item is.

    Parameters:
    -----------
    engine: Engine
    queue_name: str
        Name of Queue.
    item_id: str
        ID of item to return current stage.

    Returns:
    -----------
    Returns a QueueItemStage object for the Item.
    """
    with Session(engine) as session:
        statement = (
            select(SqlQueue.queue_item_stage)
            .where((queue_name == SqlQueue.queue_name) & \
                   (str(item_id) == SqlQueue.index_key))
        )

        item = session.exec(statement).first()

        if None == item:
            raise KeyError(item_id)

        return QueueItemStage(item)


def JsonSQLQueue(engine:Engine, queue_name):
    """Creates and returns the SQL Queue.
    """
    SQLModel.metadata.create_all(engine)

    return QueueBase(
        partial(add_json_to_sql_queue, engine, queue_name),
        partial(get_json_from_sql_queue, engine, queue_name),
        partial(update_stage, engine, queue_name, QueueItemStage.SUCCESS),
        partial(update_stage, engine, queue_name, QueueItemStage.FAIL),
        partial(queue_size, engine, queue_name),
        partial(lookup_status, engine, queue_name),
        {
            "implementation": "sql",
            "engine_url": str(engine.url)
        }
    )
