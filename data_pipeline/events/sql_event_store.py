"""Top file docstring
"""
from .event_store_interface import EventStoreInterface
from .event import Event

import json

from sqlmodel import Field, Session, SQLModel, select, func, UniqueConstraint
from sqlalchemy.dialects.postgresql import insert
from typing import List, Optional
from datetime import datetime
from sqlalchemy import Engine

class SqlEventStoreModel(SQLModel, table=True):
    """Docstring
    """
    __tablename__ = "sqleventstore"
    id : int | None = Field(default=None, primary_key=True)
    name : str
    version : str
    json_data : str # json blob
    event_metadata : str # json blob
    time : datetime

def to_event(self) -> Event:
    """Docstring

    details

    Parameters:
    -----------

    Returns:
    -----------

    """
    return Event(
        id = self.id,
        name = self.name,
        version = self.version,
        data = json.loads(self.json_data),
        metadata = json.loads(self.event_metadata),
        time = self.time
    )

def from_event(event:Event):
    """Docstring

    details

    Parameters:
    -----------

    Returns:
    -----------

    """
    # this `if/else` is necessary because if `id` is set to anything (even
    # None) then it will be set when dumped to a dict using `model_dump`, even
    # with `exlude_unset=True`.
    if event.id is None:
        return SqlEventStoreModel(
            name = event.name,
            version = event.version,
            json_data = json.dumps(event.data),
            event_metadata = json.dumps(event.event_metadata),
            time = event.time
        )
    else:
        return SqlEventStoreModel(
            id = event.id,
            name = event.name,
            version = event.version,
            json_data = json.dumps(event.data),
            event_metadata = json.dumps(event.event_metadata),
            time = event.time
        )


class SqlEventStore(EventStoreInterface):
    """Docstring
    """
    def __init__(self, engine:Engine):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        SQLModel.metadata.create_all(engine)
        self.engine = engine


    def _add_raw(self, events: List[Event]):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        if not events:
            # empty list causes issues on SQL insert
            return

        db_events = [
            from_event(evt).model_dump(exclude_unset=True)
            for evt in events
        ]

        for db_evt in db_events:
            print(db_evt)

        with Session(self.engine) as session:
            statement = insert(SqlEventStoreModel).values(db_events)

            session.exec(statement)
            session.commit()

    
    def get(self, event_name: str, time_since: datetime = None) -> List[Event]:
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        sql_query = event_name == SqlEventStoreModel.name

        if time_since is not None:
            sql_query = sql_query & (SqlEventStoreModel.time > time_since)

        with Session(self.engine) as session:
            statement = (
                select(SqlEventStoreModel)
                .where(sql_query)
            )

            items = session.exec(statement).all()

            return list(map(to_event, items))
