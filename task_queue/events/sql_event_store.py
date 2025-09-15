"""Wherein is contained the implementation of the SQL Event Store.
"""
from datetime import datetime
import json

from sqlmodel import Field, Session, SQLModel, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import tuple_

from .event_store_interface import EventStoreInterface
from .event import Event


class SqlEventStoreModel(SQLModel, table=True):
    """Initializes the SQLEventStoreModel.
    """
    __tablename__ = "sqleventstore"
    id : int | None = Field(default=None, primary_key=True)
    name : str
    version : str
    json_data : str # JSON blob
    event_metadata : str # JSON blob
    time : datetime

def to_event(self):
    """Creates and returns an Event.
    """
    return Event(
        id = self.id,
        name = self.name,
        version = self.version,
        data = json.loads(self.json_data),
        metadata = json.loads(self.event_metadata),
        time = self.time
    )

def from_event(event):
    """Takes an event and creates a SQLEventStoreModel and returns it.
    """
    # This `if/else` is necessary because if `id` is set to anything (even
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
    return SqlEventStoreModel(
        id = event.id,
        name = event.name,
        version = event.version,
        json_data = json.dumps(event.data),
        event_metadata = json.dumps(event.event_metadata),
        time = event.time
    )


class SqlEventStore(EventStoreInterface):
    """Creates the SQL Event Store.
    """
    def __init__(self, engine):
        """Initializes the SQL Event Store.
        """
        SQLModel.metadata.create_all(engine)
        self.engine = engine

    def _remove_duplicates(self, db_events: list):
        """Queries sql event store for all events
        returns list of events not already in database
        """
        statement = (
            select(SqlEventStoreModel.name,
                   SqlEventStoreModel.json_data)
            .where(tuple_(SqlEventStoreModel.name,
                          SqlEventStoreModel.json_data)
                   .in_([(event['name'], event['json_data'])
                         for event in db_events]))
            )
        with Session(self.engine) as session:
            existing_records = session.exec(statement).all()

        existing_records_set = set(existing_records)
        missing_items = []
        for event in db_events:
            if (event['name'], event['json_data']) not in existing_records_set:
                missing_items.append(event)

        return missing_items

    def _add_raw(self, events):
        """Add events to Event Store.

        Parameters:
        -----------
        events: List[Event]
            List of Events
        """
        if not events:
            # empty list causes issues on SQL insert
            return

        db_events = [
            from_event(evt).model_dump(exclude_unset=True)
            for evt in events
        ]

        db_events = self._remove_duplicates(db_events)

        if not db_events:
            return

        with Session(self.engine) as session:
            statement = insert(SqlEventStoreModel).values(db_events)

            session.exec(statement)
            session.commit()

    def get(self, event_name, time_since=None):
        """Returns list of events that have happened since a specific time.

        Parameters:
        -----------
        event_name: str
            Name of Event Store
        time_since: datetime (default=None)
            Get every event that was logged after the provided time_since.

        Returns:
        -----------
        Returns a List of Events.
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
