"""Wherein is contained the implementation of the In Memory Event Store.
"""
from datetime import datetime
from typing import List, Dict
from .event import Event
from .event_store_interface import EventStoreInterface

class InMemoryEventStore(EventStoreInterface):
    """Class for the InMemoryEventStore.
    """
    def __init__(self):
        """Initializes the InMemoryEventStore.
        """
        self.events : Dict[str, List[Event]] = {}

    def _add_raw(self, events):
        """Add events to Event Store.

        Parameters:
        -----------
        events: List[Event]
            List of Events
        """
        for event in events:
            if not event.name in self.events:
                self.events[event.name] = []

            event._id = len(self.events[event.name])
            self.events[event.name].append(event)

    def get(self, event_name, time_since=None):
        """Returns list of events that have happened since a specific time.

        Parameters:
        -----------
        event_name: str
            Name of Event Store
        time_since: datatime (default=None)
            Desired time since.

        Returns:
        -----------
        Retuns a List of Events.
        """
        return [
            e for e in self.events[event_name]
            if time_since is None or e.time > time_since
        ] if event_name in self.events else []
