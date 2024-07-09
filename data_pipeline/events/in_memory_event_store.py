from datetime import datetime
from typing import List, Dict
from .event import Event
from .event_store_interface import EventStoreInterface

class InMemoryEventStore(EventStoreInterface):
    def __init__(self):
        self.events : Dict[str, List[Event]] = {}

    def _add_raw(self, events: List[Event]):
        for event in events:
            if not event.name in self.events:
                self.events[event.name] = []

            event._id = len(self.events[event.name])
            self.events[event.name].append(event)

    def get(self, event_name:str, time_since: datetime = None) -> List[Event]:
        return [
            e for e in self.events[event_name]
            if time_since is None or e.time > time_since
        ] if event_name in self.events else []
