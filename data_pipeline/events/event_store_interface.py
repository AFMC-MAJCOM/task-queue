"""Wherein is contained the functionality for the EventStoreInterface.
"""
from abc import ABC, abstractmethod


class EventStoreInterface(ABC):
    """The Abstract Class to create an Event Store Interface.
    """

    @abstractmethod
    def _add_raw(self, events):
        """Add events to Event Store.

        Parameters:
        -----------
        events: List[Event]
            List of Events
        """
        pass


    def add(self, event):
        """Convenience method to add one or many events to the event store.

        Parameters:
        -----------
        event: Union[Event, List[Event]]
        """
        if not isinstance(event, list):
            event = [event]

        self._add_raw(event)


    @abstractmethod
    def get(self, event_name, time_since=None):
        """Returns list of events that have happened since a specific time.

        Parameters:
        -----------
        event_name: str
            Name of Event Store
        time_since: datetime (default=None)
            Desired time since.

        Returns:
        -----------
        Retuns a List of Events.
        """
        pass
