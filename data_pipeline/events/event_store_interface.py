"""Top file docstring
"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Union

from .event import Event


class EventStoreInterface(ABC):
    """Docstring
    """

    @abstractmethod
    def _add_raw(self, events : List[Event]):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        pass


    def add(self, event : Union[Event, List[Event]]):
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        if not isinstance(event, list):
            event = [event]

        self._add_raw(event)


    @abstractmethod
    def get(self, event_name : str, time_since : datetime = None) -> List[Event]:
        """Docstring

        details

        Parameters:
        -----------

        Returns:
        -----------

        """
        pass

