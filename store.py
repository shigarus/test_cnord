"""
This module represents a store of the system.
"""

import collections
import datetime
import itertools
from typing import Dict, Sequence

Source = collections.namedtuple('Source', 'id_ serial_num state last_received')
Listener = collections.namedtuple('Listener', 'id_ sources_notified')


class _SourcesStore:
    """
    Represents store for state of sources.
    >>> import datetime
    >>> store = _SourcesStore()
    >>> store.get_all()
    ()
    >>> store.update_state('asdfqwer', 23, 4, datetime.datetime(2000, 1, 1))
    >>> store.get_all()
    (Source(id_='asdfqwer', serial_num=23, state=4, last_received=datetime.datetime(2000, 1, 1, 0, 0)),)
    >>> store.get_state('asdfqwer')
    Source(id_='asdfqwer', serial_num=23, state=4, last_received=datetime.datetime(2000, 1, 1, 0, 0))
    >>> store.update_state('asdfqwer', 30, 4, datetime.datetime(2000, 1, 5))
    >>> store.get_state('asdfqwer')
    Source(id_='asdfqwer', serial_num=30, state=4, last_received=datetime.datetime(2000, 1, 5, 0, 0))
    """

    def __init__(self):
        self._sources: Dict[str, Source] = {}

    def update_state(self,
                     source_id: str,
                     serial_num: int,
                     state: int,
                     last_received: datetime.datetime
                     ):
        """ Creates or update state of source """
        self._sources[source_id] = Source(
            id_=source_id,
            serial_num=serial_num,
            state=state,
            last_received=last_received,
        )

    def get_state(self, source_id: str) -> Source:
        return self._sources.get(source_id)

    def get_all(self) -> Sequence[Source]:
        return tuple(self._sources.values())


class _ListenersStore:
    """
    Represents store for listeners.
    Helps track which listeners were notified about which sources.
    >>> listener = _ListenersStore()
    >>> listener.get_all()
    ()
    >>> listener.add_listener()
    0
    >>> listener.is_notified(0, 'asdfqwer')
    False
    >>> listener.set_notified(0, 'asdfqwer')
    >>> listener.is_notified(0, 'asdfqwer')
    True
    >>> listener.get_all()
    (Listener(id_=0, sources_notified={'asdfqwer'}),)
    >>> listener.remove_listener(0)
    >>> listener.get_all()
    ()
    """

    def __init__(self):
        self._listeners: Dict[int: Listener] = {}
        self._counter = itertools.count()

    def add_listener(self) -> int:
        """
        Creates new listener obj.
        :return: id_ of listener
        """
        id_ = next(self._counter)
        self._listeners[id_] = Listener(id_=id_, sources_notified=set())
        return id_

    def remove_listener(self, id_):
        self._listeners.pop(id_)

    def set_notified(self, id_, source_id):
        self._listeners[id_].sources_notified.add(source_id)

    def is_notified(self, id_, source_id):
        return source_id in self._listeners[id_].sources_notified

    def get_all(self) -> Sequence[Listener]:
        return tuple(self._listeners.values())


ListenersStore = _ListenersStore()
SourcesStore = _SourcesStore()
