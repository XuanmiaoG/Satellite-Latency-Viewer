"""unit.conftest

Configuration and fixtures for unit tests.
"""

from typing import Callable, Optional

import pytest
from ssec_amqp.amqp import AMQPConnectionError, ConnectionProtocol, StateError

# Give the FakeConnections a unique number
NUM_TEST_CONNS = 0


class FakeConnection(ConnectionProtocol):
    """A connection protocol that can act as if it's connected or disconnected."""

    def __init__(self, num: int, can_connect: bool = True, can_publish: bool = True):
        self._num = num
        self._connected: Optional[bool] = None
        self._can_connect = can_connect
        self._can_publish = can_publish

    # Methods for manipulating the test connection's state

    @property
    def can_connect(self) -> bool:
        return self._can_connect

    @can_connect.setter
    def can_connect(self, can: bool) -> None:
        self._can_connect = can

    @property
    def can_publish(self) -> bool:
        return self._can_publish

    @can_publish.setter
    def can_publish(self, can: bool) -> None:
        self._can_publish = can

    # Methods for ConnectionProtocol

    @property
    def identifier(self) -> str:
        return str(self)

    @property
    def connected(self) -> bool:
        if not self._can_connect or self._connected is None:
            return False
        return self._connected

    def connect(self) -> None:
        if not self._can_connect:
            self._connected = False
            raise AMQPConnectionError
        self._connected = True

    def produce(self, content_dict, route_key: Optional[str] = None, exchange: Optional[str] = None) -> bool:  # noqa: ARG002
        self.refresh()
        if not self._can_publish:
            return False
        return True

    def close(self) -> None:
        self._connected = False

    def refresh(self) -> None:
        if self._connected is None:
            raise StateError("refresh")
        if not self._can_connect:
            self._connected = False
            raise AMQPConnectionError

    def __repr__(self) -> str:
        return f"FakeConnection({self._num})"

    def __eq__(self, _oth: object) -> bool:
        if not isinstance(_oth, self.__class__):
            return False
        return _oth.identifier == self.identifier


@pytest.fixture
def fake_conn_factory() -> Callable[[bool, bool], FakeConnection]:
    """Construct a ``FakeConnection`` that can be used for unit testing."""
    global NUM_TEST_CONNS

    def factory(can_connect: bool = True, can_publish: bool = True) -> FakeConnection:
        return FakeConnection(NUM_TEST_CONNS, can_connect=can_connect, can_publish=can_publish)

    NUM_TEST_CONNS += 1
    return factory
