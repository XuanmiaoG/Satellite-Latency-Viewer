"""ssec_amqp.client

Manage multiple amqp connections at once.
"""

import logging
from contextlib import suppress
from enum import Enum
from typing import Any, Dict, List, Optional

from ssec_amqp._retry import LazyRetry
from ssec_amqp.amqp import AMQPConnectionError, ConnectionProtocol

# Default AmqpClient values
DEFAULT_RECONNECT_WINDOW = -1.0  # reconnects forever
DEFAULT_RECONNECT_INTERVAL = 15  # wait 15 seconds before another attempt

LOG = logging.getLogger("ssec_amqp")


class StrEnum(str, Enum):
    """StrEnum is a custom Enum that inherits from str.
    Use a custom StrEnum because built-in was introduced in python 3.11.

    Example usage::
        class Example(StrEnum):
            TEST = "TEST"


        assert Example.TEST == "TEST"


    Class heavily inspired by the pypi package strenum.
    https://github.com/irgeek/StrEnum/blob/master/strenum/__init__.py
    """

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return str(self.value)

    @staticmethod
    def _generate_next_value_(name: str, *_: Any, **__: Any) -> str:
        return name


class DeliveryStatus(StrEnum):
    """Enum for status of messages being delivered"""

    # Message was acknowledged by the server.
    DELIVERED = "DELIVERED"
    # Message was dropped due to reconnection.
    DROPPED = "DROPPED"
    # Message was rejected by the server.
    REJECTED = "REJECTED"


class ConnectionStatus(StrEnum):
    """Enum for status of exchange's connection"""

    # Exchange is connected to the server
    CONNECTED = "CONNECTED"

    # Exchange is reconnecting to the server
    RECONNECTING = "RECONNECTING"

    # Exchange is disconnected from the server
    DISCONNECTED = "DISCONNECTED"


class AmqpClient:
    """Client that manages multiple ConnectionProtocols at once."""

    def __init__(
        self,
        max_reconnect_time: Optional[float] = None,
        time_between_reconnects: Optional[float] = None,
        name: Optional[str] = None,
    ) -> None:
        """Initialize an AmqpClient.

        Args:
            reconnect_window (Optional[float], optional): How long an ConnectionProtocol
            has to reconnect before an error is raised. Negative for infinite time.
            Defaults to -1.
            time_between_reconnects (Optional[float], optional): Time to wait between
            reconnect attempts. It is not recommended to use a small value bc of
            negative performance side-effects.
            name (Optional[str], optional): A name to give this client, it isn't
            used within the class itself, but can help differentiate when
            logging.
        """
        self.reconnect_window = max_reconnect_time or DEFAULT_RECONNECT_WINDOW
        self.reconnect_interval = time_between_reconnects or DEFAULT_RECONNECT_INTERVAL
        self._name = name or str(id(self))

        self._connected_pool: List[ConnectionProtocol] = []
        self._reconnect_pool: Dict[ConnectionProtocol, LazyRetry[None]] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def connections(self) -> Dict[str, ConnectionStatus]:
        self.refresh_pools()
        d = {exch.identifier: ConnectionStatus.CONNECTED for exch in self._connected_pool}
        d.update({exch.identifier: ConnectionStatus.RECONNECTING for exch in self._reconnect_pool})
        return d

    def connect(self, exchange: ConnectionProtocol) -> None:
        """Connect this AmqpClient to an ConnectionProtocol

        Args:
            exchange (ConnectionProtocol): The ConnectionProtocol to connect to.

        Raises:
            ConnectionError: If it cannot connect to the exchange.
        """
        LOG.debug("%s - attempting to connect to %s", str(self), str(exchange))

        if exchange in self._connected_pool:
            LOG.debug("%s - already connected to %s, skipping...", str(self), str(exchange))
            return

        if exchange.connected:
            LOG.debug("%s - %s pre-connected, refreshing...", str(self), str(exchange))
            exchange.refresh()
            self._to_connected(exchange)
            return

        try:
            exchange.connect()
        except AMQPConnectionError:
            LOG.info(
                "%s - initial connection attempt to %s failed, reconnecting",
                str(self),
                str(exchange),
            )
            self._to_reconnect(exchange)
        else:
            LOG.info("%s - successfully connected to %s", str(self), str(exchange))
            self._to_connected(exchange)

        self.refresh_pools()  # Could raise a timeout error!

    def publish(
        self, message: Any, route_key: Optional[str] = None, exchange: Optional[str] = None
    ) -> Dict[str, DeliveryStatus]:
        """Publish an AMQP message to all exchanges connected to this client.

        Args:
            message (JSONable): A JSON-able message to publish
            route_key (Optional[str], optional): the route key to publish with. Defaults to None.
            exchange (Optional[str], optional): The exchange to publish to. If None,
            publish to the exchange the connection was initialized with. Default None.

        Returns:
            Dict[str, DeliveryStatus]: The status of the publish to all exchanges connected to this client.
        """
        pub_status = {}
        pub_failed = []
        self.refresh_pools()
        for connection in self._connected_pool:
            try:
                routable = connection.produce(message, route_key=route_key, exchange=exchange)
            except AMQPConnectionError:
                LOG.exception("%s - error publishing to %s!", str(self), str(connection))
                pub_failed.append(connection)
            else:
                LOG.debug(
                    "%s - published message to %s to %s",
                    str(self),
                    str(connection),
                    route_key or "default route",
                )
                pub_status[connection.identifier] = DeliveryStatus.DELIVERED if routable else DeliveryStatus.REJECTED

        # Set status as dropped for all reconnecting exchanges
        for exch in pub_failed:
            self._to_reconnect(exch)
        pub_status.update({conn.identifier: DeliveryStatus.DROPPED for conn in self._reconnect_pool})
        return pub_status

    def disconnect(self, exchange: Optional[ConnectionProtocol] = None) -> None:
        """Disconnect this AmqpClient from one or all exchanges.

        Args:
            exchange (Optional[ConnectionProtocol], optional): A specific exchange to disconnect from.
            If none, disconnect from all exchanges. Defaults to None.
        """
        if exchange is not None:
            exch_str = str(exchange)
            if exchange in self._reconnect_pool:
                LOG.debug(
                    "%s - removing %s from reconnect pool for disconnect",
                    str(self),
                    exch_str,
                )
                del self._reconnect_pool[exchange]
            elif exchange in self._connected_pool:
                LOG.debug(
                    "%s - removing %s from conncted pool for disconnect",
                    str(self),
                    exch_str,
                )
                self._connected_pool.remove(exchange)
            else:
                err_msg = f"Not connected to {exch_str}"
                raise ValueError(err_msg)
            LOG.info("%s - disconnecting from %s", str(self), exch_str)
            with suppress(AMQPConnectionError):
                exchange.close()
            return

        LOG.info("%s - disconnecting from everything", str(self))
        for exchange in self._connected_pool:
            with suppress(AMQPConnectionError):
                exchange.close()
        for exchange in self._reconnect_pool:
            with suppress(AMQPConnectionError):
                exchange.close()
        self._reconnect_pool.clear()
        self._connected_pool.clear()

    def _to_reconnect(self, exchange: ConnectionProtocol) -> None:
        """Move an exchange to reconnecting pool.

        Args:
            exchange (ConnectionProtocol): ConnectionProtocol to move.
        """
        if exchange in self._connected_pool:
            self._connected_pool.remove(exchange)
        LOG.debug("%s - moving %s to reconnect pool", str(self), str(exchange))
        # Make sure to close the connection before reconnecting
        with suppress(AMQPConnectionError):
            exchange.close()
        self._reconnect_pool[exchange] = LazyRetry(
            exchange.connect,
            AMQPConnectionError,
            max_retry_duration=self.reconnect_window,
            retry_interval=self.reconnect_interval,
        )

    def _to_connected(self, exchange: ConnectionProtocol) -> None:
        """Move an exchange to connected pool.

        Args:
            exchange (ConnectionProtocol): ConnectionProtocol to move.
        """
        if exchange in self._reconnect_pool:
            del self._reconnect_pool[exchange]
        LOG.debug("%s - moving %s to connected pool", str(self), str(exchange))
        self._connected_pool.append(exchange)

    def refresh_pools(self) -> None:
        """Refresh this client's pools. Checks if exchanges can reconnect."""
        LOG.debug("%s - refreshing pools", str(self))
        for exchange, reconnect_attempt in self._reconnect_pool.copy().items():
            if not reconnect_attempt.retry_ready:
                continue
            result = reconnect_attempt()
            if result is LazyRetry.NOT_YET:
                continue
            if result is LazyRetry.FAILED_ATTEMPT:
                LOG.info("%s - %s failed to reconnect", str(self), str(exchange))
            else:
                LOG.info("%s - %s has reconnected!", str(self), str(exchange))
                self._to_connected(exchange)
        for exchange in self._connected_pool:
            try:
                exchange.refresh()
            except AMQPConnectionError:
                LOG.warning("%s - %s has lost connection!", str(self), str(exchange))
                self._to_reconnect(exchange)

    def __repr__(self) -> str:
        return f"AmqpClient<{self._name}>"
