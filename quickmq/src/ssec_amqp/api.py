"""
ssec_amqp.api
~~~~~~~~~~~~~

This module implements the quickmq API.
"""

import atexit
from typing import Any, Dict, Optional

from ssec_amqp.amqp import AmqpConnection, ClusteredConnection
from ssec_amqp.client import (
    DEFAULT_RECONNECT_INTERVAL,
    DEFAULT_RECONNECT_WINDOW,
    AmqpClient,
    ConnectionStatus,
    DeliveryStatus,
)

__CLIENT = AmqpClient(name="API_CLIENT")

atexit.register(__CLIENT.disconnect)


def configure(reconnect_window: Optional[float] = None, reconnect_interval: Optional[float] = None) -> None:
    """Configure the current client.

    Args:
        reconnect_window (Optional[float], optional): How long to wait until a reconnecting exchange will throw an
        error. Defaults to None.
        reconnect_interval (Optional[float], optional): How long to wait between reconnect attempts. Not recomended
        to use a small value.
    """
    __CLIENT.reconnect_window = reconnect_window or DEFAULT_RECONNECT_WINDOW
    __CLIENT.reconnect_interval = reconnect_interval or DEFAULT_RECONNECT_INTERVAL


def connect(
    host: str,
    *hosts: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    exchange: Optional[str] = None,
    port: Optional[int] = None,
    vhost: Optional[str] = None,
    cluster: bool = False,
) -> None:
    """Connect to one or more AMQP servers. Connections will automatically be kept open until
    ``disconnect`` is called, or the calling program exits.

    Args:
        host (str): hostname of server to connect to.
        user (Optional[str], optional): User to connect with. Defaults to None.
        password (Optional[str], optional): Password to connect with. Defaults to None.
        exchange (Optional[str], optional): Exchange to connect to. Defaults to None.
        port (Optional[int], optional): Port to connect to. Defaults to None.
        vhost (Optional[str], optional): Vhost to connect to. Defaults to None.
        cluster (bool): Do the hosts form a cluster? Default False.
    """
    connections = [AmqpConnection(dest, user, password, exchange, vhost, port) for dest in (host, *hosts)]
    if cluster:
        __CLIENT.connect(ClusteredConnection(connections))
    else:
        for exch in connections:
            __CLIENT.connect(exch)


def status() -> Dict[str, ConnectionStatus]:
    """Get the status of all current connections.

    Returns:
        Dict[str, ConnectionStatus]: A dictionary containing all of the current connections
        and there status: either 'CONNECTED', 'RECONNECTING', OR 'DISCONNECTED'.
    """
    return __CLIENT.connections


def publish(message: Any, route_key: Optional[str] = None, exchange: Optional[str] = None) -> Dict[str, DeliveryStatus]:
    """Publish a message to all currently connected AMQP exchanges.

    Args:
        message (Any): The message to publish, must be json-able.
        route_key (Optional[str], optional): The key to publish the message with. Defaults to None.
        exchange (Optional[str], optional): The exchange to publish the message to.
        If None, publish to the exchange that was initially specified in ``connect``.
        Default None.

    Raises:
        RuntimeError: If not connected to any AMQP exchanges currently.

    Returns:
        Dict[str, DeliveryStatus]: The delivery status to all of the currently connected AMQP exchanges.
    """
    return __CLIENT.publish(message, route_key=route_key, exchange=exchange)


def disconnect() -> None:
    """Disconnect from all AMQP exchanges."""
    __CLIENT.disconnect()
