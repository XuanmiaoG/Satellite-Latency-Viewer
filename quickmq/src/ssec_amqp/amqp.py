"""ssec_amqp.amqp

Classes and functions related to the AMQP connection to RabbitMQ brokers.
"""

import functools
import json
import logging
import random
import socket
import sys
import warnings
from typing import Any, Callable, Dict, Generic, Iterable, Optional, Type, TypeVar
from urllib.parse import unquote as urlunquote
from urllib.parse import urlparse

if sys.version_info >= (3, 10):
    from typing import ParamSpec, Protocol, TypedDict
else:
    from typing_extensions import ParamSpec, Protocol, TypedDict

import amqp as amqplib
from amqp.exceptions import ChannelError, MessageNacked

from ssec_amqp.__about__ import __version__

LOG = logging.getLogger("ssec_amqp")

# How we identify AmqpConnections. Inspired by the AMQP URI format, and adds the exchange to the end
AMQP_EXCHANGE_ID_FORMAT = "amqp://{user:s}@{host:s}:{port:d}{vhost:s}/{exchange:s}"

# Tell RabbitMQ what version of python we're using
PY_VERSION_STR = ".".join(map(str, sys.version_info[:3]))

# Default AmqpConnection values
DEFAULT_HOST = "localhost"
DEFAULT_USER = "guest"
DEFAULT_PASS = "guest"  # noqa
DEFAULT_PORT = 5672
DEFAULT_VHOST = "/"
DEFAULT_EXCHANGE = ""
DEFAULT_ROUTE_KEY = None

DEFAULT_CONN_PARAMS: "AmqpConnectionParams" = {
    "host": DEFAULT_HOST,
    "password": DEFAULT_PASS,
    "port": DEFAULT_PORT,
    "user": DEFAULT_USER,
    "vhost": DEFAULT_VHOST,
}

# URI information
URI_AMQP_IDENT = "amqp://"
URI_AMQPS_IDENT = "amqps://"


# Type information
class ConnectionProtocol(Protocol):
    """The methods a class needs to have in order to integrate with quickmq."""

    @property
    def identifier(self) -> str: ...

    @property
    def connected(self) -> bool: ...

    def connect(self) -> None: ...

    def produce(
        self,
        content_dict: Any,
        route_key: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> bool: ...

    def close(self) -> None: ...

    def refresh(self) -> None: ...


TypeConnection = TypeVar("TypeConnection", bound=ConnectionProtocol)
_RT = TypeVar("_RT")
_PT = ParamSpec("_PT")


class AmqpConnectionParams(TypedDict):
    """How to connect to a AMQP broker."""

    host: str
    user: str
    password: str
    port: int
    vhost: str


def catch_amqp_errors(func: Callable[_PT, _RT]) -> Callable[_PT, _RT]:
    """Utility decorator to catch all of Pika's AMQPConnectionError and
    raise them as built-in ConnectionError

    Args:
        func (Callable): Function to decorate
    """

    @functools.wraps(func)
    def wrapper(*args: _PT.args, **kwargs: _PT.kwargs) -> _RT:
        try:
            return func(*args, **kwargs)
        except amqplib.Connection.recoverable_connection_errors as e:
            raise AMQPConnectionError from e

    return wrapper


def params_from_uri(uri: str) -> AmqpConnectionParams:
    """Parse an amqp uri and return the parameters to open the connection.
    See ``https://www.rabbitmq.com/docs/uri-spec`` for the uri format.

    Raises:
        NotImplementedError: If the "amqps" URI scheme is used.
        URIFormatError: If ``uri`` doesn't follow the AMQP URI scheme.

    Args:
        uri (str): The uri to parse.

    Returns:
        AmqpConnectionParams: The connection parameters.
    """
    if not isinstance(uri, str):
        msg = f"<{type(uri)}> cannot be a valid URI!"
        raise URIFormatError(msg)

    if uri.startswith(URI_AMQPS_IDENT):
        msg = "The amqps URI specification is not yet supported!"
        raise NotImplementedError(msg)

    if not uri.startswith(URI_AMQP_IDENT):
        msg = "Misformatted URI: URI must start with 'amqp://'"
        raise URIFormatError(msg)

    try:
        parts = urlparse(uri)
    except ValueError as e:
        raise URIFormatError from e

    vhost = urlunquote(parts.path[1:]) if parts.path else DEFAULT_VHOST

    host = urlunquote(parts.hostname) if parts.hostname is not None else DEFAULT_HOST
    user = urlunquote(parts.username) if parts.username is not None else DEFAULT_USER
    pswd = urlunquote(parts.password) if parts.password is not None else DEFAULT_PASS

    # Reading the port attribute will raise a ValueError if an invalid port is
    # specified in the URL.
    try:
        port = parts.port
    except ValueError as e:
        raise URIFormatError from e

    return {
        "host": host,
        "password": pswd,
        "port": port or DEFAULT_PORT,
        "user": user,
        "vhost": vhost,
    }


# Exceptions that are used within ssec_amqp
class AMQPConnectionError(ConnectionError):
    """All purpose error for any problems with the AMQP connection."""

    def __init__(self, *args: object) -> None:
        super().__init__(*args)

    def __str__(self) -> str:
        msg = "AMQPConnectionError"
        if self.__cause__ is not None:
            msg += f" from {self.__cause__.__class__} ({self.__cause__})"
        return msg


class StateError(Exception):
    """Wrong state to perform an action."""

    def __init__(self, action: str, state_info: Optional[str]) -> None:
        msg = f"Cannot perform {action} in this state"
        if state_info is None:
            msg += "!"
        else:
            msg += f"({state_info})!"
        super().__init__(msg)


class URIFormatError(ValueError):
    """Error parsing an amqp URI."""


class AmqpConnection(ConnectionProtocol):
    """A connection to an AMQP broker."""

    def __init__(
        self,
        host: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        exchange: Optional[str] = None,
        vhost: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """Declare a connection to an AMQP broker. Note this will not
        actually create a network connection, use ``AmqpConnection.connect``.

        Args:
            host (str): Hostname of the broker.
            user (str): The user to initialize the connection with.
            password (str): The password to initialize the connection with.
            vhost (Optional[str], optional): The vhost on the broker to connection to.
            Defaults to None.
            port (Optional[int], optional): The port to connect with. Defaults to None.
            exchange (Optional[str], optional): The default exchange to interact
            with on the broker. Defaults to None.
        """
        self.host = host
        self.user = user or DEFAULT_USER
        self.vhost = vhost or DEFAULT_VHOST
        self.port = port or DEFAULT_PORT
        self.exchange = exchange or DEFAULT_EXCHANGE
        self.__password = password or DEFAULT_USER

        self.__conn = self._amqp_connection_factory()
        self.__chan = None

    @classmethod
    def from_uri(cls: Type["AmqpConnection"], uri: str) -> "AmqpConnection":
        """Create an AmqpConnection from an AMQP URI."""
        return cls(**params_from_uri(uri))

    @property
    def connected(self) -> bool:
        status = self.__conn.connected
        if status is None:
            return False
        return status  # type: ignore [no-any-return]

    @property
    def identifier(self) -> str:
        return str(self)

    def _amqp_connection_factory(self) -> amqplib.Connection:
        """Factory method for creating the underlying amqp transport mechanism."""
        return amqplib.Connection(
            f"{self.host}:{self.port}",
            userid=self.user,
            password=self.__password,
            virtual_host=self.vhost,
            confirm_publish=True,
            connect_timeout=0.25,
            client_properties={
                "product": "QuickMQ Python Client Library",
                "product_version": __version__,
                "platform": "Python {}".format(PY_VERSION_STR),
            },
        )

    @catch_amqp_errors
    def connect(self) -> None:
        """Open the network connection to the AMQP broker."""
        if self.connected:
            LOG.debug("%s - connect() called, but already connected...", str(self))
            return

        LOG.info("%s - attempting connection...", str(self))
        if self.__conn.channels is None:
            # Connection previously closed.
            LOG.info("%s - creating new amqplib object", str(self))
            self.__conn = self._amqp_connection_factory()
        self.__conn.connect()
        LOG.info("%s - connected", str(self))
        self.__chan = self.__conn.channel()

    @catch_amqp_errors
    def produce(self, content_dict: Any, route_key: Optional[str] = None, exchange: Optional[str] = None) -> bool:
        """Send a message to the AMQP Broker.

        Args:
            content_dict (JSON): The body of the message to produce.
            key (Optional[str], optional): key to send with. Defaults to None.
            exchange (Optional[str]): The exchange to send the message to. Defaults
            to the exchange declared with this AmqpConnection instance.

        Raises:
            AmqpConnectionError: If there is a problem with the connection when publishing.

        Returns:
            bool: Was the message delivered?
        """
        self.refresh()
        content_json = json.dumps(content_dict)
        route_key = route_key or DEFAULT_ROUTE_KEY
        dst_exch = exchange or self.exchange
        LOG.debug("%s - Attempting publish to exchange '%s' with key '%s'", self, dst_exch, route_key)
        try:
            self.__chan.basic_publish(  # type: ignore [attr-defined]
                msg=amqplib.Message(
                    body=content_json,
                    content_type="application/json",
                    content_encoding="utf-8",
                ),
                exchange=dst_exch,
                routing_key=route_key,
                timeout=5,
            )
        except MessageNacked:
            LOG.error("%s - message nacked!", str(self))
            return False
        except socket.timeout:
            LOG.error("%s - publish timeout!", str(self))
            raise  # This will get caught by @catch_amqp_errors
        except ChannelError as e:
            LOG.error("%s - Channel received an error! %s", self, e)
            # Make sure it's possible to use this AmqpConnection after an error
            self.__chan = self.__conn.channel()
            raise  # This will get caught by @catch_amqp_errors
        else:
            LOG.debug("%s - Successfully published", self)
            return True

    @catch_amqp_errors
    def refresh(self) -> None:
        """Refresh the AMQP connection, assure that it is still connected.

        Raises:
            StateError: If the exchange is not connected.
        """
        if self.__conn.connected is None:
            raise StateError(action="refresh", state_info="call connect()")
        try:
            self.__conn.heartbeat_tick()
        except amqplib.ConnectionForced:
            LOG.info("%s - missed heartbeat, trying reconnect", str(self))
            self.connect()  # Try again on heartbeat misses

    @catch_amqp_errors
    def close(self) -> None:
        """Closes the connection."""
        self.__conn.collect()

    def __hash__(self) -> int:
        return hash(self.identifier)

    def __repr__(self) -> str:
        return AMQP_EXCHANGE_ID_FORMAT.format(
            user=self.user,
            host=self.host,
            port=self.port,
            vhost=self.vhost,
            exchange=self.exchange,
        )

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, self.__class__):
            return False
        return (
            __value.host == self.host
            and __value.exchange == self.exchange
            and __value.user == self.user
            and __value.port == self.port
            and __value.vhost == self.vhost
        )


class AmqpExchange(AmqpConnection):
    """An abstraction of an AMQP exchange.
    Note: This class is deprecated, use AmqpConnection instead."""

    def __init__(
        self,
        host: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        exchange: Optional[str] = None,
        vhost: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        warnings.warn(
            "The AmqpExchange namespace will be removed in the future. Please use AmqpConnection instead.",
            DeprecationWarning,
            stacklevel=1,
        )
        super().__init__(host, user, password, exchange, vhost, port)


class ClusteredConnection(ConnectionProtocol, Generic[TypeConnection]):
    """A connection to a cluster. ClusteredConnection will use a primary connection to interact
    with the cluster. When that connection is disrupted, it will choose another primary."""

    def __init__(
        self,
        connections: Iterable[TypeConnection],
    ) -> None:
        self._connections: Dict[str, TypeConnection] = {proto.identifier: proto for proto in connections}
        if not self._connections:
            msg = "Cannot create a cluster with no nodes!"
            raise ValueError(msg)
        self._primary: Optional[TypeConnection] = None
        LOG.debug("Initialized %s", self)

    @classmethod
    def from_uris(
        cls: Type["ClusteredConnection[AmqpConnection]"], *uris: str
    ) -> "ClusteredConnection[AmqpConnection]":
        """Connect to a RabbitMQ cluster by specifying a collection of AMQP URIs."""
        return cls(AmqpConnection.from_uri(uri) for uri in uris)

    @property
    def identifier(self) -> str:
        """A unique identifier for this ClusteredConnection."""
        return str(self)

    @property
    def connected(self) -> bool:
        """Is there an active connection to the cluster?"""
        if self._primary is None:
            # Haven't called ``connect`` yet
            return False
        # Could possibly use any(), but don't want to accidentally iterate extra candidates
        for candidate in self._cluster_candidates():  # noqa
            if candidate.connected:
                LOG.info("%s - is connected to cluster through %s", self, candidate)
                return True
        return False

    @property
    def primary(self) -> Optional[TypeConnection]:
        """Get the active connection to the cluster. This will be None if ``connect``
        hasn't been called yet or all connections to the cluster are broken."""
        return self._primary

    def _cluster_candidates(self) -> Iterable[TypeConnection]:
        """Yield a series of "candidates" (open connections) to the cluster.
        If the generator is exhausted, no connections are available to the cluster.

        Use ``GeneratorExit`` to signal that the candidate worked, e.g.
        ```
        for conn in self._cluster_candidates():
            try:
                conn.publish("ha")
            except Exception:
                # Candidate didn't work
                continue
            else:
                # Candidate did work!
                return  # Leaving early signals a GeneratorExit
        ```

        Note: This method will automatically update this instances ``primary``.

        Yields:
            AmqpConnection: Possible candidate.
        """
        LOG.debug("%s - Generating node candidates", self)
        candidates = list(self._connections.values())
        # Connect to cluster's connections in random order
        random.shuffle(candidates)

        # But always consider the primary first
        if self._primary is not None:
            LOG.debug("%s - A primary exists (%s), consider it first", self, self._primary)
            candidates.remove(self._primary)
            candidates.insert(0, self._primary)

        for candidate in candidates:
            try:
                # Assure the connection is still open
                candidate.connect()
            except AMQPConnectionError:
                LOG.info("%s - AMQPError when connecting to cluster node %s", self, candidate)
                continue
            # Candidate is valid, can the caller use it?
            LOG.debug("%s - Connected to candidate %s, yielding for caller", self, candidate)
            try:
                yield candidate
            except GeneratorExit:
                # Worked for them
                if candidate != self._primary:
                    LOG.debug("%s - New primary %s", self, candidate)
                    self._primary = candidate
                return
            else:
                # Didn't work for them (they want another candidate)
                LOG.debug("%s - Caller rejected candidate %s", self, candidate)
        LOG.error("%s - Couldn't find an acceptable connection candidate!")
        self._primary = None

    def connect(self) -> None:
        """Connect to a RabbitMQ cluster. This will only open one connection at
        a time, changing which node it connects to when there's an error.
        """
        LOG.debug("%s - Connecting to cluster", self)
        for conn in self._cluster_candidates():
            if conn.connected:
                LOG.info("%s Connected to cluster through %s", self, conn)
                return
        msg = f"{self} - Couldn't connect to any nodes in the cluster!"
        LOG.error(msg)
        raise AMQPConnectionError(msg)

    def produce(
        self,
        content_dict: Any,
        route_key: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> bool:
        """Send an AMQP message the cluster. This will only deliver the message
        to one node, relying on RabbitMQ's duplication to get the message to all
        nodes in the cluster.

        Args:
            content_dict (Any): A JSON-serializable object to send.
            route_key: (str | None): The route key to send the message with.
            exchange (str | None): The exchange to send the message to, will
            default to the exchange that this ClusteredConnection was initialized with.

        Returns:
            bool: True if delivered, False if nacked.
        """
        for conn in self._cluster_candidates():
            try:
                return conn.produce(content_dict, route_key, exchange)
            except AMQPConnectionError:
                pass
        # If none of the connections can publish the message, error
        msg = f"{self} - Couldn't deliver payload to the cluster!"
        LOG.error(msg)
        raise AMQPConnectionError(msg)

    def close(self) -> None:
        """Closes all connections to the cluster."""
        for conn in self._connections.values():
            conn.close()
        LOG.info("%s - Closed all connections to cluster.", self)

    def refresh(self) -> None:
        """Make sure the connection to the cluster is still open."""
        if self._primary is None:
            raise StateError(action="refresh", state_info="call connect()")
        self._primary.refresh()

    def __hash__(self) -> int:
        return hash(self.identifier)

    def __repr__(self) -> str:
        conns = ", ".join(sorted(map(str, self._connections)))
        return f"ClusteredConnection({conns})"

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, self.__class__):
            return False
        if len(self._connections) != len(__value._connections):
            return False
        # Compare by the connection's identifiers
        for ours, theirs in zip(sorted(self._connections.keys()), sorted(__value._connections.keys())):
            if ours != theirs:
                return False
        return True
