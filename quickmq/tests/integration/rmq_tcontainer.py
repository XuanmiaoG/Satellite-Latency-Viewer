"""integration.topology

Create and test against RabbitMQ containers.
"""

from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, TypedDict, Unpack

from testcontainers.core.network import Network
from testcontainers.rabbitmq import RabbitMqContainer

# Command to use to get RabbitMQ connections
CONNECTION_LIST_CMD = "rabbitmqctl --silent list_connections pid"

# Topology defaults
DEFAULT_IMAGE = "rabbitmq:4.0-management"
DEFAULT_PORT = 5672
DEFAULT_USER = "guest"
DEFAULT_PASS = "guest"  # noqa

DEFAULT_CONTAINER_PARAMS: "RabbitMqContainerParams" = {
    "image": DEFAULT_IMAGE,
    "password": DEFAULT_PASS,
    "port": DEFAULT_PORT,
    "username": DEFAULT_USER,
}


class ConnectionParameters(TypedDict):
    """Information for how to connect to a RabbitMQ container."""

    host: str
    user: str
    password: str
    port: int


class RabbitMqContainerQueryNamespace(TypedDict, total=False):
    """What information can be used with ContainerTopolgy.query"""

    port: int
    exposed_port: int
    internal_port: int
    username: str
    password: str
    vhost: str
    host: str


def queryns_from_container(c: RabbitMqContainer) -> RabbitMqContainerQueryNamespace:
    """Create a query namespace from a RabbitMqContainer."""
    con_params = c.get_connection_params()
    return {
        "exposed_port": con_params.port,
        "host": con_params.host,
        "internal_port": c.port,
        "password": c.password,
        "port": con_params.port,
        "username": c.username,
        "vhost": c.vhost,
    }


class RabbitMqContainerParams(TypedDict, total=False):
    """Kwargs passed to RabbitMqContainer on creation"""

    image: str
    port: int
    username: str
    password: str


TopologyCallback = Callable[["ContainerTopology"], None]


@dataclass
class TopologyConfig:
    """Defines which/how many containers to start and in what way."""

    # The number of containers to start
    num_containers: int = field(default=1)

    # Callback function for when the topology starts
    on_start: TopologyCallback | None = field(default=None)

    # Callback function for when the topology is reset
    on_reset: TopologyCallback | None = field(default=None)

    # Callback function for when the topology is stopped
    on_stop: TopologyCallback | None = field(default=None)

    # Default arguments used to construct all RabbitMqContainers
    default_image: str = field(default=DEFAULT_IMAGE)
    default_user: str = field(default=DEFAULT_USER)
    default_pass: str = field(default=DEFAULT_PASS)

    # Default environment variables for all RabbitMqContainers
    default_env: dict[str, str] = field(default_factory=dict)

    # Keyword arguments for individual containers, key is index of container
    container_param: dict[int, RabbitMqContainerParams] = field(default_factory=dict)

    # Environment variables for individual containers, key is index of container
    container_env: dict[int, dict[str, str]] = field(default_factory=dict)

    # The docker/podman network to put the containers on
    network: Network | None = field(default=None)

    # Names of the containers on the network, the list must have num_containers elements
    network_aliases: List[str] | None = field(default=None)


def callback_compose(*funcs: TopologyCallback) -> TopologyCallback:
    """Use multiple callbacks at once."""

    def callback(topo: ContainerTopology) -> None:
        for func in funcs:
            func(topo)

    return callback


class ContainerTopology:
    """Run and interact with multiple RabbitMQ test containers at once."""

    def __init__(self, config: TopologyConfig) -> None:
        # How to connect to each container
        self._params: list[ConnectionParameters] = []

        # Callback functions
        self._on_start = config.on_start
        self._on_reset = config.on_reset
        self._on_stop = config.on_stop

        # Initialize our containers from the configuration
        self._container_list: list[RabbitMqContainer] = []

        default_params: RabbitMqContainerParams = {
            "image": config.default_image,
            "username": config.default_user,
            "password": config.default_pass,
        }
        default_env = config.default_env

        for i in range(config.num_containers):
            # Construct container
            params = default_params.copy()
            params.update(config.container_param.get(i, {}))
            c = RabbitMqContainer(**params)

            # Add environment variables
            env = default_env.copy()
            env.update(config.container_env.get(i, {}))
            for key, val in env.items():
                c.with_env(key, val)

            if config.network is not None:
                c.with_network(config.network)
            if config.network_aliases is not None:
                c.with_network_aliases(config.network_aliases[i])

            self._container_list.append(c)

    @property
    def connection_params(self) -> list[ConnectionParameters]:
        return self._params

    @property
    def num_containers(self) -> int:
        """Number of containers runnings in the topology."""
        return len(self._container_list)

    @property
    def num_connections(self) -> int:
        """Number of active RabbitMQ connections across the topology."""
        pids = {}  # Only get the number of unique PIDs
        for pid_list in self.check_exec(CONNECTION_LIST_CMD):
            for pid in pid_list.splitlines():
                pids[pid] = 1
        return len(pids.keys())

    def start(self) -> None:
        """Start all of the test containers within the topology in parallel."""
        with concurrent.futures.ThreadPoolExecutor() as executer:
            futures = [executer.submit(container.start) for container in self._container_list]
            for future in concurrent.futures.as_completed(futures):
                future.exception()
        self._params = [
            {
                "host": container.get_container_host_ip(),
                "port": int(container.get_exposed_port(container.port)),
                "password": "guest",
                "user": "guest",
            }
            for container in self._container_list
        ]
        if self._on_start is not None:
            self._on_start(self)

    def stop(self) -> None:
        """Stop all of the test containers within the topology."""
        for container in self._container_list:
            container.stop()
        if self._on_stop is not None:
            self._on_stop(self)

    def reset(self) -> None:
        """Reset the state of all the test containers within the topology."""
        for container in self._container_list:
            container.exec("rabbitmqctl reset")
            container.exec("rabbitmqctl close_all_connections")
        if self._on_reset is not None:
            self._on_reset(self)

    def exec(self, command: str) -> list[tuple[int, bytes]]:
        """Run a command on all the test containers in the topology.

        Args:
            command (str): The command to run.

        Returns:
            List[tupel[int, bytes]]: A tuple of the return code and the output
            (in bytes) of the command in each container.
        """
        return [c.exec(command) for c in self._container_list]

    def check_exec(self, command: str) -> list[str]:
        """Run a command on all test containers in the topology. Raise an
        assertion error if any of the commands fail (return code != 0).

        Args:
            command (str): The command to run.

        Returns:
            List[str]: The results of the command in each container it was run in.
        """
        results = []
        for container in self._container_list:
            rc, output = container.exec(command)
            output = output.decode("utf-8")
            if rc != 0:
                msg = f"Command {command} failed on container {container}! '{output}'"
                raise AssertionError(msg)
            results.append(output)
        return results

    def get(self, container: int) -> RabbitMqContainer:
        """Get a specific container in the topology to act on.

        Args:
            Container (int): A number between [0..num_containers). The same
            number will always correspond to the same container.

        Returns:
            RabbitMqContainer: A RabbitMQ instance powered by testcontainers.

        """
        return self.__getitem__(container)

    def query(
        self, _matcher: Callable[[Iterable[bool]], bool] = all, **where: Unpack[RabbitMqContainerQueryNamespace]
    ) -> List[RabbitMqContainer]:
        """Get the RabbitMqContainer(s) within this topology that match a query.
        Valid query parameters are defined by ``RabbitMqContainerQueryNamespace``.

        Args:
            _matcher (Callable[[Iterable[bool]], bool]): Function used to tell if the query
            matched. Default is the built-in ``all``.
            **where (str): The query parameters to match.

        Returns:
            List[RabbitMqContainer]: All containers that match the query parameters.
        """
        matched = []
        for c in self._container_list:
            if _matcher(qval == queryns_from_container(c)[qprop] for qprop, qval in where.items()):
                matched.append(c)
        return matched

    def __getitem__(self, item: int) -> RabbitMqContainer:
        if isinstance(item, int):
            return self._container_list[item]
        raise ValueError(f"{self.__class__}.__getitem__ not supported for item of type {type(item)!r}!")

    def __enter__(self) -> "ContainerTopology":
        if not self.connection_params:
            self.start()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.stop()

    def __repr__(self) -> str:
        containers = ", ".join(map(str, self._container_list))
        return f"ContainerTopology({containers})"
