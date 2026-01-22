"""integration.conftest
~~~~~~~~~~~~~~~~~~~~

Configuration for integration tests.
"""

import json
import logging
from typing import Protocol

import pytest
from testcontainers.core.network import Network

from .rmq_tcontainer import ContainerTopology, TopologyConfig, callback_compose

# Causes too much garbedly gook in test failures
logging.getLogger("pika").propagate = False
logging.getLogger("testcontainers").propagate = False
logging.getLogger("urllib3").propagate = False
logging.getLogger("docker").propagate = False

# The parallel container topologies that are currently running.
ACTIVE_PARALLEL_CONTAINERS: dict[int, ContainerTopology] = {}

# The clustered container topologies that are currently running.
ACTIVE_CLUSTER_CONTAINERS: dict[int, ContainerTopology] = {}

# The networks that the container topologies are communicating on
ACTIVE_CLUSTER_NETWORKS: dict[int, Network] = {}


class ParallelTopologyFactory(Protocol):
    def __call__(self, num_containers: int) -> ContainerTopology: ...


class ClusterTopologyFactory(Protocol):
    def __call__(self, num_containers: int) -> ContainerTopology: ...


# on_start callbacks for ContainerToplogy


def start_with_rabbitmqadmin(topo: ContainerTopology):
    """Topology on_start callback that installs the `rabbitmqadmin` command."""
    topo.check_exec(
        """
        python3 -c
        "src='http://localhost:15672/cli/rabbitmqadmin';
        dst='/usr/local/bin/rabbitmqadmin';
        from urllib.request import urlretrieve;
        urlretrieve(src,dst)"
        """.replace("\n", " ")
    )


def start_as_cluster(topo: ContainerTopology):
    """Topology on_start callback that makes the containers cluster together."""

    main_node = "rabbit@rabbit0"
    for i in range(1, topo.num_containers, 1):
        topo.get(i).exec(f"rabbitmqctl join_cluster {main_node}")

    # Assure ourselves that we're actually in a cluster
    statuses = [json.loads(status) for status in topo.check_exec("rabbitmqctl --formatter json cluster_status")]
    assert all([len(status["running_nodes"]) == topo.num_containers for status in statuses])


@pytest.fixture
def parallel_containers(request):
    """Fixture to create/manage RabbitMQ containers. Returns a function that,
    when called, creates the containers.
    """

    def parallel_start_inner(num_containers: int) -> ContainerTopology:
        """Start a parallel RabbitMQ container topology with ``num_containers`` containers.
        A parallel topology is one where the containers act independently.

        Args:
            num_containers (int): The number of independent containers in the topology.

        Returns:
            ContainerTopology: An object to interface with the container topology.
        """
        if num_containers in ACTIVE_PARALLEL_CONTAINERS:
            topo = ACTIVE_PARALLEL_CONTAINERS[num_containers]
        else:
            topo = ContainerTopology(TopologyConfig(num_containers=num_containers, on_start=start_with_rabbitmqadmin))
            topo.start()
            ACTIVE_PARALLEL_CONTAINERS[num_containers] = topo

        def cleanup():
            ACTIVE_PARALLEL_CONTAINERS[num_containers].reset()

        request.addfinalizer(cleanup)
        return topo

    return parallel_start_inner


@pytest.fixture
def cluster_containers(request):
    """Fixture to create/manage RabbitMQ containers. Returns a function that,
    when called, creates the containers.
    """

    def cluster_start_inner(num_containers: int) -> ContainerTopology:
        """Start a clustered RabbitMQ container topology with ``num_containers`` containers.
        A clustered topology is one where the containers act a one unit.
        See RabbitMQ Clusters for more information.

        Args:
            num_containers (int): The number of collaborative containers in the topology.

        Returns:
            ContainerTopology: An object to interface with the container topology.
        """
        if num_containers in ACTIVE_CLUSTER_CONTAINERS:
            topo = ACTIVE_CLUSTER_CONTAINERS[num_containers]
        else:
            network = Network()
            network.create()
            cfg = TopologyConfig(
                num_containers=num_containers,
                on_start=callback_compose(start_with_rabbitmqadmin, start_as_cluster),
                default_env={
                    "RABBITMQ_ERLANG_COOKIE": "mmmmcookie",
                },
                # Make sure the containers can talk with eachother
                network=network,
                network_aliases=[f"rabbit{i}" for i in range(num_containers)],
                container_env={i: {"RABBITMQ_NODENAME": f"rabbit@rabbit{i}"} for i in range(num_containers)},
            )
            topo = ContainerTopology(cfg)
            topo.start()
            ACTIVE_CLUSTER_CONTAINERS[num_containers] = topo

        def cleanup():
            ACTIVE_CLUSTER_CONTAINERS[num_containers].reset()

        request.addfinalizer(cleanup)
        return topo

    return cluster_start_inner


def pytest_sessionfinish(session, exitstatus):  # noqa
    """Called when pytest is done. Shutdown the remaining containers."""
    for topo in ACTIVE_PARALLEL_CONTAINERS.values():
        topo.stop()
    for topo in ACTIVE_CLUSTER_CONTAINERS.values():
        topo.stop()
    for net in ACTIVE_CLUSTER_NETWORKS.values():
        net.remove()
