"""integration.test_api

Tests for ssec_amqp.api
"""

import time

import pytest
import ssec_amqp.api as mq
from ssec_amqp import ConnectionStatus, DeliveryStatus

from tests.integration.conftest import ClusterTopologyFactory, ParallelTopologyFactory


@pytest.mark.parametrize("n_containers", [1, 3])
def test_reconnect_abrupt(parallel_containers: ParallelTopologyFactory, n_containers):
    """Make sure client reconnects after an abrupt disconnect (broker tells us).
    e.g. rabbitmqctl stop_app"""
    topo = parallel_containers(n_containers)
    for params in topo.connection_params:
        mq.connect(**params)

    recon_interval = 1
    mq.configure(reconnect_interval=recon_interval)
    assert topo.num_connections == topo.num_containers

    ## Test reconnecting after "known" shutdown
    topo.exec("rabbitmqctl stop_app")
    # Sanity check to make sure the node is down
    for res in topo.exec("rabbitmqctl list_connections"):
        assert res[0] == 64

    # We won't know we're disconnected until we try to interact with the server again
    for res in mq.publish("hi").values():
        assert res is DeliveryStatus.DROPPED
    for status in mq.status().values():
        assert status is ConnectionStatus.RECONNECTING

    topo.exec("rabbitmqctl start_app")
    topo.exec("rabbitmqctl await_startup")
    # We must wait until the retry timer is good to go
    time.sleep(recon_interval)

    for status in mq.status().values():
        assert status is ConnectionStatus.CONNECTED
    assert topo.num_connections == topo.num_containers

    mq.disconnect()
    assert topo.num_connections == 0


@pytest.mark.skip
@pytest.mark.parametrize("n_containers", [1, 3])
def test_reconnect_silent(parallel_containers: ParallelTopologyFactory, n_containers):
    """Make sure client reconnects after a silent disconnect (broker doesn't tell us).
    e.g. an ethernet chord is yanked.
    """
    topo = parallel_containers(n_containers)
    recon_interval = 1
    mq.connect(**topo.connection_params[0])
    mq.configure(reconnect_interval=recon_interval)
    assert topo.num_connections == 1

    ## Test reconnecting after "silent" shutdown
    topo.exec("TBD")
    # We won't know we're disconnected until we try to interact with the server again
    mq.publish("hi")
    for status in mq.status().values():
        assert status is ConnectionStatus.RECONNECTING

    topo.exec("TBD")
    time.sleep(recon_interval)

    # We must wait until the retry timer is good to go
    for status in mq.status().values():
        assert status is ConnectionStatus.CONNECTED

    mq.disconnect()


@pytest.mark.parametrize("n_containers", [1, 3])
def test_publish_status_after_disconnect(parallel_containers: ParallelTopologyFactory, n_containers):
    """Make sure client.publish returns DeliveryStatus.DROPPED after disconnecting."""
    topo = parallel_containers(n_containers)
    for params in topo.connection_params:
        mq.connect(**params)
    assert topo.num_connections == topo.num_containers

    num_connected_pubs = 10
    for i in range(num_connected_pubs):
        pub_status = mq.publish({"i": i}).values()
        assert len(pub_status) == topo.num_containers
        assert all(status is DeliveryStatus.DELIVERED for status in pub_status)

    num_disconnected_pubs = 10
    topo.exec("rabbitmqctl stop_app")
    for i in range(num_disconnected_pubs):
        pub_status = mq.publish({"i": i}).values()
        assert len(pub_status) == topo.num_containers
        for status in pub_status:
            assert status is DeliveryStatus.DROPPED
    topo.exec("rabbitmqctl start_app")
    topo.exec("rabbitmqctl await_startup")
    mq.disconnect()


def test_connect_both(cluster_containers: ClusterTopologyFactory, parallel_containers: ParallelTopologyFactory):
    """Make sure that an additional cluster connection won't
    affect the non-clustered connections."""
    num_cluster_nodes = 1
    num_parallel_nodes = 2
    num_total_nodes = num_cluster_nodes + num_parallel_nodes

    ctopo = cluster_containers(num_cluster_nodes)
    ptopo = parallel_containers(num_parallel_nodes)

    for params in ptopo.connection_params:
        mq.connect(**params)

    mq.connect(**ctopo.connection_params[0], cluster=True)
    assert len(mq.status()) == num_total_nodes
    assert ctopo.num_connections == num_cluster_nodes
    assert ptopo.num_connections == num_parallel_nodes

    deliv_stat = mq.publish("hi")
    assert len(deliv_stat) == num_total_nodes
    for node, status in deliv_stat.items():
        assert status is DeliveryStatus.DELIVERED, f"Didn't deliver to {node}"

    ctopo.exec("rabbitmqctl stop_app")

    deliv_stat = mq.publish("hi again")
    assert len(deliv_stat) == num_total_nodes
    for node, status in deliv_stat.items():
        if "ClusteredConnection" in node:
            assert status is DeliveryStatus.DROPPED
        else:
            assert status is DeliveryStatus.DELIVERED

    mq.disconnect()
