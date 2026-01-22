"""integration.test_cluster

Make sure ClusteredConnection in
"""

from ssec_amqp.amqp import AmqpConnection, ClusteredConnection

from tests.integration.conftest import ClusterTopologyFactory


def test_connection(cluster_containers: ClusterTopologyFactory):
    """When connecting to a cluster, only 1 connection should be open at a time."""
    topo = cluster_containers(3)
    ex = ClusteredConnection(AmqpConnection(**params) for params in topo.connection_params)

    ex.connect()

    assert ex.connected
    assert topo.num_connections == 1

    ex.close()


def test_reconnect(cluster_containers: ClusterTopologyFactory):
    """When the primary connection gets disconnected, ClusteredConnection
    should switch to a different "server" in the cluster."""
    topo = cluster_containers(3)
    ex = ClusteredConnection(AmqpConnection(**params) for params in topo.connection_params)
    ex.connect()
    assert ex.connected
    assert topo.num_connections == 1

    assert ex.primary is not None
    init_primary = ex.primary

    # Get the 'server' that the primary is connected to
    servers = topo.query(port=init_primary.port)
    assert len(servers) == 1
    # And shut it down
    servers[0].exec("rabbitmqctl stop_app")
    # (make sure we realize it's shutdown)
    assert ex.produce("test")

    # We should still be connected...
    assert ex.connected
    # Make sure we're still connected servers-side
    # (can't use num_connections because not all servers are up')
    servers = topo.query(port=ex.primary.port)
    assert len(servers) == 1
    rc, out = servers[0].exec("rabbitmqctl --silent list_connections")
    assert rc == 0
    assert len(out.splitlines()) == 1

    # ...but with a different connection
    assert ex.primary.identifier != init_primary.identifier

    ex.close()
