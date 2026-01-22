"""integration.test_exch

Make sure ``ssec_amqp.amqp`` works as expected when interacting with a broker.
"""

import json

import amqp
import pytest
from ssec_amqp.amqp import AmqpConnection

from tests.integration.conftest import ParallelTopologyFactory


def test_connection(parallel_containers: ParallelTopologyFactory):
    """Make sure the AmqpConnection connects and disconnects."""
    topo = parallel_containers(1)
    params = topo.connection_params[0]
    ex = AmqpConnection(**params)
    ex.connect()
    assert ex.connected
    assert topo.num_connections == 1

    ex.close()
    assert topo.num_connections == 0


def test_connect_auth_error(parallel_containers: ParallelTopologyFactory):
    """Make sure connecting with bad authentication raises a connection error."""
    topo = parallel_containers(1)
    params = topo.connection_params[0]

    unauth_user = "HACKER_MAN"
    unauth_pass = "leet"  # noqa
    exch = AmqpConnection(host=params["host"], port=params["port"], user=unauth_user, password=unauth_pass)

    with pytest.raises(amqp.AccessRefused):
        exch.connect()


def test_produce(parallel_containers: ParallelTopologyFactory):
    """Make sure messages are published properly."""
    topo = parallel_containers(1)
    params = topo.connection_params[0]

    # Use the default amqp exchange
    # => When routing key == queue name, queue will receive the message
    ex = AmqpConnection(**params, exchange="")
    ex.connect()

    # Set up RabbitMQ queue to receive the message
    q_name = "test"
    topo.exec(f"rabbitmqadmin declare queue name={q_name}")

    # Publish the message to the queue
    send_msg = {"hi": "there"}
    ex.produce(send_msg, route_key=q_name)

    # Get the message from RabbitMQ
    msg_info = topo.check_exec(f"rabbitmqadmin --format raw_json get queue={q_name}")[0]
    msg_info = json.loads(msg_info)[0]  # Get info for the first message

    assert msg_info["routing_key"] == q_name
    assert msg_info["properties"]["content_type"] == "application/json"
    assert json.loads(msg_info["payload"]) == send_msg

    ex.close()


def test_channel_auto_reopen(parallel_containers: ParallelTopologyFactory):
    """Make sure that AmqpConnection automatically re-opens the AMQP channel
    used for publishing after it has been closed, e.g. when publishing to non-existent exchange."""
    topo = parallel_containers(1)
    params = topo.connection_params[0]

    ex = AmqpConnection(**params, exchange="")
    ex.connect()

    # Action that closes the AMQP channel
    with pytest.raises(amqp.NotFound):
        ex.produce("hi", route_key="", exchange="DNE")

    # Can we stil publish after a closed channel?
    assert ex.produce("test again")

    ex.close()
