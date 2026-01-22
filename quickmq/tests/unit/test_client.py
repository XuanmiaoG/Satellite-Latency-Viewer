from unittest import mock

import pytest
import ssec_amqp as mq
from ssec_amqp._retry import LazyRetry
from ssec_amqp.client import DEFAULT_RECONNECT_WINDOW, AMQPConnectionError


def is_reconnecting(client: mq.AmqpClient, exchange: mq.AmqpConnection) -> bool:
    return client.connections.get(exchange.identifier) == mq.ConnectionStatus.RECONNECTING


def is_connected(client: mq.AmqpClient, exchange: mq.AmqpConnection) -> bool:
    return client.connections.get(exchange.identifier) == mq.ConnectionStatus.CONNECTED


def is_disconnected(client: mq.AmqpClient, exchange: mq.AmqpConnection) -> bool:
    return not (is_reconnecting(client, exchange) or is_connected(client, exchange))


def test_init():
    cl = mq.AmqpClient()
    assert cl.connections == {}
    assert cl.reconnect_window == DEFAULT_RECONNECT_WINDOW

    new_window = 20
    cl = mq.AmqpClient(new_window)
    assert cl.connections == {}
    assert cl.reconnect_window == new_window


def test_connect_err():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connect.side_effect = AMQPConnectionError
        mock_exch.connected = False
        cl.connect(mock_exch)
        assert is_reconnecting(cl, mock_exch)


def test_connect():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connected = False
        cl.connect(mock_exch)
        mock_exch.connect.assert_called_once()
        assert is_connected(cl, mock_exch)
        assert not is_reconnecting(cl, mock_exch)


def test_exch_already_connected():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connected = True
        cl.connect(mock_exch)
        mock_exch.connect.assert_not_called()
        assert is_connected(cl, mock_exch)
        assert not is_reconnecting(cl, mock_exch)


def test_already_connected():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        assert not is_connected(cl, mock_exch)
        mock_exch.connected = True
        cl.connect(mock_exch)
        cl.connect(mock_exch)
        mock_exch.connect.assert_not_called()
        assert is_connected(cl, mock_exch)


def test_connect_when_reconnecting():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connected = False
        cl.connect(mock_exch)
        mock_exch.refresh.side_effect = AMQPConnectionError
        cl.refresh_pools()
        assert is_reconnecting(cl, mock_exch)
        mock_exch.connect.reset_mock()
        mock_exch.refresh.side_effect = None

        cl.connect(mock_exch)
        mock_exch.connect.assert_called_once()
        assert is_connected(cl, mock_exch)


def test_connect_when_reconnecting_error():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connected = False
        cl.connect(mock_exch)
        mock_exch.refresh.side_effect = AMQPConnectionError
        cl.refresh_pools()
        assert is_reconnecting(cl, mock_exch)
        mock_exch.connect.reset_mock()

        mock_exch.connect.side_effect = AMQPConnectionError
        cl.connect(mock_exch)
        mock_exch.connect.assert_called()
        assert is_reconnecting(cl, mock_exch)


def test_disconnect_one_con():
    """Properly disconnect singular exchange when it's connected."""
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        cl.connect(mock_exch)
        mock_exch.reset_mock()
        cl.disconnect(exchange=mock_exch)
        mock_exch.close.assert_called_once()
        assert is_disconnected(cl, mock_exch)


def test_disconnect_one_recon():
    """Properly disconnect singular exchange when it's reconnecting."""
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connect.side_effect = AMQPConnectionError
        mock_exch.connected = False
        cl.connect(mock_exch)
        mock_exch.reset_mock()
        cl.disconnect(exchange=mock_exch)
        mock_exch.close.assert_called_once()
        assert is_disconnected(cl, mock_exch)


def test_disconnect_one_dne():
    """Disconnecting an exchnage that isn't connected should raise a ValuError."""
    cl = mq.AmqpClient()
    ex = mq.AmqpConnection("localhost")
    with pytest.raises(ValueError, match=str(ex)):
        cl.disconnect(exchange=ex)


def test_disconnect_all():
    """Amqpclient.disconnect() w/o arguments disconnects all exchanges."""
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection"):
        mock_exch1 = mock.Mock(wraps=mq.AmqpConnection)
        mock_exch2 = mock.Mock(wraps=mq.AmqpConnection)
        cl.connect(mock_exch1)
        cl.connect(mock_exch2)

        # make this exchange 'reconnect'
        mock_exch1.refresh.side_effect = AMQPConnectionError
        cl.refresh_pools()
        mock_exch1.reset_mock()

        cl.disconnect()
        mock_exch1.close.assert_called_once()
        mock_exch2.close.assert_called_once()
        assert is_disconnected(cl, mock_exch2)
        assert is_disconnected(cl, mock_exch1)
        assert cl.connections == {}


def test_refresh_to_reconnect():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connected = False
        cl.connect(mock_exch)
        assert is_connected(cl, mock_exch)
        mock_exch.refresh.reset_mock()

        mock_exch.refresh.side_effect = AMQPConnectionError
        cl.refresh_pools()
        mock_exch.refresh.assert_called_once()
        assert is_reconnecting(cl, mock_exch)


def test_reconnect_close():
    """Make sure _to_reconnect closes connection before attempting reconnect."""
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connect.side_effect = AMQPConnectionError
        mock_exch.connected = False
        cl.connect(mock_exch)
        assert is_reconnecting(cl, mock_exch)
        mock_exch.close.assert_called_once()


def test_name():
    """Make sure setting name updates property."""
    test_name = "hi there"
    cl = mq.AmqpClient(name=test_name)
    assert cl.name == test_name


@pytest.mark.skip
def test_refresh_to_connect():
    cl = mq.AmqpClient(time_between_reconnects=0)
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        mock_exch.connected = False
        cl.connect(mock_exch)
        assert is_connected(cl, mock_exch)
        mock_exch.refresh.side_effect = AMQPConnectionError
        cl.refresh_pools()
        assert is_reconnecting(cl, mock_exch)

        mock_exch.connect.return_value = LazyRetry.FAILED_ATTEMPT
        cl.refresh_pools()
        assert is_reconnecting(cl, mock_exch)

        mock_exch.refresh.side_effect = None
        mock_exch.connect.return_value = None
        cl.refresh_pools()
        assert is_connected(cl, mock_exch)


def test_publish_no_connections():
    cl = mq.AmqpClient()
    assert cl.publish("hi") == {}


def test_publish_success():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        cl.connect(mock_exch)
        assert is_connected(cl, mock_exch)

        mock_exch.produce.return_value = True

        status = cl.publish("hi")
        assert status[mock_exch.identifier] == mq.DeliveryStatus.DELIVERED


def test_publish_nack():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        cl.connect(mock_exch)
        assert is_connected(cl, mock_exch)

        mock_exch.produce.return_value = False

        status = cl.publish("hi")
        assert status[mock_exch.identifier] == mq.DeliveryStatus.REJECTED


def test_publish_error():
    cl = mq.AmqpClient()
    with mock.patch("ssec_amqp.AmqpConnection") as mock_exch:
        cl.connect(mock_exch)
        assert is_connected(cl, mock_exch)

        mock_exch.produce.side_effect = AMQPConnectionError
        mock_exch.connect.side_effect = AMQPConnectionError

        status = cl.publish("hi")
        assert status[mock_exch.identifier] == mq.DeliveryStatus.DROPPED
        assert is_reconnecting(cl, mock_exch)
