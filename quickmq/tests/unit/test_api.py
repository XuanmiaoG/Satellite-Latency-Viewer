"""ssec_amqp.test_api
~~~~~~~~~~~~~~~~~~

Unit tests for quickmq's API.
"""

from unittest import mock

import pytest
import ssec_amqp.api as mq


@pytest.fixture
def mock_api_client():
    with mock.patch.object(mq, "__CLIENT") as mock_client:
        yield mock_client
        mock_client.reset_mock(return_value=True, side_effect=True)


def test_configure(mock_api_client):
    new_reconnect = 100
    mq.configure(reconnect_window=new_reconnect)
    assert mock_api_client.reconnect_window == new_reconnect


def test_status(mock_api_client):
    cons = {"test": "reconnecting"}
    m_conns = mock.PropertyMock(return_value=cons)
    type(mock_api_client).connections = m_conns
    status = mq.status()
    assert status == cons


def test_disconnect(mock_api_client):
    mq.disconnect()
    mock_api_client.disconnect.assert_called_once_with()


def test_connect(mock_api_client):
    args = ["test1", "test2"]
    kwargs = {"user": "test", "vhost": "/f"}
    with mock.patch.object(mq.AmqpConnection, "connect"):
        mq.connect(*args, **kwargs)
        calls = [mock.call(mq.AmqpConnection(dest, **kwargs)) for dest in args]
        mock_api_client.connect.assert_has_calls(calls, any_order=True)
        assert mock_api_client.connect.call_count == 2


def test_publish(mock_api_client):
    mq.publish("new_message", route_key="hi")
    mock_api_client.publish.assert_called_once_with("new_message", route_key="hi", exchange=None)


def test_publish_exchange(mock_api_client):
    mq.publish("new_message", route_key="hi", exchange="test")
    mock_api_client.publish.assert_called_once_with("new_message", route_key="hi", exchange="test")
