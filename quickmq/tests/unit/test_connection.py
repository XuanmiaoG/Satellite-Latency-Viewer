from typing import Hashable
from unittest import mock
from urllib.parse import quote as urlquote

import pytest
from amqp.exceptions import ConnectionForced, MessageNacked, RecoverableConnectionError
from ssec_amqp import (
    AmqpConnection,
)
from ssec_amqp.amqp import (
    DEFAULT_EXCHANGE,
    DEFAULT_PORT,
    DEFAULT_VHOST,
    AMQPConnectionError,
    StateError,
)


def test_initialization():
    test_dest = "amqp"
    test_user = "u"
    test_pass = "p"
    test_port = 123
    test_vhost = "/new"
    test_exch = "model"

    ex = AmqpConnection(test_dest, test_user, test_pass)

    assert not ex.connected
    assert ex.host == test_dest
    assert ex.user == test_user
    assert ex.port == DEFAULT_PORT
    assert ex.exchange == DEFAULT_EXCHANGE
    assert ex.vhost == DEFAULT_VHOST

    ex1 = AmqpConnection(test_dest, test_user, test_pass, test_exch, test_vhost, test_port)

    assert ex1.port == test_port
    assert ex1.vhost == test_vhost
    assert ex1.exchange == test_exch


def test_uri_init():
    """Make sure the uri properly maps to AmqpConnection's values.
    unit.test_uri already tests for validity of uri parsing."""
    t_user = "myuser"
    t_port = 12
    t_vhost = "/"
    t_host = "myhost"
    uri = f"amqp://{t_user}@{t_host}:{t_port}/{urlquote(t_vhost)}"
    ex = AmqpConnection.from_uri(uri)
    assert ex.host == t_host
    assert ex.port == t_port
    assert ex.user == t_user
    assert ex.vhost == t_vhost


def test_connect_error():
    ex = AmqpConnection("localhost", "guest", "guest")
    with mock.patch.object(ex, "_AmqpConnection__conn") as mock_con:
        mock_con.connected = False
        mock_con.connect.side_effect = RecoverableConnectionError
        with pytest.raises(AMQPConnectionError):
            ex.connect()


@pytest.mark.parametrize(
    ("con1", "con2"),
    [
        (("host",), ("host",)),
        (("host1",), ("host2",)),
        (("host", "user"), ("host", "user")),
        (
            (
                "host",
                "user1",
            ),
            ("host", "user2"),
        ),
        (("host", "user", "pass"), ("host", "user", "pass")),
        (("host", "user", "pass1"), ("host", "user", "pass2")),
        (("host", "user", "pass", "exch"), ("host", "user", "pass", "exch")),
        (("host", "user", "pass", "exch1"), ("host", "user", "pass", "exch2")),
        (
            ("host", "user", "pass", "exch", "vhost"),
            ("host", "user", "pass", "exch", "vhost"),
        ),
        (
            ("host", "user", "pass", "exch", "vhost1"),
            ("host", "user", "pass", "exch", "vhost2"),
        ),
        (
            ("host", "user", "pass", "exch", "vhost", 4000),
            ("host", "user", "pass", "exch", "vhost", 4000),
        ),
        (
            ("host", "user", "pass", "exch", "vhost", 4001),
            ("host", "user", "pass", "exch", "vhost", 4002),
        ),
    ],
)
def test_equality(con1, con2):
    ex1 = AmqpConnection(*con1)
    ex2 = AmqpConnection(*con2)
    assert ex1 != con1
    assert ex2 != con2
    if con1 == con2:
        assert ex1 == ex2
        assert ex1.identifier == ex2.identifier
    elif con1[:2] == con2[:2] and con1[3:] == con2[3:]:
        # password doesn't get checked for equality!
        assert ex1 == ex2
        assert ex1.identifier == ex2.identifier
    else:
        assert ex1 != ex2
        assert ex1.identifier != ex2.identifier


def test_hashable():
    assert isinstance(AmqpConnection, Hashable)
    ex1 = AmqpConnection("test")
    ex2 = AmqpConnection("test")
    ex3 = AmqpConnection("nottest")
    assert hash(ex1) == hash(ex2)
    assert hash(ex2) != hash(ex3)


def test_refresh_not_connected():
    ex = AmqpConnection("test")
    with pytest.raises(StateError):
        ex.refresh()


def test_refresh():
    ex = AmqpConnection("test")
    with mock.patch.object(ex, "_AmqpConnection__conn") as mock_con:
        mock_con.connected = False
        ex.refresh()
        mock_con.heartbeat_tick.assert_called_once()


def test_refresh_error():
    ex = AmqpConnection("test")
    with mock.patch.object(ex, "_AmqpConnection__conn") as mock_con, mock.patch.object(ex, "connect") as mock_method:
        mock_con.heartbeat_tick.side_effect = ConnectionForced
        ex.refresh()
        mock_method.assert_called_once()


def test_produce_not_connected():
    ex = AmqpConnection("test")
    with pytest.raises(StateError):
        ex.produce("hello")


def test_produce_nacked():
    ex = AmqpConnection("test")
    with mock.patch.object(ex, "_AmqpConnection__conn") as mock_con:
        ex.connect()
        mock_con.connected = True
        with mock.patch.object(ex, "_AmqpConnection__chan") as mock_chan:
            mock_chan.basic_publish.side_effect = MessageNacked
            assert not ex.produce("t")
