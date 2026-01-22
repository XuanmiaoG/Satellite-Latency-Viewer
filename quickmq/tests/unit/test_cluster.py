from typing import Iterable

import pytest
from ssec_amqp.amqp import ClusteredConnection, ConnectionProtocol, StateError

from tests.unit.conftest import FakeConnection


def test_primary(fake_conn_factory):
    """Primary should be None until connect is called."""
    f = fake_conn_factory()
    c = ClusteredConnection([f])
    assert c.primary is None
    c.connect()
    assert c.primary == f


def test_no_conns_err():
    """ClusteredConnection should raise an error if it is initialized
    without any connections."""
    with pytest.raises(ValueError):  # noqa: PT011
        ClusteredConnection([])


@pytest.mark.parametrize(
    ("con_list1", "con_list2"),
    [
        ([FakeConnection(1)], [FakeConnection(1)]),
        (
            [FakeConnection(1), FakeConnection(2)],
            [FakeConnection(1), FakeConnection(2)],
        ),
        (
            [FakeConnection(1), FakeConnection(2)],
            [FakeConnection(2), FakeConnection(1)],
        ),
    ],
)
def test_equality(con_list1: Iterable[ConnectionProtocol], con_list2: Iterable[ConnectionProtocol]):
    con1 = ClusteredConnection(con_list1)
    con2 = ClusteredConnection(con_list2)
    assert con1 == con2
    assert hash(con1) == hash(con2)
    assert con1.identifier == con2.identifier


@pytest.mark.parametrize(
    ("con_list1", "con_list2"),
    [
        ([FakeConnection(1)], [FakeConnection(2)]),
        (
            [FakeConnection(1)],
            [FakeConnection(1), FakeConnection(2)],
        ),
        (
            [FakeConnection(1), FakeConnection(2)],
            [FakeConnection(1), FakeConnection(3)],
        ),
    ],
)
def test_inequality(con_list1: Iterable[ConnectionProtocol], con_list2: Iterable[ConnectionProtocol]):
    con1 = ClusteredConnection(con_list1)
    con2 = ClusteredConnection(con_list2)
    assert con1 != con2
    assert hash(con1) != hash(con2)
    assert con1.identifier != con2.identifier


def test_refresh_err(fake_conn_factory):
    """If ClusteredExchange hasn't been connected, refresh should error."""
    f = fake_conn_factory()
    c = ClusteredConnection([f])
    with pytest.raises(StateError):
        c.refresh()


# cc = cluster_candidates


def test_cc_new(fake_conn_factory):
    """Make sure cluster_candidate sets the primary if it hasn't been set yet."""
    f = fake_conn_factory()
    c = ClusteredConnection([f])
    for candidate in c._cluster_candidates():
        if candidate == f:
            break
    assert c.primary == f


def test_cc_exhaust(fake_conn_factory):
    """If no candidates are used, the primary should be None"""
    f = fake_conn_factory()
    c = ClusteredConnection([f])
    # Set the primary first
    for candidate in c._cluster_candidates():
        if candidate == f:
            break
    assert c.primary == f
    # Then unset it
    for _ in c._cluster_candidates():
        pass
    assert c.primary is None


def test_cc_primary_discon(fake_conn_factory):
    """Primary should be updated when an established primary is rejected."""
    f1 = fake_conn_factory()
    f2 = fake_conn_factory()
    c = ClusteredConnection([f1, f2])
    # Set f1 to be the primary
    for candidate in c._cluster_candidates():
        if candidate == f1:
            break
    assert c.primary == f1

    # Connection gets disconnected (will get rejected)
    f1.can_connect = False

    for candidate in c._cluster_candidates():
        if candidate == f2:
            break
        msg = f"Unexpected candidate {candidate:r}!"
        raise AssertionError(msg)
    assert c.primary == f2
