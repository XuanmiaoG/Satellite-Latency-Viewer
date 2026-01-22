"""tests.unit.test_cli

Unit tests for ``ssec_amqp.main``.
"""

import pytest
import ssec_amqp.main as cli


def test_metadata_type():
    """Ensure -m/--metadata option parsed correctly."""

    key, val = cli.key_value_type("1=2")
    assert key == "1"
    assert val == "2"

    with pytest.raises(ValueError):  # noqa: PT011
        cli.key_value_type("")

    with pytest.raises(ValueError):  # noqa: PT011
        cli.key_value_type("key=")

    with pytest.raises(ValueError):  # noqa: PT011
        cli.key_value_type("key")


def test_client_creation():
    """Ensure ``client_from_uris`` connects to both clusters and individual hosts."""

    cl = cli.client_from_uris(["amqp://localhost:8080"], [["amqp://localhost:8", "amqp://localhost:20"]])

    assert len(cl.connections) == 2

    # don't really have a great way to query the types of connections

    cons = list(sorted(cl.connections.keys()))

    assert "Cluster" in cons[0]
    assert "localhost:8" in cons[0]
    assert "localhost:20" in cons[0]
    assert "localhost:8080" in cons[1]


def test_hydrate_topic():
    """Topic is properly formatted."""

    assert cli.hydrate_topic("{test}", {"test": "val"}) == "val"

    assert cli.hydrate_topic("{test}.one.two.three", {"test": "val"}) == "val.one.two.three"

    assert cli.hydrate_topic("{test[two]}.one.two.three", {"test": {"two": "val"}}) == "val.one.two.three"

    assert cli.hydrate_topic("one.two.three", {"test": "val"}) == "one.two.three"
