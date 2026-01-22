"""unit.test_uri

Make sure ssec_amqp.amqp parses amqp URIs according to
https://www.rabbitmq.com/docs/uri-spec
"""

from typing import List, Tuple

import pytest
from ssec_amqp.amqp import (
    DEFAULT_CONN_PARAMS,
    DEFAULT_HOST,
    DEFAULT_PASS,
    DEFAULT_PORT,
    DEFAULT_USER,
    DEFAULT_VHOST,
    AmqpConnectionParams,
    URIFormatError,
    params_from_uri,
)

# Mapping of AMQP URIs to expected connection parameters
# Examples taken from https://www.rabbitmq.com/docs/uri-spec#appendix-a-examples
PARSE_EXAMPLES: List[Tuple[str, AmqpConnectionParams]] = [
    (
        "amqp://user:pass@host:10000/vhost",
        {"host": "host", "user": "user", "password": "pass", "port": 10000, "vhost": "vhost"},
    ),
    (
        "amqp://user:passw%23rd@host:10000/vhost",
        {"host": "host", "user": "user", "password": "passw#rd", "port": 10000, "vhost": "vhost"},
    ),
    (
        "amqp://user%61:%61pass@ho%61st:10000/v%2fhost",
        {"host": "hoast", "user": "usera", "password": "apass", "port": 10000, "vhost": "v/host"},
    ),
    ("amqp://", DEFAULT_CONN_PARAMS),
    (
        "amqp://:@/",
        {
            "host": DEFAULT_HOST,
            "port": DEFAULT_PORT,
            "user": "",
            "password": "",
            "vhost": "",
        },
    ),
    (
        "amqp://user@",
        {
            "host": DEFAULT_HOST,
            "port": DEFAULT_PORT,
            "user": "user",
            "password": DEFAULT_PASS,
            "vhost": DEFAULT_VHOST,
        },
    ),
    (
        "amqp://user:pass@",
        {
            "host": DEFAULT_HOST,
            "port": DEFAULT_PORT,
            "user": "user",
            "password": "pass",
            "vhost": DEFAULT_VHOST,
        },
    ),
    (
        "amqp://host",
        {
            "host": "host",
            "port": DEFAULT_PORT,
            "user": DEFAULT_USER,
            "password": DEFAULT_PASS,
            "vhost": DEFAULT_VHOST,
        },
    ),
    (
        "amqp://:10000",
        {
            "host": DEFAULT_HOST,
            "port": 10000,
            "user": DEFAULT_USER,
            "password": DEFAULT_PASS,
            "vhost": DEFAULT_VHOST,
        },
    ),
    (
        "amqp:///vhost",
        {
            "host": DEFAULT_HOST,
            "port": DEFAULT_PORT,
            "user": DEFAULT_USER,
            "password": DEFAULT_PASS,
            "vhost": "vhost",
        },
    ),
    (
        "amqp://host/",
        {
            "host": "host",
            "port": DEFAULT_PORT,
            "user": DEFAULT_USER,
            "password": DEFAULT_PASS,
            "vhost": "",
        },
    ),
    (
        "amqp://127.0.0.1:1200/",
        {
            "host": "127.0.0.1",
            "port": 1200,
            "user": DEFAULT_USER,
            "password": DEFAULT_PASS,
            "vhost": "",
        },
    ),
    (
        "amqp://[::1]",
        {
            "host": "::1",
            "port": DEFAULT_PORT,
            "user": DEFAULT_USER,
            "password": DEFAULT_PASS,
            "vhost": DEFAULT_VHOST,
        },
    ),
    (
        "amqp:///",
        {
            "host": DEFAULT_HOST,
            "port": DEFAULT_PORT,
            "user": DEFAULT_USER,
            "password": DEFAULT_PASS,
            "vhost": "",
        },
    ),
]


@pytest.mark.parametrize(("uri", "expected_params"), PARSE_EXAMPLES)
def test_parse(uri: str, expected_params: AmqpConnectionParams):
    """Make sure the parameters extracted from the uri are expected."""
    assert params_from_uri(uri) == expected_params


def test_unsupported_parse():
    """Make sure a descriptive error is raised when attempting to parse the amqps
    URI spec."""
    with pytest.raises(NotImplementedError):
        params_from_uri("amqps://")


@pytest.mark.parametrize(
    "uri",
    [
        None,
        "asdfasdfasdfa",
        "amqp:/user:name@host",
        "amqp://user@host:asdfasdf",
    ],
)
def test_parse_error(uri: str):
    """Make sure ill-formatted URIs raise ValueError"""

    with pytest.raises(URIFormatError):
        params_from_uri(uri)
