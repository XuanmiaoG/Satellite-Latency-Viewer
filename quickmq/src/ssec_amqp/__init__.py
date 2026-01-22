"""ssec_amqp

Namespace package for QuickMQ.

Quickly and easily publish messages to multiple and/or clustered RabbitMQ brokers.
"""

from ssec_amqp.amqp import AmqpConnection, AMQPConnectionError, AmqpExchange, ClusteredConnection, StateError
from ssec_amqp.client import AmqpClient, ConnectionStatus, DeliveryStatus

__all__ = [
    "AmqpConnection",
    "AmqpClient",
    "AmqpExchange",
    "ClusteredConnection",
    "DeliveryStatus",
    "ConnectionStatus",
    "AMQPConnectionError",
    "StateError",
]
