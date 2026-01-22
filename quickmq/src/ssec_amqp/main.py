#!/usr/bin/env python3
"""ssec_amqp.main

CLI for quickmq.
"""

import argparse
import json
import logging
import sys
from typing import TYPE_CHECKING, List, Mapping, Optional, Tuple

from ssec_amqp import AmqpClient, AmqpConnection, ClusteredConnection, ConnectionStatus
from ssec_amqp.__about__ import __version__
from ssec_amqp.client import DEFAULT_RECONNECT_INTERVAL, DEFAULT_RECONNECT_WINDOW, DeliveryStatus

if TYPE_CHECKING:
    # Not available in py36
    from typing import Any


def key_value_type(val: str) -> Tuple[str, str]:
    """Turn CLI arg key=value into tuple (key, value)."""
    key, eq, val = val.partition("=")
    if not eq or not val:
        raise ValueError
    return (key, val)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="quickmq", allow_abbrev=False)

    parser.add_argument("-V", "--version", action="store_true", help="Show version information and exit.")

    con_parser = parser.add_argument_group("Connection Options")
    con_parser.add_argument(
        "-H",
        "--host",
        metavar="URI",
        dest="hosts",
        default=[],
        nargs="+",
        help="Zero or more amqp URIs to individual servers where to publish data to.",
    )
    con_parser.add_argument(
        "-C",
        "--cluster",
        nargs="+",
        default=[],
        action="append",
        metavar="URI",
        dest="clusters",
        help=(
            "Group of amqp URIs to publish to as cluster(s)."
            "Use multiple times for multiple clusters, e.g. -C uri1 uri2 -C uri3 uri4."
        ),
    )
    con_parser.add_argument("-X", "--exchange", default="", help="The exchange to publish the data to.")
    con_parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=DEFAULT_RECONNECT_INTERVAL,
        help="How many seconds a connection will wait before reconnecting. Default is '%(default)s'.",
    )
    con_parser.add_argument(
        "--reconnect-window",
        type=int,
        default=DEFAULT_RECONNECT_WINDOW,
        help=(
            "How many seconds a connection can attempt reconnecting before exiting."
            " Negative signifies forever. Default is '%(default)s'."
        ),
    )
    con_parser.add_argument(
        "--fast-fail",
        action="store_true",
        help="If no connections can be established initially, exit before attempting to publish.",
    )

    publish_parser = parser.add_argument_group("Publishing Options")
    publish_parser.add_argument(
        "-T",
        "--topic",
        metavar="TOPIC_FMT",
        default="",
        help=(
            "Topic to publish data with. Use a format string to change topics to be specific to the data."
            " E.g. '{satellite_fam}.{satellite_id}.test'"
        ),
    )
    publish_parser.add_argument(
        "-m",
        "--metadata",
        metavar="KV",
        nargs="+",
        type=key_value_type,
        default=[],
        help="Extra key/value pair(s) to add to each payload. E.g. 'server_ip=nsp1.ssec.wisc.edu'",
    )
    publish_parser.add_argument(
        "-D",
        "--data",
        default=None,
        help="JSON string of encoded data to publish. For long-lived publishing, write data to stdin.",
    )

    log_parser = parser.add_argument_group("Log Options")
    verbosity_group = log_parser.add_mutually_exclusive_group(required=False)
    verbosity_group.add_argument(
        "-v",
        "--verbose",
        dest="verbosity",
        metavar="LEVEL",
        type=lambda x: x.upper(),
        choices=["DEBUG", "INFO", "WARNING", "CRITICAL", "ERROR"],
        default="WARNING",
        help="Verbosity of %(prog)s (ERROR, CRITICAL, WARNING, INFO, DEBUG). Default %(default)s.",
    )
    # If -q is specified args.verbosity will be None
    verbosity_group.add_argument(
        "-q", "--quiet", dest="verbosity", action="store_const", const=None, help="Disable all log output to stderr."
    )

    args = parser.parse_args()

    if args.version:
        parser.exit(0, f"{parser.prog}: {__version__}\n")

    if not args.clusters and not args.hosts:
        parser.error("At least one connection required! Use --cluster and/or --host")

    if args.data is not None:
        try:
            args.data = json.loads(args.data)
        except ValueError:
            parser.error("invalid argument -D/--data: not JSON data")

        if not isinstance(args.data, dict):
            parser.error("invalid argument -D/--data: must be a JSON object")

    return args


def client_from_uris(
    uris: List[str],
    clusters: List[List[str]],
    time_between_reconnects: Optional[float] = None,
    max_reconnect_time: Optional[int] = None,
) -> AmqpClient:
    """Creates an AmqpClient with connections to all URIs in ``uris`` and cluster
    connections to all URIs in ``clusters``.
    """
    cl = AmqpClient(
        time_between_reconnects=time_between_reconnects, max_reconnect_time=max_reconnect_time, name="CLI-CLIENT"
    )
    for uri in uris:
        cl.connect(AmqpConnection.from_uri(uri))

    for cluster in clusters:
        cl.connect(ClusteredConnection.from_uris(*cluster))  # type: ignore

    return cl


def hydrate_topic(topic_fmt: str, data: Mapping[str, "Any"]) -> str:
    """Create a topic given a format string and message data.

    If ``topic_fmt`` doesn't contain any '{}' pairs, returns the topic.
    """
    if "{" not in topic_fmt:
        # straight up topic, no formatting
        return topic_fmt
    return topic_fmt.format_map(data)


def main() -> Optional[int]:
    args = parse_args()

    if args.verbosity is not None:
        logging.basicConfig(level=args.verbosity)

    client = client_from_uris(
        args.hosts,
        args.clusters,
        time_between_reconnects=args.reconnect_delay,
        max_reconnect_time=args.reconnect_window,
    )

    if args.fast_fail and all(val == ConnectionStatus.RECONNECTING for val in client.connections.values()):
        print(
            f"Fast fail; couldn't establish any connection(s) to: {', '.join(client.connections.keys())}",
            file=sys.stderr,
        )
        return 1

    if args.data:
        # Quick publish
        data = args.data
        data.update(args.metadata)
        topic = hydrate_topic(args.topic, data)

        pub_status = client.publish(data, route_key=topic, exchange=args.exchange)

        # If nothing got published, return an error
        ret_status = all(st in (DeliveryStatus.REJECTED, DeliveryStatus.DROPPED) for st in pub_status)
        return int(ret_status)

    try:
        for line in iter(sys.stdin.readline, ""):
            data = json.loads(line.strip())
            data.update(args.metadata)
            try:
                topic = hydrate_topic(args.topic, data)
            except KeyError:
                logging.warning("Couldn't create topic with format %s and data %s", args.topic, data)
                continue
            stats = client.publish(data, route_key=topic, exchange=args.exchange)
            logging.info("Published to topic %s with status %s", topic, stats)
    except KeyboardInterrupt:
        logging.info("Got interrupt, ta ta for now")
        return 0
    except BrokenPipeError:
        logging.critical("Stdin broke")
        return 1
    finally:
        client.disconnect()

    return 0


if __name__ == "__main__":
    sys.exit(main())
