"""
sat_latency.interface
~~~~~~~~~~~~~~~~~~~~~

Programatic/command line interface to interact with satellite data.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from datetime import datetime as dt
from datetime import time
from datetime import timedelta as td
from datetime import timezone as tz
from typing import Any

import polars as pl
import pyarrow.compute as pc

from sat_latency._utils import clean_exit
from sat_latency.env import LATENCY_DIR
from sat_latency.pipeline import STORAGE_SCHEMA, read_satellite_data


def date_or_time_type(strtime: str) -> dt:
    """Parses a string passed into the program into a datetime. The string
    can be an ISO formatted date, time, or datetime, or 'now' for the current
    time.

    Args:
        strtime (str): The time string.

    Raises:
        ValueError: A value cannot be parsed from the string.

    Returns:
        dt: The parsed datetime object.
    """
    if "T" in strtime:
        return dt.fromisoformat(strtime).replace(tzinfo=tz.utc)
    elif ":" in strtime:
        return dt.combine(
            date.today(), time.fromisoformat(strtime), tzinfo=tz.utc
        )
    elif "-" in strtime:
        return dt.combine(date.fromisoformat(strtime), time.min, tzinfo=tz.utc)
    elif strtime.lower() == "now":
        return dt.now(tz.utc)
    raise ValueError(
        "Cannot parse datetime %s, please follow the ISO format," % strtime
    )


def parse_interface_args() -> argparse.Namespace:
    """Parses the arguments used for the interface.

    Returns:
        Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="sat_latency_interface",
        epilog="All times should be in UTC!",
        allow_abbrev=False,
        usage="%(prog)s [options] [time options] [qualifier options] [output options]",
    )

    parser.add_argument(
        "-d",
        "--database-dir",
        dest="db_dest",
        type=str,
        default=LATENCY_DIR,
        help="Path to top level directory that contains latency db information.",
    )

    time_group = parser.add_argument_group(
        "Time Options",
        "Specify what datapoints to output based on their time.",
    )

    time_group.add_argument(
        "--from",
        dest="dtfrom",
        type=date_or_time_type,
        default=date.today().isoformat(),
        help=(
            "The beginning of the time window to get data from. Must be "
            "formatted as an ISO complient datetime, date, or time, or 'now'. "
            "If just time, date is assumed to be today. If just date, time "
            "is assumed to be 00:00:00. Default is date.today()."
        ),
    )
    time_group.add_argument(
        "--until",
        dest="dtuntil",
        type=date_or_time_type,
        default=dt.now(tz=tz.utc),
        help=(
            "The end of the time window to get data from. Same format as "
            "--from. Default is datetime.now()"
        ),
    )
    time_group.add_argument(
        "--datematch",
        dest="dmatch",
        default=None,
        help=(
            "Only output datapoints with matching datetimes. Can use "
            "sqlite's wildcards '%%' and '_' to match arbitrary data. e.g. "
            "2024-08-31T__:15:%% matches all 00:15, 01:15, 02:15, etc for "
            "2024-08-31."
        ),
    )

    parser.add_argument(
        "--topic",
        dest="topic",
        default=None,
        help=(
            "Specify latencies to get by AMQP topic. Will output latencies "
            "all latencies that contain matching regex."
        ),
    )

    parser.add_argument(
        "--satellite-id",
        dest="sat_id",
        default=None,
        nargs="+",
        help="Specify latencies to get by satellite id.",
    )
    parser.add_argument(
        "--band",
        dest="band",
        default=None,
        nargs="+",
        help="Specify latencies to get by satellite band.",
    )
    parser.add_argument(
        "--coverage",
        dest="coverage",
        default=None,
        nargs="+",
        help="Specify latencies to get by satellite coverage.",
    )
    parser.add_argument(
        "--section",
        dest="section",
        default=None,
        nargs="+",
        help="Specify latencies to get by satellite section.",
    )
    parser.add_argument(
        "--ingest-source",
        dest="source",
        default=None,
        nargs="+",
        help="Specify latencies to get by data source.",
    )
    parser.add_argument(
        "--instrument",
        dest="instrument",
        default=None,
        nargs="+",
        help="Specify latencies to get by satellite instrument.",
    )
    out_group = parser.add_argument_group(
        "Output Options", "Specify how to output the datapoints."
    )
    out_group.add_argument(
        "--columns",
        dest="out_cols",
        nargs="+",
        default=[
            "satellite_ID",
            "band",
            "coverage",
            "ingest_source",
            "instrument",
            "section",
            "start_time",
            "latency",
        ],
        help=(
            "Specify which columns to print in output. Options are: "
            "{opts} and latency. Default is %(default)s".format(
                opts=", ".join(STORAGE_SCHEMA.names)
            )
        ),
    )
    out_group.add_argument(
        "--output-type",
        dest="output",
        choices=["pretty_json", "json", "json_lines", "pretty_json_lines"],
        default="pretty_json_lines",
        help="Choose output format.",
    )

    args = parser.parse_args()

    # Validate arguments
    if not os.path.isdir(args.db_dest):
        parser.error("--database-dir needs to be an existing directory!")

    if (args.dtuntil - args.dtfrom) < td(0):
        parser.error(
            "Negative time range specified, from %s until %s"
            % (args.dtfrom, args.dtuntil),
        )

    return args


def json_serialize(non_serializable: Any):
    """serialize dates for json."""
    if isinstance(non_serializable, (dt, date, time)):
        return non_serializable.isoformat()
    raise ValueError


def satellite_data_from_filters(
    base_dir: str,
    start_date: dt,
    end_date: dt | None = None,
    satellite_ids: list[str] | None = None,
    coverages: list[str] | None = None,
    bands: list[str] | None = None,
    sections: list[str] | None = None,
    sources: list[str] | None = None,
    instruments: list[str] | None = None,
    topic_regex: str | None = None,
    date_like: str | None = None,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    q_filter = (pc.field("start_time") >= pc.scalar(start_date)) & (
        pc.field("start_time") <= pc.scalar(end_date or dt.now(tz.utc))
    )

    if satellite_ids is not None:
        q_filter = q_filter & (pc.field("satellite_ID").isin(satellite_ids))
    if coverages is not None:
        q_filter = q_filter & (pc.field("coverage").isin(coverages))
    if bands is not None:
        q_filter = q_filter & (pc.field("band").isin(bands))
    if sections is not None:
        q_filter = q_filter & (pc.field("section").isin(sections))
    if sources is not None:
        q_filter = q_filter & (pc.field("ingest_source").isin(sources))
    if instruments is not None:
        q_filter = q_filter & (pc.field("instrument").isin(instruments))

    if date_like is not None:
        q_filter = q_filter & (
            pc.match_like(pc.strftime(pc.field("start_time")), date_like)
        )

    if topic_regex is not None:
        q_filter = q_filter & (
            pc.match_substring_regex(pc.field("topic"), topic_regex)
        )

    df = read_satellite_data(
        base_dir,
        start_date,
        end_date,
        q_filter,
    )

    if columns is not None:
        df = df.select(columns)

    return df


@clean_exit
def main():
    args = parse_interface_args()

    db_dest = args.db_dest or LATENCY_DIR

    df = satellite_data_from_filters(
        base_dir=db_dest,
        start_date=args.dtfrom,
        end_date=args.dtuntil,
        satellite_ids=args.sat_id,
        coverages=args.coverage,
        bands=args.band,
        sections=args.section,
        sources=args.source,
        instruments=args.instrument,
        topic_regex=args.topic,
        date_like=args.dmatch,
        columns=args.out_cols,
    )

    if df.is_empty():
        print(
            "No data found in %s with filters: %s"
            % (db_dest, ", ".join(sys.argv[1:])),
            file=sys.stderr,
        )
        return
    indent = 4 if "pretty" in args.output else None
    lines = True if "lines" in args.output else False

    d_list = df.to_dicts()

    if not lines:
        print(json.dumps(d_list, indent=indent, default=json_serialize))
    else:
        for d in d_list:
            print(json.dumps(d, indent=indent, default=json_serialize))


if __name__ == "__main__":
    sys.exit(main())
