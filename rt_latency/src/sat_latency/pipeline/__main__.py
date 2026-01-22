"""
sat_latency.pipeline.__main__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Entrypoint to run the TL parts of the pipeline.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import date

from sat_latency._utils import clean_exit
from sat_latency.env import BATCH_MAX_DELAY, BATCH_MAX_SIZE, LATENCY_DIR
from sat_latency.pipeline.extract import read_input
from sat_latency.pipeline.load import BatchWriter
from sat_latency.pipeline.transform import (
    STORAGE_SCHEMA,
    storage_batch_from_list,
)

PARTITION_KEY = "start_time"


def parse_pipeline_args() -> argparse.Namespace:
    """Parses the arguments used for the pipeline process.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="sat_latency.pipeline",
        usage=(
            "Do not call directly! Use sat_latency_pipeline to run "
            "automatically!"
        ),
        description="Runs the TL part of the pipeline.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="count",
        default=0,
        help="verbosity of pipeline.",
    )

    return parser.parse_args()


def init() -> None:
    """Initialize the transformer for execution.
    Parse argumets, set up logging.
    """
    args = parse_pipeline_args()

    levels = [
        logging.FATAL,
        logging.CRITICAL,
        logging.ERROR,
        logging.WARNING,
        logging.INFO,
        logging.DEBUG,
    ]
    level = levels[min(args.verbose, 5)]
    logging.basicConfig(
        format="[%(levelname)s:%(asctime)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=level,
    )


def run():
    """Run the pipeline."""
    batch_size = 0
    batch_time = time.time()

    data: dict[date, list[dict[str, bytes]]] = defaultdict(list)
    with BatchWriter(LATENCY_DIR, STORAGE_SCHEMA) as writer:
        for point in read_input(sys.stdin.buffer):
            try:
                partition = date.fromisoformat(
                    point[PARTITION_KEY].decode("utf-8")[:10]
                )
            except ValueError:
                logging.warning(
                    "Couldn't decode start date for %s" % point["topic"]
                )
                continue
            logging.debug("Got %s" % point["topic"])
            data[partition].append(point)
            batch_size += 1

            if (
                batch_size < BATCH_MAX_SIZE
                and time.time() - batch_time < BATCH_MAX_DELAY
            ):
                continue
            for partition, data_list in data.items():
                logging.info("Writing batches for %s" % str(partition))
                writer.write_batch(
                    storage_batch_from_list(data_list), partition
                )
            data.clear()
            batch_size = 0
            batch_time = time.time()


if __name__ == "__main__":
    init()
    sys.exit(clean_exit(run)())
