import argparse
import logging
import sys
import time
from collections import deque
from functools import lru_cache
from typing import Tuple

import ssec_amqp.api as mq
from rich.live import Live
from rich.table import Table
from ssec_amqp import ConnectionStatus

CON_PARAMS = [
    {"host": "localhost", "port": 32777},
    {"host": "localhost", "port": 32771},
]

TEST_LOGGER = logging.getLogger("ssec_mq_test")


@lru_cache
def test_payload():
    return {
        "message_type": "product",
        "status": "complete",
        "medium": "adde",
        "server_ip": "satbuf1.ssec.wisc.edu",
        "path": "/data/goes/grb/goes16/2024/2024_06_27_179/abi/L2/PDA/DSIM1/OR_ABI-L2-DSIM1-M6_G16_s20241791538254_e20241791538311_c20241791539150.nc",
        "dataFile": "",
        "event": "end",
        "__injector_script__": "oper@satbuf1.ssec.wisc.edu:/home/oper/goesr/bin/event_processing_other.py",
        "server_type": "realtime",
        "start_time": "2024-06-27 15:38:25.4",
        "end_time": "2024-06-27 15:38:31.1",
        "create_time": "2024-06-27 15:39:15.0",
        "coverage": "Mesoscale-1",
        "instrument": "ABI",
        "mode": "6",
        "satellite_location": "GOES-East",
        "satellite_ID": "G16",
        "satellite_family": "GOES",
        "title": "ABI L2 Derived Stability Indices",
        "band": "Not Available",
        "signal_type": "grb",
        "adde_dataset": "EASTL2A/DSIM1-PDA",
        "data_type": "DSIM1",
        "exposure": "short_flare",
        "processing_system": "PDA",
        "DQF_percent_good_pixel": "1.0000000",
        "publish_status": "OK",
        "imageDesc": "Level-2 Derived Product ABI",
        "__topic__": "geo.goes.g16.abi.adde.realtime.ncdf.product.complete",
        "__reception_time__": "2024-06-27T15:39:21.643916",
        "__reception_host__": "mq1.ssec.wisc.edu",
        "message_type": "product",
        "status": "complete",
        "medium": "adde",
        "server_ip": "satbuf1.ssec.wisc.edu",
        "path": "/data/goes/grb/goes16/2024/2024_06_27_179/abi/L2/PDA/DSIM1/OR_ABI-L2-DSIM1-M6_G16_s20241791538254_e20241791538311_c20241791539150.nc",
        "dataFile": "",
        "event": "end",
        "__injector_script__": "oper@satbuf1.ssec.wisc.edu:/home/oper/goesr/bin/event_processing_other.py",
        "server_type": "realtime",
        "start_time": "2024-06-27 15:38:25.4",
        "end_time": "2024-06-27 15:38:31.1",
        "create_time": "2024-06-27 15:39:15.0",
        "coverage": "Mesoscale-1",
        "instrument": "ABI",
        "mode": "6",
        "satellite_location": "GOES-East",
        "satellite_ID": "G16",
        "satellite_family": "GOES",
        "title": "ABI L2 Derived Stability Indices",
        "band": "Not Available",
        "signal_type": "grb",
        "adde_dataset": "EASTL2A/DSIM1-PDA",
        "data_type": "DSIM1",
        "exposure": "short_flare",
        "processing_system": "PDA",
        "DQF_percent_good_pixel": "1.0000000",
        "publish_status": "OK",
        "imageDesc": "Level-2 Derived Product ABI",
        "__topic__": "geo.goes.g16.abi.adde.realtime.ncdf.product.complete",
        "__reception_time__": "2024-06-27T15:39:21.643916",
        "__reception_host__": "mq1.ssec.wisc.edu",
    }


def init() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--verbose",
        type=int,
        default=0,
        help="Verbosity number",
    )

    parser.add_argument("--all-logs", action="store_true", help="Show all logs or not")

    args = parser.parse_args()

    def config_logger(logger: logging.Logger, level):
        logger.handlers.clear()
        handler = logging.FileHandler(
            "integration_test.log", mode="w", encoding="utf-8"
        )
        handler.setFormatter(
            logging.Formatter("[%(asctime)s-%(levelname)s]-%(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(level)

    log_levels = [
        logging.CRITICAL,
        logging.ERROR,
        logging.WARNING,
        logging.INFO,
        logging.DEBUG,
    ]

    level = log_levels[min(args.verbose, len(log_levels) - 1)]
    config_logger(TEST_LOGGER, level)

    logging.getLogger("amqp").propagate = False
    if not args.all_logs:
        logging.getLogger("ssec_amqp").propagate = False
    else:
        config_logger(logging.getLogger("ssec_amqp"), level)


def create_table(*stats: Tuple[str, float, float]):
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Source", style="dim", width=50)
    table.add_column("Successes / second", justify="right", width=50)
    table.add_column("Fails / second", justify="right", width=50)

    for source, success, fail in stats:
        table.add_row(source, "%.2f" % success, "%.2f" % fail)
    return table


def run_test():
    for params in CON_PARAMS:
        mq.connect(**params)
    for server, status in mq.status().items():
        if status == ConnectionStatus.RECONNECTING:
            mq.disconnect()
            raise ConnectionError("Couldn't initially connect to %s" % server)

    recent_window = 10

    total_succs = 0
    total_fails = 0
    stats = {
        con: {
            "fails": deque(),
            "succs": deque(),
        }
        for con in mq.status()
    }
    start_time = time.time()

    with Live(create_table(), refresh_per_second=8, screen=True) as live:
        while True:
            publish_status = mq.publish(test_payload())
            publish_time = time.time()

            for con, status in publish_status.items():
                succ_times = stats[con]["succs"]
                fail_times = stats[con]["fails"]

                # Stat aggregation
                if status != "DELIVERED":
                    total_fails += 1
                    fail_times.append(publish_time)
                else:
                    succ_times.append(publish_time)
                    total_succs += 1

                # old stat removals
                while succ_times and succ_times[0] <= publish_time - recent_window:
                    succ_times.popleft()
                while fail_times and fail_times[0] <= publish_time - recent_window:
                    fail_times.popleft()

            total_runtime = publish_time - start_time
            # Update table
            tbl_stats = []
            for con, con_stats in stats.items():
                tbl_stats.append(
                    (
                        con,
                        len(con_stats["succs"]) / recent_window,
                        len(con_stats["fails"]) / recent_window,
                    )
                )
            tbl_stats.append(
                ("Total", total_succs / total_runtime, total_fails / total_runtime)
            )
            live.update(create_table(*tbl_stats))
            tbl_stats.clear()


if __name__ == "__main__":
    try:
        init()
        run_test()
    except KeyboardInterrupt:
        pass
    sys.exit()
