"""
sat_latency.pipeline.load
~~~~~~~~~~~~~~~~~~~

Abstract how the satellite data is saved to disk from the pipeline
and interface.
"""

from __future__ import annotations

import datetime as dt
import functools
import os
from collections import OrderedDict
from typing import Generator

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from sat_latency._utils import daterange
from sat_latency.pipeline.transform import STORAGE_SCHEMA


@functools.lru_cache(maxsize=10)
def _path_stub_from_date(date: dt.date) -> str:
    """Gets the path stub to a latency file based on its date. The stub
    doesn't include the top level directory specified by LATENCY_DIR
    in env.py.

    Args:
        date (date): The date that the file holds latency data for.

    Returns:
        str: The path stub for the file that contains data at the
        specified date.
    """
    return date.strftime("%Y/%Y_%m/%Y_%m_%d_latencies.arrows")


def _yield_batches(*files: str) -> Generator[pa.RecordBatch, None, None]:
    """Given a set of Apache Arrow files, load RecordBatches from
    all of them

    Yields:
        Generator[pa.RecordBatch, None, None]: All record batches in files.
    """
    for file in files:
        with pa.memory_map(file, "r") as source:
            with pa.ipc.open_stream(source) as reader:
                while True:
                    try:
                        yield reader.read_next_batch()
                    except StopIteration:
                        break
                    except OSError:
                        # This is from a corrupt batch
                        continue


def _files_from_date_range(
    base_dir: str, date_start: dt.datetime, date_end: dt.datetime | None = None
) -> Generator[str, None, None]:
    """Get partition files for a range of dates.

    Args:
        base_dir (str): The base dir to look for partition files in.
        date_start (dt.date): Start date.
        date_end (dt.date | None, optional): End date. Defaults to today.

    Yields:
        Generator[str, None, None]: File path.
    """
    for date in daterange(date_start, date_end):
        path = os.path.join(base_dir, _path_stub_from_date(date))
        if os.path.isfile(path):
            yield path


# Reading data from disk
def read_satellite_data(
    base_dir: str,
    date_from: dt.datetime,
    date_until: dt.datetime | None = None,
    arrow_filter: pc.Expression | None = None,
) -> pl.DataFrame:
    """Loads the latency data into a polars dataframe object.

    Args:
        base_dir (str): The directory that contains the latency data
        arrow files, split by date.
        date_from (date | dt): Satellite date of start of latency data to get.
        date_until (date | dt | None, optional): Satellite date of end of
        latency data to get. Defaults to today.
        filter (pc.Expression | None, optional): pyarrow Expression to filter
        the rows with. Defaults to None.

    Returns:
        pl.DataFrame: A dataframe with the latency data.
    """
    tbl = pa.Table.from_batches(
        _yield_batches(*_files_from_date_range(base_dir, date_from, date_until))
    )
    if arrow_filter is not None:
        tbl = tbl.filter(arrow_filter)
    tbl = tbl.cast(STORAGE_SCHEMA)

    # calculate the latency
    tbl = tbl.append_column(
        "latency",
        pc.divide(
            pc.cast(
                pc.milliseconds_between(
                    tbl.column("start_time"),
                    tbl.column("reception_time"),
                ),
                target_type=pa.float64(),
            ),
            1000.0,
        ),
    )
    return pl.from_arrow(tbl)  # type: ignore


class BatchWriter:
    def __init__(
        self, base_dir: str, schema: pa.Schema | None = None, pool_size=5
    ) -> None:
        self._base_dir = base_dir
        if not os.path.isdir(base_dir):
            os.mkdir(base_dir)
        self._writer_pool: OrderedDict[
            dt.date, tuple[pa.OSFile, pa.RecordBatchStreamWriter]
        ] = OrderedDict()
        self._max_size = pool_size
        self._schema = schema

    @property
    def base_dir(self) -> str:
        return self._base_dir

    @property
    def schema(self) -> pa.Schema:
        return self._schema

    def _get_writer(self, date: dt.date) -> pa.RecordBatchStreamWriter:
        writer = self._writer_pool.get(date)
        if writer is not None:
            self._writer_pool.move_to_end(date)
            return writer[1]
        path = os.path.join(self._base_dir, _path_stub_from_date(date))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        file = pa.OSFile(path, mode="ab")
        new_writer = pa.ipc.new_stream(file, self._schema)
        if len(self._writer_pool) >= self._max_size:
            old_date = next(iter(self._writer_pool))
            self._close_date(old_date)
            self._writer_pool.popitem(False)
        self._writer_pool[date] = (file, new_writer)
        return new_writer

    def write_batch(self, batch, date: dt.date) -> None:
        writer = self._get_writer(date)
        writer.write_batch(batch)

    def _close_date(self, date: dt.date) -> None:
        value = self._writer_pool.get(date)
        if value is None:
            return
        file, writer = value
        writer.close()
        file.flush()
        file.close()

    def close(self) -> None:
        for date in self._writer_pool:
            self._close_date(date)

    def __enter__(self) -> BatchWriter:
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.close()
