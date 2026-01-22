"""
sat_latency.pipeline.ingest
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Defintions for the ingestor. All of the ingest logic is in the
sat_latency_pipeline script installed with the package.
"""

from __future__ import annotations

import sys
from typing import BinaryIO, Generator

import pyarrow as pa

from amqpfind.amqpfind import _missing_

# These fields MUST match the -j option from the sat_latency_pipeline script.
INGEST_FIELDS = [
    "topic",
    "band",
    "coverage",
    "ingest_source",
    "instrument",
    "satellite_ID",
    "section",
    "reception_time",
    "start_time",
    "end_time",
    "create_time",
]

AMQPFIND_MISSING = _missing_().encode("utf-8")
INGEST_SEPARATOR = b"!"


def read_input(
    source: BinaryIO | None = None,
) -> Generator[dict[str, bytes | None], None, None]:
    if source is None:
        source = sys.stdin.buffer
    for line in iter(source.readline, b""):
        yield {
            name: value
            for name, value in zip(INGEST_FIELDS, fields_from_line(line))
        }


def fields_from_line(line: bytes) -> Generator[bytes | None, None, None]:
    """Load the fields of of schema from bytes.

    Args:
        b (bytes): The bytes object for one line from the ingestor.

    Yields:
        Generator[pa.Buffer | None, None, None]: Each field from the schema,
        one at a time.
    """
    with pa.input_stream(pa.py_buffer(line)) as stream:
        p_index = 0
        p_size = 0
        while True:
            byte = stream.read(1)
            if not byte:
                return
            elif byte == INGEST_SEPARATOR or byte == b"\n":
                field = stream.read_at(p_size, p_index)
                if not field or field == AMQPFIND_MISSING:
                    yield None
                else:
                    yield field
                p_index = p_index + p_size + 1
                p_size = 0
            else:
                p_size += 1
