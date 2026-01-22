"""
sat_latency.pipeline.transform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Transform logic for the satellite data pipeline.
"""

import pyarrow as pa
import pyarrow.compute as pc

from sat_latency.pipeline.extract import INGEST_FIELDS

TIME_SCHEMA = pa.schema(
    [
        pa.field("reception_time", pa.timestamp("us", tz="UTC")),
        pa.field("start_time", pa.timestamp("us", tz="UTC")),
        pa.field("end_time", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("create_time", pa.timestamp("us", tz="UTC"), nullable=True),
    ]
)


META_SCHEMA = pa.schema(
    [
        pa.field("topic", pa.string(), nullable=True),
        pa.field("band", pa.string(), nullable=True),
        pa.field("coverage", pa.string(), nullable=True),
        pa.field("ingest_source", pa.string(), nullable=True),
        pa.field("instrument", pa.string(), nullable=True),
        pa.field("satellite_ID", pa.string(), nullable=True),
        pa.field("section", pa.string(), nullable=True),
    ]
)

# The field names MUST be the same as INGEST_FIELDS
# in sat_latency.pipeline.extract
STORAGE_SCHEMA = pa.unify_schemas([META_SCHEMA, TIME_SCHEMA])
_str_schema = pa.schema([pa.field(name, pa.string()) for name in INGEST_FIELDS])


def storage_batch_from_list(data: list[dict[str, bytes]]) -> pa.RecordBatch:
    """Turn a list of dictionaries loaded from the ingestor into a
    record batch that can be written to disk.

    Args:
        data (list[dict[str, bytes]]): input data.

    Returns:
        pa.RecordBatch: record batch.
    """
    batch = pa.RecordBatch.from_pylist(data)
    batch = batch.cast(_str_schema)
    # turn time fields to timestamps
    for i, name in enumerate(batch.column_names):
        if name not in TIME_SCHEMA.names:
            continue

        batch = batch.set_column(
            i,
            name,
            pc.assume_timezone(
                pc.cast(
                    batch.column(i),
                    target_type=pa.timestamp("us"),
                ),
                "UTC",
            ),
        )

    return batch.cast(STORAGE_SCHEMA)
