"""
sat_latency
~~~~~~~~~~~

Package for ETL pipeline that ingests satellite data from RabbitMQ servers,
calculates latencies for each datapoint, and writes them to disk. Also contains
an interface to interact with the written data.
"""

__version__ = "2.0.1"
__author__ = "Max Drexler"
__email__ = "mndrexler@wisc.edu"

from sat_latency.interface import satellite_data_from_filters
from sat_latency.pipeline import (
    INGEST_FIELDS,
    STORAGE_SCHEMA,
    read_satellite_data,
)

__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "read_satellite_data",
    "satellite_data_from_filters",
    "STORAGE_SCHEMA",
    "INGEST_FIELDS",
]
