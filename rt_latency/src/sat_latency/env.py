"""
sat_latency.env
~~~~~~~~~~~~~~~

Variables that can optionally be retrieved from the environment.
"""

import os

# The directory that holds the latency data files.
LATENCY_DIR = os.getenv("SAT_LATENCY_DIR", os.path.join(os.curdir, "latencies"))

# How many messages to ingest before writing to file.
BATCH_MAX_SIZE = int(os.getenv("SAT_LATENCY_BATCH_SIZE", 1024))

# How long to wait when there are no messages before writing to file.
BATCH_MAX_DELAY = int(os.getenv("SAT_LATENCY_BATCH_DELAY", 120))
