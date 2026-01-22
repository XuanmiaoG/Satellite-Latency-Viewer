# rt_latency (sat_latency pipeline wrapper)

Populate and query a real-time database with satellite reception latency using the SDS RabbitMQ infrastructure. This repository is the wrapper and runtime packaging around the latency pipeline.

## Description

The sat_latency package provides two programs: a daemon that continually listens, processes, and updates the inventory using AMQP messages from RabbitMQ servers and interfaces (shell script and Python library) to query the database. This repo focuses on the runtime wrapper and deployment details for the pipeline.

## Installation

### Requirements

- Python >= 3.9

> Note: sat_latency has only been tested on RHEL systems.

Install from the project's [package registry](https://gitlab.ssec.wisc.edu/mdrexler/mk_latency/-/packages/):

```bash
pip install sds-sat-latency --index-url https://gitlab.ssec.wisc.edu/api/v4/projects/2693/packages/pypi/simple
```

> Note: The `sat_latency_pipeline` daemon script must be manually uninstalled, e.g. `pip uninstall sds-sat-latency` won't remove the script.

## Usage

### CLI

After installation, the `sat_latency_interface` bash script will be available to use. For the full documentation on all the options of the script, use `sat_latency_interface --help`.

Some important things to note:

- All times are in UTC.
- The `--database-dir` option specifies the folder that contains the sqlite databases. On qcweb2 this is `/data/sat_latency`.
- The default time range is from midnight to now.
- There's a lot of data in the default database on qcweb2, large queries will take a while!

### Library

The `sds-sat-latency` Python package that drives the CLI also has a Python library that can be used to query data.

Using a Python interpreter that has `sds-sat-latency` installed, the following will query qcweb2's database for the past 5 minutes of GOES-18 latencies.

```python
from datetime import datetime, timezone, timedelta

from sat_latency.interface import satellite_data_from_filters

start_time = datetime.now(timezone.utc) - timedelta(minutes=5)

data = satellite_data_from_filters('/data/sat_latency', start_date=start_time, end_date=datetime.now(timezone.utc), satellite_ids=['G18'])
```

The return type is a [Polars dataframe](https://docs.pola.rs/api/python/stable/reference/index.html).

### Daemon

An example systemd unit file for running the daemon (in this example the `sds-sat-latency` package is installed under the conda environment `/home/oper/.mdrexler_conda`):

```
[Unit]
Description=Calculates satellite latencies using AMQP.
Documentation=https://gitlab.ssec.wisc.edu/mdrexler/mk_latency/-/blob/main/README.md?ref_type=heads

[Service]
Type=simple
Environment="PYTHON=/home/oper/.mdrexler_conda/bin/python3"
Environment="PYTHONPATH=$PYTHONPATH:/home/oper/.mdrexler_conda/lib/python3.9/site-packages"
Environment="SAT_LATENCY_DIR=/data/sat_latency/"
User=oper
ExecStart=/home/oper/.mdrexler_conda/bin/sat_latency_pipeline 4
WorkingDirectory=/home/oper/

[Install]
WantedBy=multi-user.target
```

The `PYTHON` environment variable must point to a Python interpreter that is at *least* version 3.9.

The `SAT_LATENCY_DIR` environment variable tells `sat_latency_pipeline` where to put the latency data.

## Future Work

sat_latency uses [Apache Arrow](https://arrow.apache.org/docs/python/index.html) files to store the AMQP message data. The original thought-process was the Python binding for Arrow (pyarrow) provides memory efficient operations that allows the daemon to process the millions of AMQP messages that are received daily. The downside is Arrow's file format is tricky and prone to corruption and querying isn't very intuitive.

A better solution would be to use [sqlite with a write-ahead-log](https://www.sqlite.org/wal.html). The write-ahead-log provides enough efficiency to allow the daemon to keep up the the number of messages while also providing a straightforward interface for querying the data.

## Contributing

See [CONTRIBUTING](/CONTRIBUTING).

## Authors

Pipeline wrapper, integration, and packaging maintained by Yang Gao.

mk_latency package created by [Max Drexler](mailto:mndrexler@wisc.edu).

amqpfind package created by [Ray Garcia](mailto:rayg@ssec.wisc.edu) and [Kevin Hallock](kevin.hallock@ssec.wisc.edu).

## License

Licensed under GPLv3. See [LICENSE](/LICENSE) for more information.
