"""
Builds the package.

The use of setup.py is not reccomended for new projects, but it was the only way
I could find to include bash scripts in the installation.

Setup script lightly inspired by Pytorch's setup.py
(https://github.com/pytorch/pytorch/blob/main/setup.py).

PACKAGE REQUIREMENTS:
- POSIX System
- Python >= 3.8
"""

import os
import sys

from setuptools import find_namespace_packages, setup

# Check requirements
if os.name != "posix":
    print("This package is only suppported on POSIX machines.")
    sys.exit(-1)

python_min_version = (3, 8, 0)
python_min_version_str = ".".join(map(str, python_min_version))

if sys.version_info < python_min_version:
    print(
        f"You are using python {sys.version_info}. "
        f"Python >={python_min_version} is required"
    )
    sys.exit(-1)


# Constant known variables used throughout this file
cwd = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(cwd, "src", "sat_latency")
package_name = "sds-sat-latency"
install_requires = [
    "typing-extensions >=4.0, <5",
    "pika >= 1.1.0",  # required by amqpfind
    "pyarrow >= 16.0.0",
    "polars >= 0.20",
]

# Generate setup metadata

with open(os.path.join(src_path, "__init__.py"), "r", encoding="utf-8") as f:
    try:
        meta = {
            k.strip(): v.strip().strip('"')
            for k, v in [
                line.split("=", maxsplit=1)
                for line in f.readlines()
                if line.startswith("__")
            ]
        }
    except OSError:
        print("Error getting package metadata from __init__.py")
        sys.exit(-1)


with open(os.path.join(cwd, "README.md"), "r", encoding="utf-8") as f:
    long_description = f.read()


setup(
    name=package_name,
    description="Calculate AMQP message latencies. Made for the use at the SSEC.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=meta["__author__"],
    author_email=meta["__email__"],
    license="GPLv3",
    license_files=[
        "LICENSE",
    ],
    platforms=["POSIX"],
    url="https://gitlab.ssec.wisc.edu/mdrexler/mk_latency",
    download_url="https://gitlab.ssec.wisc.edu/mdrexler/mk_latency/-/packages",
    version=meta["__version__"],
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=install_requires,
    python_requires=f">={python_min_version_str}",
    scripts=[
        "bin/sat_latency_pipeline",
    ],
    entry_points={
        "console_scripts": [
            "sat_latency_interface = sat_latency.interface:main"
        ]
    },
    zip_safe=True,
)
