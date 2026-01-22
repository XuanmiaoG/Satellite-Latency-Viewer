"""
amqpfind.__main__
~~~~~~~~~~~~~~~~~

Simple wrapper for the amqpfind code, allows for discovery when using python -m
"""

import sys

from amqpfind.amqpfind import main

if __name__ == "__main__":
    sys.exit(main())
