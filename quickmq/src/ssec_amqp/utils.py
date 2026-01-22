"""
ssec_amqp.utils
~~~~~~~~~~~~~~~

Utility functions and definitions that could be helpful for event publishers.
"""

import datetime
import getpass
import inspect
import os
import socket


def _get_calling_py_file() -> str:
    """Gets the path of the first python file that is calling this module."""
    for frame in inspect.stack():
        frame_file = frame.filename
        if os.path.splitext(frame_file)[1] == ".py" and frame_file != __file__:
            return os.path.abspath(frame_file)
    return os.path.abspath(os.curdir)


# Name of server publishing from
SERVER_NAME = socket.gethostname()

# Name of the user publishing messages
try:
    CURRENT_USER = getpass.getuser()
except KeyError:
    CURRENT_USER = ""

# Script that publishes the messages
INJECTOR_SCRIPT = f"{CURRENT_USER}@{SERVER_NAME}:{_get_calling_py_file()}"

# Payload datetime format
PAYLOAD_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def format_datetime(dt: datetime.datetime) -> str:
    """Formats a datetime consistent with other AMQP publishers at the SSEC.

    Args:
        dt (datetime): the datetime object to format.

    Returns:
        str: the formatted datetime.
    """
    # use 1 decimal of precision for microseconds
    return dt.strftime(PAYLOAD_DATETIME_FORMAT)[:-5]
