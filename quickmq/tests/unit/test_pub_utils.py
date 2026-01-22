import os
from datetime import datetime, timezone

# import ssec_amqp


def test_injector_script():
    from ssec_amqp import utils

    """Injector script should return the current filepath."""
    cur_file = os.path.abspath(__file__)
    assert utils.INJECTOR_SCRIPT.split(":")[1] == cur_file


def test_format_datetime():
    from ssec_amqp import utils

    assert isinstance(utils.format_datetime(datetime.now(tz=timezone.utc)), str)
