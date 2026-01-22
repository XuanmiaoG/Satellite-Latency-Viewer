"""
sat_latency._utils
~~~~~~~~~~~~~~~~~~

Utility functions used internally by sat_latency.
"""

from __future__ import annotations

import functools
import signal
import typing as tp
from contextlib import contextmanager
from datetime import datetime, timedelta


@contextmanager
def signalcontext(signum: int, handler: tp.Callable):
    """Context manager that changes a signal handler on entry and resets it
    on exit.

    Args:
        signum (int): Signal to change.
        handler (tp.Callable): New signal handler to use.
    """
    try:
        orig_h = signal.signal(signum, handler)
        yield
    finally:
        signal.signal(signum, orig_h)


def __raise_interrupt(_sig, _frame):  # noqa: ARG001
    """Signal handler that raises KeybardInterrupt."""
    raise KeyboardInterrupt


def clean_exit(
    original_func=None, *, cleanup_func: tp.Callable[[], None] | None = None
):
    """Decorator that runs a function and cleanly exits after
    EOFErrors and termination signals (SIGINT, SIGTERM).
    """

    def _decorate(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            with signalcontext(signal.SIGTERM, __raise_interrupt):
                try:
                    return function(*args, **kwargs)
                except (EOFError, KeyboardInterrupt):
                    pass
                finally:
                    if cleanup_func:
                        cleanup_func()

        return wrapper

    if original_func:
        return _decorate(original_func)
    return _decorate


def daterange(
    start_date: datetime,
    end_date: datetime | None = None,
    day_step: int | None = None,
) -> tp.Generator[datetime, None, None]:
    """range() function for iterating through datetimes.

    Args:
        start_date (datetime): Start date to iterate from.
        end_date (datetime | None, optional): End date to iterate through
        (inclusive). Defaults to today + 1.
        day_step (int | None, optional): Amount of days in step.
        Defaults to 1.

    Yields:
        datetime: a datetime for each day in range.
    """
    _max_date = end_date or datetime.now(start_date.tzinfo) + timedelta(days=1)
    for n in range(0, (_max_date - start_date).days + 1, day_step or 1):
        yield start_date + timedelta(days=n)
