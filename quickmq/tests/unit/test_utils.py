import time
from typing import Any, Union
from unittest import mock

import pytest
from amqp.exceptions import RecoverableConnectionError
from ssec_amqp._retry import LazyRetry, RetryError
from ssec_amqp.amqp import AMQPConnectionError, catch_amqp_errors


def mock_function(to_return: Union[Any, None] = None, to_raise: Union[Exception, None] = None):
    if to_return is not None:
        return to_return
    if to_raise is not None:
        raise to_raise
    return None


def test_catch_amqp_errors_no_error():
    """catch_amqp_errors decorator returns original value."""
    rv = 5
    assert catch_amqp_errors(mock_function)(to_return=rv) == rv


def test_catch_amqp_errors_amqp_error():
    """catch_amqp_errors decorator catches amqp errors."""
    with pytest.raises(AMQPConnectionError):
        catch_amqp_errors(mock_function)(to_raise=RecoverableConnectionError)


def test_catch_amqp_errors_diff_error():
    """catch_amqp_errors decorator doesn't catch non amqp errors."""
    err = TypeError
    with pytest.raises(err):
        catch_amqp_errors(mock_function)(to_raise=err)


def test_retry_succeed():
    """RetryAction() returns callable value on success."""
    test_val = 5
    retry = LazyRetry(mock_function, to_return=test_val)
    time.sleep(0.01)  # make sure ready to retry
    assert retry() == test_val


def test_retry_callable_from_method():
    """RetryAction() == RetryAction.retry()"""
    retry = LazyRetry(mock_function)
    with mock.patch.object(retry, "__call__") as patch:
        retry.retry_action()
        patch.assert_called_once()


def test_retry_not_yet():
    """NOT_YET should be returned if RetryAction is between retries."""
    test_val = 5
    retry = LazyRetry(mock_function, retry_interval=1000000, to_return=test_val)
    assert retry() == test_val  # First retry ready after initialization
    assert retry() is LazyRetry.NOT_YET  # Now must wait retry_interval


def test_negative_max_retry():
    """ValueError if negative max_retry"""
    with pytest.raises(ValueError, match="max_retry_attempts"):
        LazyRetry(None, max_retry_attempts=-1)


def test_zero_retry_interval():
    """ValueError if max_retry_interval is 0"""
    with pytest.raises(ValueError, match="max_retry_duration"):
        LazyRetry(None, max_retry_duration=0)


def test_not_callable():
    """ValueError if action isn't callable"""
    with pytest.raises(TypeError):
        LazyRetry(None)


def test_retry_error():
    """RetryAction catches errors and returns FAILED_ATTEMPT."""
    t_err = ValueError
    retry = LazyRetry(mock_function, t_err, to_raise=t_err)
    assert retry() == LazyRetry.FAILED_ATTEMPT


def test_small_interval():
    """time_between_retries must be big enough."""
    with pytest.raises(ValueError, match=".*retry_interval.*"):
        LazyRetry(None, retry_interval=0)
    with pytest.raises(ValueError, match=".*retry_interval.*"):
        LazyRetry(None, retry_interval=-1)
    with pytest.raises(ValueError, match=".*retry_interval.*"):
        LazyRetry(None, retry_interval=0.000000000001)


def test_timeout():
    """RetryError occurs after too long without success."""
    called = False

    def action():
        nonlocal called
        called = True
        raise ValueError

    retry = LazyRetry(action, ValueError, max_retry_duration=0.00001)
    time.sleep(0.1)
    with pytest.raises(RetryError):
        retry()
    assert called


def test_attempts():
    """RetryAction.attempts is tracked correctly."""
    attempts = 3

    def action():
        pass

    retry = LazyRetry(action, retry_interval=0.00001)
    for _ in range(attempts):
        time.sleep(0.001)
        retry()
    assert retry.attempts == attempts
