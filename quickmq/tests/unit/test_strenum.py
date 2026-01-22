"""test_strenum

Make sure the custom StrEnum class works as expected.

Tests heavily inspired by the strenum library on pypi.

https://github.com/irgeek/StrEnum/tree/master/tests
"""

from enum import Enum, auto

from ssec_amqp.client import StrEnum


class Example(StrEnum):
    TEST1 = auto()


def test_enum_cls():
    """Sublcasses of StrEnum should be sublcass of Enum."""
    assert issubclass(Example, Enum)


def test_member_cls():
    """StrEnum members should be str."""
    assert isinstance(Example.TEST1, str)


def test_member_value():
    """The value of a StrEnum member should be equal to their name when using auto()."""
    assert Example.TEST1 == "TEST1"


def test_member_hash():
    """The value of the hash of a StrEnum member should be equal to the hash of the
    same string."""
    assert hash(Example.TEST1) == hash("TEST1")
