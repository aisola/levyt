
from levyt import Record

from pytest import raises


def test_record_dir():
    keys, values = ['id', 'name', 'email'], [1, '', '']
    record = Record(keys, values)
    _dir = dir(record)
    for key in keys:
        assert key in _dir
    for key in dir(object):
        assert key in _dir


def test_record_duplicate_column():
    keys, values = ['id', 'name', 'email', 'email'], [1, '', '', '']
    record = Record(keys, values)
    with raises(KeyError):
        record['email']