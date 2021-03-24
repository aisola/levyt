
from pytest import raises

from levyt import Collection, Record


def check_id(i, row):
    assert row.id == i


def test_collection_iter():
    rows = Collection(Record(['id'], [i]) for i in range(10))
    for i, row in enumerate(rows):
        check_id(i, row)


def test_collection_next():
    rows = Collection(Record(['id'], [i]) for i in range(10))
    for i in range(10):
        check_id(i, next(rows))


def test_collection_iter_and_next():
    rows = Collection(Record(['id'], [i]) for i in range(10))
    i = enumerate(iter(rows))
    check_id(*next(i))  # Cache first row.
    next(rows)  # Cache second row.
    check_id(*next(i))  # Read second row from cache.


def test_collection_multiple_iter():
    rows = Collection(Record(['id'], [i]) for i in range(10))
    i = enumerate(iter(rows))
    j = enumerate(iter(rows))

    check_id(*next(i))  # Cache first row.

    check_id(*next(j))  # Read first row from cache.
    check_id(*next(j))  # Cache second row.

    check_id(*next(i))  # Read second row from cache.


def test_collection_slice_iter():
    rows = Collection(Record(['id'], [i]) for i in range(10))
    for i, row in enumerate(rows[:5]):
        check_id(i, row)
    for i, row in enumerate(rows):
        check_id(i, row)
    assert len(rows) == 10


# all

def test_collection_all_returns_a_list_of_records():
    rows = Collection(Record(['id'], [i]) for i in range(3))
    assert rows.all() == [Record(['id'], [0]), Record(['id'], [1]), Record(['id'], [2])]


# first

def test_collection_first_returns_a_single_record():
    rows = Collection(Record(['id'], [i]) for i in range(1))
    assert rows.first() == Record(['id'], [0])


def test_collection_first_defaults_to_none():
    rows = Collection(iter([]))
    assert rows.first() is None


def test_collection_first_default_is_overridable():
    rows = Collection(iter([]))
    assert rows.first(default="Spam & Eggs") == "Spam & Eggs"


def test_collection_first_raises_default_if_its_an_exception_subclass():
    rows = Collection(iter([]))
    raises(Exception, rows.first, default=Exception)


def test_collection_first_raises_default_if_its_an_exception_instance():
    rows = Collection(iter([]))
    raises(Exception, rows.first, default=Exception("testing"))


# one

def test_collection_one_returns_a_single_record():
    rows = Collection(Record(['id'], [i]) for i in range(1))
    assert rows.one() == Record(['id'], [0])


def test_collection_one_defaults_to_none():
    rows = Collection(iter([]))
    assert rows.one() is None


def test_collection_one_default_is_overridable():
    rows = Collection(iter([]))
    assert rows.one(default="Spam & Eggs") == "Spam & Eggs"


def test_collection_one_raises_when_more_than_one():
    rows = Collection(Record(['id'], [i]) for i in range(3))
    raises(ValueError, rows.one)


def test_collection_one_raises_default_if_its_an_exception_subclass():
    rows = Collection(iter([]))
    raises(Exception, rows.first, default=Exception)


def test_collection_one_raises_default_if_its_an_exception_instance():
    rows = Collection(iter([]))
    raises(Exception, rows.first, default=Exception("testing"))


# scalar

def test_collection_scalar_returns_a_single_record():
    rows = Collection(Record(['id'], [i]) for i in range(1))
    assert rows.scalar() == 0


def test_collection_scalar_defaults_to_none():
    rows = Collection(iter([]))
    assert rows.scalar() is None


def test_collection_scalar_default_is_overridable():
    rows = Collection(iter([]))
    assert rows.scalar(default="testing") == "testing"


def test_collection_scalar_raises_when_more_than_one():
    rows = Collection(Record(['id'], [i]) for i in range(3))
    raises(ValueError, rows.scalar)
