import pytest

from levyt import Database


@pytest.fixture(scope="session")
def db():
    d = Database("sqlite:///:memory:")
    yield d
    d.close()


@pytest.fixture
def foo_table(db):
    db.execute('CREATE TABLE foo (a integer)')
    yield
    db.execute('DROP TABLE foo')
