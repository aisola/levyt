import pytest


@pytest.mark.usefixtures('foo_table')
def test_plain_db(db):
    """Manipulate db by `db.execute` and read with `db.query` without transactions."""
    db.execute('INSERT INTO foo VALUES (42)')
    db.execute('INSERT INTO foo VALUES (43)')
    assert db.query('SELECT COUNT(*) AS n FROM foo').first().n == 2


@pytest.mark.usefixtures('foo_table')
def test_plain_conn(db):
    """Manipulate db by `conn.execute` and read with `conn.query` without transactions."""
    conn = db.get_connection()
    conn.execute('INSERT INTO foo VALUES (42)')
    conn.execute('INSERT INTO foo VALUES (43)')
    assert conn.query('SELECT COUNT(*) AS n FROM foo')[0].n == 2


@pytest.mark.usefixtures('foo_table')
def test_failing_transaction_self_managed(db):
    conn = db.get_connection()
    tx = conn.transaction()
    try:
        conn.execute('INSERT INTO foo VALUES (42)')
        conn.execute('INSERT INTO foo VALUES (43)')
        raise ValueError()
        tx.commit()
        conn.execute('INSERT INTO foo VALUES (44)')
    except ValueError:
        tx.rollback()
    finally:
        conn.close()
        assert db.query('SELECT COUNT(*) AS n FROM foo')[0].n == 0


@pytest.mark.usefixtures('foo_table')
def test_failing_transaction(db):
    with db.transaction() as conn:
        conn.query('INSERT INTO foo VALUES (42)')
        conn.query('INSERT INTO foo VALUES (43)')
        raise ValueError()
    assert db.query('SELECT COUNT(*) AS n FROM foo')[0].n == 0


@pytest.mark.usefixtures('foo_table')
def test_passing_transaction_self_managed(db):
    conn = db.get_connection()
    tx = conn.transaction()
    conn.execute('INSERT INTO foo VALUES (42)')
    conn.execute('INSERT INTO foo VALUES (43)')
    tx.commit()
    conn.close()
    assert db.query('SELECT COUNT(*) AS n FROM foo')[0].n == 2


@pytest.mark.usefixtures('foo_table')
def test_passing_transaction(db):
    with db.transaction() as conn:
        conn.execute('INSERT INTO foo VALUES (42)')
        conn.execute('INSERT INTO foo VALUES (43)')
    assert db.query('SELECT COUNT(*) AS n FROM foo')[0].n == 2


@pytest.mark.usefixtures('foo_table')
def test_getting_tables(db):
    assert db.tables() == ['foo']
