
from contextlib import contextmanager
from typing import List

from sqlalchemy import create_engine, exc, inspect, text

from .records import Collection, Record


class Connection:
    """A wrapper around the sqlalchemy connections so that we can do our fancy stuff."""

    __slots__ = ("open", "_conn")

    def __init__(self, connection):
        self._conn = connection
        self.open = not connection.closed

    def close(self):
        self._conn.close()
        self.open = False

    def execute(self, query: str, **params):
        """Executes the given SQL query against the db. Parameters may be provided using the `:param` syntax. If
        you expect your query to return result rows, please use query."""
        self._conn.execute(text(query), **params)

    def transaction(self):
        """Returns a transaction on which to run queries. Call commit and rollback as appropriate."""
        return self._conn.begin()

    def query(self, query: str, **params) -> Collection:
        """Executes the given SQL query against the db. Parameters may be provided using `:param` syntax. Returns
        a Collection of Records which can be iterated over to get resulting Records."""
        cursor = self._conn.execute(text(query), **params)

        # Create a row-by-row Record generator and convert results into a Collection.
        record_generator = (Record(list(cursor.keys()), row) for row in cursor)
        return Collection(record_generator)

    # TODO: bulk_query

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    def __repr__(self):
        return f"<Connection open={self.open}>"


class Database:
    """A real db. This holds a connection url and the SQLAlchemy engine with a pool of connections."""

    __slots__ = ("url", "open", "_engine")

    def __init__(self, url: str, **kwargs):
        self.url = url
        self._engine = create_engine(self.url, **kwargs)
        self.open = True

    def close(self):
        """Closes the db, disposes of the engine."""
        self._engine.dispose()
        self.open = False

    def execute(self, query: str, **params):
        """Executes the given SQL query against the db. Parameters may be provided using the `:param` syntax. If
        you expect your query to return result rows, please use query."""
        with self.get_connection() as conn:
            conn.execute(query, **params)

    def get_connection(self) -> Connection:
        """Gets a connection to the db. Connections are retrieved from a pool."""
        if not self.open:
            raise exc.ResourceClosedError("db closed")
        return Connection(self._engine.connect())

    def query(self, query: str, **params) -> Collection:
        """Executes the given SQL query against the db. Parameters may be provided using `:param` syntax. Returns
        a Collection of Records which can be iterated over to get resulting Records."""
        with self.get_connection() as conn:
            return conn.query(query, **params)

    # TODO: bulk_query

    def tables(self) -> List[str]:
        """Returns a list of table names for the connected db."""
        return inspect(self._engine).get_table_names()

    @contextmanager
    def transaction(self) -> Connection:
        """A context manager for executing queries within transactions on the db."""
        conn = self.get_connection()
        tx = conn.transaction()
        try:
            yield conn
            tx.commit()
        except:
            tx.rollback()
        finally:
            conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __repr__(self):
        return f"<Database open={self.open}>"
