Levyt: Python Database Access Made Easy
=======================================

Levyt provides a very simple interface for running SQL queries against most relational databases. Under the hood, we use
SQLAlchemy to manage the connections, so pretty much any database that is supported by SQLAlchemy is supported here.

## Installing

Per usual, just use `pip` (or your favorite python dependency manager).

```bash
pip install levyt
```

## Basic Usage

Levyt assumes that you just want to write some SQL. You'll start by opening up a database.

Below, you see us connecting to a sqlite in-memory database and setting up a simple table for us to use.

```python
from levyt import Database

db = Database("sqlite:///:memory:")
db.execute("CREATE TABLE numbers (n INTEGER NOT NULL);")
db.execute("INSERT INTO numbers (n) VALUES (42);")
```

When you are making a request which does _not_ expect to have rows returned (i.e. `INSERT` or `UPDATE` queries), you
should use the `execute` method. When you expect to have rows returned (i.e. `SELECT` or `INSERT ... RETURNING *`
queries), then you should use the `query` method.

```python
records = db.query("SELECT * FROM numbers;")

 # Parametric queries work too. It uses the :parameter syntax.
parametric_query = db.query("SELECT * FROM numbers WHERE n=:n", n=42)
```

You can grab records from a collection one at a time:

```python
>>> records[0]
<Record (n=42)>
```

Or you can iterate over them:

```python
for record in records:
    print(record.n)
```

Column values can be access in a few ways:

- As attributes `record.n`
- As items `record['n']`
- As indexes `record[0]`
- Using `get` syntax `record.get('n')`

If your column names have weird characters in them, it should still work.

You can also get only the first record in a collection:

```python
>>> records.first()
<Record (n=42)>
```

If you're expecting exactly one, and you'd like an error if there is more than one, just use `one`.

```python
>>> records.one()
<Record (n=42)>
```

You can also easily transform the collection or record into a dictionary:

```python
>>> records.dict()
[{'n': 42}]
>>> records.first().dict()
{'n': 42}
```


### Getting Table Names

Sometimes, you may want to get the names of all of the tables in the database. You can do that with the
`Database.tables` method.

```python
>>> from levyt import Database
>>> db = Database("sqlite:///:memory:")
>>> db.execute("CREATE TABLE numbers (n INTEGER NOT NULL);")
>>> db.tables()
['numbers']
```

### Transactions

Sometimes, you may want to run queries inside of a transaction:

```python
from levyt import Database
db = Database("sqlite:///:memory:")
db.execute("CREATE TABLE numbers (n INTEGER NOT NULL);")

with db.transaction() as conn:
    conn.query("INSERT INTO numbers (n) VALUES (42);")
    conn.query("INSERT INTO numbers (n) VALUES (43);")
```

If you don't want to use a context manager (and have more control over the transaction), you may do so programmatically:

```python
from levyt import Database
db = Database("sqlite:///:memory:")
db.execute("CREATE TABLE numbers (n INTEGER NOT NULL);")

conn = db.get_connection()
tx = conn.transaction()
try:
    conn.execute('INSERT INTO numbers VALUES (42)')
    conn.execute('INSERT INTO numbers VALUES (43)')
    tx.commit()
except:
    tx.rollback()
finally:
    conn.close()
```

### Exporting Data to Other Formats

Levyt uses tablib under the hood to supply some pretty useful data export functionality. You are able to export your
collections to CSV, XLSX, JSON, HTML Tables, YAML, or Pandas DataFrames very easily.

```python
>>> print(records.dataset)
n
--
42
```

#### CSVs

```python
>>> print(records.export('csv'))
n
42
```

#### Pandas DataFrame

If you have pandas installed as well, then you can export DataFrames:

```python
>>> records.export('df')
    n
0    42
```


## Prior Art

Levyt is largely based on [records by Kenneth Rietz](https://github.com/kenreitz42/records), which seems to be largely
out of support at the time of writing. Levyt mostly has made a some relatively minor usability fixes and API changes.
Additionally, Levyt still makes use of Tablib, also by Rietz, to handle the export logic.

I would be remiss if I didn't mention all of the work that the SQLAlchemy project has done. All of the hard parts of
database connectivity and handling are handled by SQLAlchemy.
