
from collections import OrderedDict
from inspect import isclass
from typing import Any, Dict, Iterator, List, Tuple, Union

from tablib import Dataset


class Record:
    """A single record resulting from a db query."""
    __slots__ = ('_keys', '_values')

    def __init__(self, keys: List[str], values: List[Any]):
        self._keys = keys
        self._values = values
        assert len(self._keys) == len(self._values), "record must have the same number of keys as values"

    @property
    def dataset(self) -> Dataset:
        """Returns a tablib dataset containing the record."""
        ds = Dataset()
        ds.headers = self.keys()
        ds.append(_dt_to_str(self.values()))
        return ds

    def dict(self, *, ordered=False) -> Dict[str, Any]:
        """Returns the record as a dictionary. If ordered==True, the dictionary will be an OrderedDict."""
        items = zip(self.keys(), self.values())
        return OrderedDict(items) if ordered else dict(items)

    def export(self, format: str, **kwargs) -> Any:
        """Exports the row into the given format (Thanks, Tablib!)"""
        return self.data.format(format, **kwargs)

    def get(self, key: str, *, default: Any = None) -> Any:
        """Returns the value for the given key."""
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self) -> List[str]:
        """Returns the keys of the record."""
        return self._keys

    def values(self) -> List[Any]:
        """Returns the values of the record"""
        return self._values

    def __dir__(self) -> List[str]:
        standard = dir(super(Record, self))
        return sorted(standard + [str(k) for k in self.keys()])

    def __eq__(self, other):
        if not isinstance(other, Record):
            return False

        return self._keys == other._keys and self._values == other._values

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __getitem__(self, key: Union[str, int]) -> Any:
        # Try integer index-based lookup...
        if isinstance(key, int):
            return self.values()[key]

        # Default to string-index based lookup.
        if key in self.keys():
            if self.keys().count(key) > 1:
                raise KeyError(f"Record contains multiple '{key}' fields.")

            index = self.keys().index(key)
            return self.values()[index]

        raise KeyError(f"Record contains no '{key}' field.")

    def __repr__(self):
        items = ", ".join([f"{key}={value}" for key, value in self.dict(ordered=True).items()])
        return f"<Record ({items})>"


class Collection:
    """The collection of record results from a db query."""
    __slots__ = ("_rows", "_all_rows", "pending")

    def __init__(self, rows: Iterator[Record]):
        self._rows = rows
        self._all_rows = []
        self.pending = True

    def all(self, as_dict: bool = False, ordered: bool = False) -> List[Dict[str, Any]]:
        """Returns a list of all rows in the Collection. If they haven't been fetched yet, consume the iterator and
        cache the results. Setting as_dict to True will return the rows as dicts instead of Records. Setting ordered
        will set the ordered flag on Record.dict()."""

        # By calling list it calls the __iter__ method
        rows = list(self)

        if as_dict:
            return [r.dict(ordered=ordered) for r in rows]

        return rows

    @property
    def dataset(self) -> Dataset:
        """Returns a tablib.Dataset for the collection."""
        ds = Dataset()

        # If we have an empty Collection, return the empty set. By calling list, we are consuming the iterator.
        if len(list(self)) == 0:
            return ds

        # Set the column names as headers on Tablib Dataset.
        ds.headers = self[0].keys()

        for record in self.all():
            record = _dt_to_str(record.values())
            ds.append(record)

        return ds

    def dict(self, *, ordered: bool = False) -> List[Dict[str, Any]]:
        """Returns all records as dictionaries. Despite the misleading name, the result should be a list. The naming is
        kept consistent with Record in order to avoid confusion."""
        return self.all(as_dict=True, ordered=ordered)

    def export(self, format: str, **kwargs) -> Any:
        """Exports the the Collection to the given format. (Thanks tablib!)"""
        return self.dataset.export(format, **kwargs)

    def first(self, *, default: Any = None, as_dict: bool = False, ordered: bool = False) -> Any:
        """Returns a single Record from the Collection. It will be the first result (not including headers). If there
        are no results, `default` will be returned. If default is an instance or subclass of an exception, then it will
        be raised instead of returning it."""
        try:
            record = self[0]
        except IndexError:
            if _is_exception(default):
                raise default
            return default

        if as_dict:
            return record.dict(ordered=ordered)

        return record

    def next(self) -> Record:
        return self.__next__()

    def one(self, *, default: Any = None, as_dict: bool = False, ordered: bool = False) -> Any:
        """Returns a single record from the Collection. It will be the first result, ensuring that there is exactly one
        result in the Collection. If there are no results, `default` will be returned. If default is an instance or
        subclass of an exception, then it will be raised instead of returning it."""
        try:
            self[1]
        except IndexError:
            return self.first(default=default, as_dict=as_dict, ordered=ordered)
        else:
            raise ValueError("Collection contains more than one row, exactly one row expected.")

    def scalar(self, *, default: Any = None) -> Any:
        """Returns the first column of the first record or `default`."""
        record = self.one()
        return record[0] if record else default

    def __getitem__(self, key: Union[str, int]) -> Any:
        is_int = isinstance(key, int)

        # Convert Collection[1] into slice.
        if is_int:
            key = slice(key, key + 1)

        while len(self) < key.stop or key.stop is None:
            try:
                next(self)
            except StopIteration:
                break

        rows = self._all_rows[key]
        if is_int:
            return rows[0]
        else:
            return Collection(iter(rows))

    def __iter__(self):
        """Iterate over all records, consuming the underlying generator when necessary."""
        i = 0
        while True:
            # Other code may have run between the yields, so check the cache.
            if i < len(self):
                yield self[i]
            else:
                # Throws StopIteration when done.
                # Prevent StopIteration bubbling from generator, following https://www.python.org/dev/peps/pep-0479/
                try:
                    yield next(self)
                except StopIteration:
                    return
            i += 1

    def __len__(self):
        return len(self._all_rows)

    def __next__(self):
        try:
            record = next(self._rows)
            self._all_rows.append(record)
            return record
        except StopIteration:
            self.pending = False
            raise StopIteration('Collection contains no more rows.')

    def __repr__(self):
        return f"<Collection size={len(self)} pending={self.pending}>"


def _is_exception(obj) -> bool:
    """Given an object, return a boolean indicating whether it is an instance
    or subclass of :py:class:`Exception`.
    """
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False


def _dt_to_str(row: List[Any]) -> Tuple[Any]:
    """Receives a row, converts datetimes to strings."""
    row = list(row)
    for i in range(len(row)):
        if hasattr(row[i], 'isoformat'):
            row[i] = row[i].isoformat()
    return tuple(row)
