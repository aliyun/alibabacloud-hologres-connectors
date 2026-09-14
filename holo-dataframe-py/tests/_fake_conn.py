"""Test doubles for the psycopg driver.

Enables Session tests without a live Hologres. The doubles are intentionally
minimal but faithful to the parts of psycopg3 that Session touches: cursor
context managers, ``execute``/``fetchall``, iteration on named cursors, and
``description`` as a list of column info objects.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any


@dataclass
class FakeColumnInfo:
    """Duck-typed stand-in for psycopg's cursor.description entry."""

    name: str
    type_code: int  # PostgreSQL type OID


# OIDs we use throughout the tests.
OID_INT4 = 23
OID_INT8 = 20
OID_TEXT = 25
OID_VARCHAR = 1043
OID_FLOAT8 = 701
OID_BOOL = 16


class FakeCursor:
    """A single scripted cursor.

    Args:
        connection: The parent :class:`FakeConnection`. Each ``execute`` pulls
            the next scripted response off the connection, so successive cursors
            keep advancing through the script (matching the fact that the real
            driver would return a fresh cursor per call).
        record: The shared list where executed queries are appended (rendered
            SQL string), letting tests assert on generated SQL.
        name: The name of a server-side named cursor, if any.
    """

    def __init__(
        self,
        connection: "FakeConnection",
        record: list[str],
        name: str | None = None,
    ) -> None:
        self._conn = connection
        self._record = record
        self._closed = False
        self.name = name  # named cursors set this
        self._rows: list[tuple] = []
        self._offset = 0
        self.description: list[FakeColumnInfo] | None = None

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._closed = True

    def execute(self, query: Any, params: Any = None) -> None:
        # Record what the caller sent us.
        if hasattr(query, "as_string"):
            self._record.append(query.as_string())
        else:
            self._record.append(str(query))
        rows, description = self._conn._next_response()
        self._rows = list(rows)
        self._offset = 0
        self.description = list(description) if description else None

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def fetchmany(self, size: int) -> list[tuple]:
        """Return the next ``size`` rows, advancing an internal offset."""
        batch = self._rows[self._offset : self._offset + size]
        self._offset += len(batch)
        return list(batch)

    def copy(self, statement: Any) -> "FakeCopy":
        """Stand in for ``cursor.copy``, recording the rows written."""
        if hasattr(statement, "as_string"):
            self._record.append(statement.as_string())
        else:
            self._record.append(str(statement))
        return FakeCopy(self._conn)

    def __iter__(self):
        yield from self._rows


class FakeCopy:
    """Collects rows/bytes handed to a COPY, for later assertions."""

    def __init__(self, connection: "FakeConnection") -> None:
        self._conn = connection

    def __enter__(self) -> "FakeCopy":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def write_row(self, row) -> None:
        self._conn.copied_rows.append(tuple(row))

    def write(self, data) -> None:
        self._conn.copied_data.append(data)


class FakeConnection:
    """A single scripted connection producing scripted cursors."""

    def __init__(
        self,
        script: list[tuple[list[tuple], list[FakeColumnInfo]]] | None = None,
    ) -> None:
        self._script = script or []
        self._index = -1
        self.recorded: list[str] = []
        self.closed = False
        # Rows / byte payloads handed to COPY, for assertions.
        self.copied_rows: list[tuple] = []
        self.copied_data: list = []
        # Track cursors so tests can inspect.
        self.cursors: list[FakeCursor] = []

    def _next_response(self) -> tuple[list[tuple], list[FakeColumnInfo]]:
        """Advance and return the next scripted response.

        Once the script is exhausted the last entry keeps being returned, so
        tests that don't care about later responses can leave them out.
        """
        if not self._script:
            return [], []
        self._index = min(self._index + 1, len(self._script) - 1)
        return self._script[self._index]

    def cursor(self, name: str | None = None) -> FakeCursor:
        cur = FakeCursor(self, self.recorded, name=name)
        self.cursors.append(cur)
        return cur

    def close(self) -> None:
        self.closed = True


class ServerlessConnection(FakeConnection):
    """Fake connection that records transactions and can reject a setting."""

    def __init__(self, script=None, *, fail_on=None):
        super().__init__(script)
        self.fail_on = fail_on
        self.events = []

    def cursor(self, name=None):
        cursor = ServerlessCursor(self, self.recorded, name=name)
        self.cursors.append(cursor)
        return cursor

    @contextmanager
    def transaction(self):
        self.events.append("begin")
        try:
            yield
        except Exception:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")


class ServerlessCursor(FakeCursor):
    """Cursor companion for :class:`ServerlessConnection`."""

    def execute(self, query, params=None):
        text = query.as_string() if hasattr(query, "as_string") else str(query)
        if self._conn.fail_on and self._conn.fail_on in text:
            self._record.append(text)
            raise RuntimeError("setting is unavailable")
        super().execute(query, params)


def make_session(script=None):
    """Build a Session backed by a FakeConnection with the given script."""
    from hologres.dataframe.session import _session_from_connection

    conn = FakeConnection(script=script or [])
    session = _session_from_connection(conn, dbname="test_db")
    return session, conn


def make_reconnectable_session(scripts_by_db, dbname):
    """Build a Session whose pool_factory reconnects to another database.

    Args:
        scripts_by_db: Maps a database name to its scripted responses (the same
            ``(rows, description)`` list format ``make_session`` accepts).
        dbname: The initial database.

    Returns:
        ``(session, opened)`` where ``opened`` is a list recording, in order,
        every ``_SingleConnectionPool`` the factory built (each exposes
        ``_closed`` and ``_conn``), so a test can assert which pools were opened
        and whether the old one was closed.
    """
    from hologres.dataframe.session import Session, _SingleConnectionPool

    opened = []

    def factory(db):
        conn = FakeConnection(script=scripts_by_db.get(db, []))
        pool = _SingleConnectionPool(conn)
        opened.append(pool)
        return pool

    session = Session(factory(dbname), dbname=dbname, pool_factory=factory)
    return session, opened
