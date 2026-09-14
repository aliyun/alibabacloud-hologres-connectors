"""Unit tests for HoloClient / AsyncHoloClient metadata helpers.

Covers table_exists, get_holo_version (version-string parsing) and copy_to
(LZ4 / Arrow decode) without a live database, using lightweight connection
fakes that mimic psycopg's cursor / copy context managers.
"""

import asyncio
import io
import struct

import lz4.block
import pyarrow as pa

from hologres.async_client import AsyncHoloClient
from hologres.client import HoloClient


# ── sync connection fakes ────────────────────────────────────────────────


class _FakeCopy:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def rows(self):
        return iter(self._rows)


class _FakeCursor:
    def __init__(self, fetch=None, copy_rows=None):
        self._fetch = fetch
        self._copy_rows = copy_rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return self._fetch

    def copy(self, query):
        return _FakeCopy(self._copy_rows)


class _FakeConn:
    def __init__(self, fetch=None, copy_rows=None):
        self._cursor = _FakeCursor(fetch, copy_rows)

    def cursor(self, *a, **k):
        return self._cursor


def _sync_client(fetch=None, copy_rows=None) -> HoloClient:
    client = HoloClient.__new__(HoloClient)
    client._check_state = lambda: None
    conn = _FakeConn(fetch, copy_rows)
    client._ensure_meta_conn = lambda: conn
    client._ensure_sql_conn = lambda: conn
    return client


def _arrow_ipc_bytes(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _lz4_chunk(ipc_bytes: bytes) -> bytes:
    # 4-byte big-endian uncompressed length prefix + raw LZ4 block.
    compressed = lz4.block.compress(ipc_bytes, store_size=False)
    return struct.pack(">I", len(ipc_bytes)) + compressed


class TestGetHoloVersion:
    def test_parses_major_minor_fix(self):
        client = _sync_client(fetch=("Hologres 3.1.2-abcdef",))
        assert client.get_holo_version() == (3, 1, 2)

    def test_missing_fix_defaults_zero(self):
        client = _sync_client(fetch=("Hologres 2.5",))
        assert client.get_holo_version() == (2, 5, 0)

    def test_no_row_returns_zeros(self):
        assert _sync_client(fetch=None).get_holo_version() == (0, 0, 0)

    def test_unparseable_returns_zeros(self):
        assert _sync_client(fetch=("nonsense",)).get_holo_version() == (0, 0, 0)


class TestTableExists:
    def test_true_when_row(self):
        assert _sync_client(fetch=(1,)).table_exists("t") is True

    def test_false_when_no_row(self):
        assert _sync_client(fetch=None).table_exists("t") is False


class TestCopyTo:
    def _table(self) -> pa.Table:
        return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    def test_lz4_roundtrip(self):
        tbl = self._table()
        chunk = _lz4_chunk(_arrow_ipc_bytes(tbl))
        client = _sync_client(copy_rows=[(chunk,)])
        assert client.copy_to("SELECT 1", format="arrow_lz4").equals(tbl)

    def test_uncompressed_roundtrip(self):
        tbl = self._table()
        client = _sync_client(copy_rows=[(_arrow_ipc_bytes(tbl),)])
        assert client.copy_to("SELECT 1", format="arrow").equals(tbl)

    def test_empty_returns_empty_table(self):
        client = _sync_client(copy_rows=[])
        assert client.copy_to("SELECT 1").num_rows == 0


# ── async fakes (table_exists / get_holo_version mirror the sync logic) ───


class _FakeAsyncCursor:
    def __init__(self, fetch):
        self._fetch = fetch

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute(self, *a, **k):
        pass

    async def fetchone(self):
        return self._fetch


class _FakeAsyncConn:
    def __init__(self, fetch):
        self._fetch = fetch

    def cursor(self, *a, **k):
        return _FakeAsyncCursor(self._fetch)


def _async_client(fetch) -> AsyncHoloClient:
    client = AsyncHoloClient.__new__(AsyncHoloClient)
    client._check_state = lambda: None

    async def _conn():
        return _FakeAsyncConn(fetch)

    client._ensure_meta_conn = _conn
    return client


class TestAsyncMeta:
    def test_get_holo_version(self):
        client = _async_client(("Hologres 3.1.2-abcdef",))
        assert asyncio.run(client.get_holo_version()) == (3, 1, 2)

    def test_table_exists(self):
        assert asyncio.run(_async_client((1,)).table_exists("t")) is True
        assert asyncio.run(_async_client(None).table_exists("t")) is False
