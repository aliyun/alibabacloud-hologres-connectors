"""Integration tests for the Hologres Ray connector.

These tests require a running Hologres/PostgreSQL database and exercise the
real ``read_hologres`` / ``write_hologres`` paths end-to-end (no mocks). They
follow the same environment-variable convention as
``holo-client-py/tests/test_integration.py``.

To run integration tests:
    export HOLO_TEST_HOST=localhost
    export HOLO_TEST_PORT=5432
    export HOLO_TEST_DATABASE=postgres  # Base database used to create the test db
    export HOLO_TEST_USERNAME=test_user
    export HOLO_TEST_PASSWORD=test_pass
    pytest tests/test_integration.py -v

When the environment variables are not set the whole module is skipped, so
``pytest tests/`` stays green without a database.
"""

import datetime
import os
import time
from urllib.parse import quote

import psycopg
import pytest

ray = pytest.importorskip("ray")
pytest.importorskip("ray.data")

from hologres_ray import (  # noqa: E402
    OnConflictAction,
    WriteMode,
    read_hologres,
    write_hologres,
)

# ── Environment / connection setup ──────────────────────────────────────

_REQUIRED_ENV = [
    "HOLO_TEST_HOST",
    "HOLO_TEST_PORT",
    "HOLO_TEST_DATABASE",
    "HOLO_TEST_USERNAME",
    "HOLO_TEST_PASSWORD",
]

_missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
if _missing:
    pytest.skip(
        "Integration test environment not configured. Missing: " + ", ".join(_missing),
        allow_module_level=True,
    )


def _base_conn_params():
    return {
        "host": os.environ["HOLO_TEST_HOST"],
        "port": int(os.environ["HOLO_TEST_PORT"]),
        "database": os.environ["HOLO_TEST_DATABASE"],
        "user": os.environ["HOLO_TEST_USERNAME"],
        "password": os.environ["HOLO_TEST_PASSWORD"],
    }


def _build_uri(database: str) -> str:
    p = _base_conn_params()
    user = quote(p["user"], safe="")
    password = quote(p["password"], safe="")
    return f"postgresql://{user}:{password}@{p['host']}:{p['port']}/{quote(database)}"


def _get_test_db_name() -> str:
    return f"holo_ray_test_{int(time.time())}"


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    """Start a small local Ray cluster for the module."""
    if not ray.is_initialized():
        ray.init(num_cpus=2, ignore_reinit_error=True, log_to_driver=False)
    yield
    ray.shutdown()


@pytest.fixture(scope="module")
def test_database():
    """Create a fresh test database for the module and drop it afterwards."""
    params = _base_conn_params()
    test_db_name = _get_test_db_name()

    conn = psycopg.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["database"],
        user=params["user"],
        password=params["password"],
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{test_db_name}"')
            cur.execute(
                f'ALTER DATABASE "{test_db_name}" '
                f"SET hg_experimental_enable_shard_count_cap = off"
            )
    finally:
        conn.close()

    yield test_db_name

    conn = psycopg.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["database"],
        user=params["user"],
        password=params["password"],
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{test_db_name}"')
    finally:
        conn.close()


@pytest.fixture
def connection_uri(test_database):
    return _build_uri(test_database)


@pytest.fixture
def require_stage_support(connection_uri):
    """Skip COPY_STAGE tests on instances that predate internal stages."""
    from hologres import HoloClient, HoloConfig

    with HoloClient(HoloConfig.from_connection_uri(connection_uri)) as client:
        version = client.get_holo_version()
    if version < (4, 1, 0):
        pytest.skip(
            "COPY_STAGE requires Hologres >= 4.1.0, instance reports "
            + ".".join(map(str, version))
        )


@pytest.fixture
def db_conn(test_database):
    """A raw autocommit connection to the test database for DDL / verification."""
    params = _base_conn_params()
    conn = psycopg.connect(
        host=params["host"],
        port=params["port"],
        dbname=test_database,
        user=params["user"],
        password=params["password"],
        autocommit=True,
    )
    yield conn
    conn.close()


# ── Helpers ─────────────────────────────────────────────────────────────


def _create_table(conn, ddl: str):
    with conn.cursor() as cur:
        cur.execute(ddl)


def _insert_rows(conn, table: str, columns, rows):
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
            rows,
        )


def _select_all(conn, table: str, order_by: str = "id"):
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(f"SELECT * FROM {table} ORDER BY {order_by}")
        return cur.fetchall()


def _rows_from_dataset(ds) -> list:
    """Return dataset rows as a list of plain dicts sorted by id."""
    rows = ds.take_all()
    return sorted((dict(r) for r in rows), key=lambda r: r["id"])


# ── Read tests ──────────────────────────────────────────────────────────


class TestReadHologres:
    def test_read_copy_to(self, connection_uri, db_conn):
        table = "read_copy_to"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT, val INT)",
        )
        _insert_rows(
            db_conn,
            table,
            ["id", "name", "val"],
            [(i, f"name{i}", i * 10) for i in range(20)],
        )

        ds = read_hologres(
            table_name=table,
            connection_uri=connection_uri,
            read_mode="copy_to",
        )
        rows = _rows_from_dataset(ds)

        assert len(rows) == 20
        assert rows[0] == {"id": 0, "name": "name0", "val": 0}
        assert rows[19] == {"id": 19, "name": "name19", "val": 190}

    def test_read_select_mode(self, connection_uri, db_conn):
        table = "read_select"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT)",
        )
        _insert_rows(db_conn, table, ["id", "name"], [(i, f"n{i}") for i in range(10)])

        ds = read_hologres(
            table_name=table,
            connection_uri=connection_uri,
            read_mode="select",
        )
        rows = _rows_from_dataset(ds)
        assert len(rows) == 10
        assert {r["name"] for r in rows} == {f"n{i}" for i in range(10)}

    def test_read_uncompressed(self, connection_uri, db_conn):
        table = "read_uncompressed"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT)",
        )
        _insert_rows(db_conn, table, ["id", "name"], [(i, f"n{i}") for i in range(5)])

        ds = read_hologres(
            table_name=table,
            connection_uri=connection_uri,
            read_mode="copy_to",
            is_compressed=False,
        )
        rows = _rows_from_dataset(ds)
        assert len(rows) == 5

    def test_read_with_columns(self, connection_uri, db_conn):
        table = "read_columns"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT, val INT)",
        )
        _insert_rows(
            db_conn,
            table,
            ["id", "name", "val"],
            [(i, f"n{i}", i) for i in range(8)],
        )

        ds = read_hologres(
            table_name=table,
            connection_uri=connection_uri,
            columns=["id", "val"],
        )
        rows = _rows_from_dataset(ds)
        assert len(rows) == 8
        assert set(rows[0].keys()) == {"id", "val"}

    def test_read_with_where_filter(self, connection_uri, db_conn):
        table = "read_where"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, val INT)",
        )
        _insert_rows(db_conn, table, ["id", "val"], [(i, i) for i in range(20)])

        ds = read_hologres(
            table_name=table,
            connection_uri=connection_uri,
            where_filter="val >= 10",
        )
        rows = _rows_from_dataset(ds)
        assert len(rows) == 10
        assert min(r["val"] for r in rows) == 10

    def test_read_shard_parallel(self, connection_uri, db_conn):
        table = "read_shards"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, val INT)",
        )
        _insert_rows(db_conn, table, ["id", "val"], [(i, i) for i in range(200)])

        # Force multiple read tasks; result must still be complete & unique.
        ds = read_hologres(
            table_name=table,
            connection_uri=connection_uri,
            override_num_blocks=4,
        )
        rows = _rows_from_dataset(ds)
        ids = [r["id"] for r in rows]
        assert len(ids) == 200
        assert sorted(ids) == list(range(200))

    def test_read_nonexistent_table_raises(self, connection_uri):
        with pytest.raises(ValueError):
            read_hologres(
                table_name="does_not_exist_table",
                connection_uri=connection_uri,
            )


# ── Write tests ─────────────────────────────────────────────────────────


class TestWriteHologres:
    def test_write_bulk_load_no_pk(self, connection_uri, db_conn):
        table = "write_no_pk"
        _create_table(db_conn, f"CREATE TABLE {table} (id INT, name TEXT)")

        ds = ray.data.from_items([{"id": i, "name": f"name{i}"} for i in range(30)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_BULK_LOAD,
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 30
        assert {r["name"] for r in rows} == {f"name{i}" for i in range(30)}

    def test_write_bulk_load_with_pk_reshuffle(self, connection_uri, db_conn):
        table = "write_pk_reshuffle"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT)",
        )

        ds = ray.data.from_items([{"id": i, "name": f"name{i}"} for i in range(100)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_BULK_LOAD,
            on_conflict_action=OnConflictAction.UPDATE,
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 100
        assert sorted(r["id"] for r in rows) == list(range(100))

    def test_write_on_conflict_update(self, connection_uri, db_conn):
        table = "write_conflict_update"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT)",
        )
        _insert_rows(db_conn, table, ["id", "name"], [(i, "old") for i in range(5)])

        ds = ray.data.from_items([{"id": i, "name": "new"} for i in range(5)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STREAM,
            on_conflict_action=OnConflictAction.UPDATE,
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 5
        assert all(r["name"] == "new" for r in rows)

    def test_write_on_conflict_ignore(self, connection_uri, db_conn):
        table = "write_conflict_ignore"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT)",
        )
        _insert_rows(db_conn, table, ["id", "name"], [(i, "old") for i in range(5)])

        ds = ray.data.from_items([{"id": i, "name": "new"} for i in range(5)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STREAM,
            on_conflict_action=OnConflictAction.IGNORE,
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 5
        assert all(r["name"] == "old" for r in rows)

    def test_write_binary_format(self, connection_uri, db_conn):
        table = "write_binary"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT, val INT)",
        )

        ds = ray.data.from_items(
            [{"id": i, "name": f"b{i}", "val": i * 2} for i in range(20)]
        )
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STREAM,
            on_conflict_action=OnConflictAction.UPDATE,
            format="binary",
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 20
        assert rows[5]["val"] == 10


# ── COPY_STAGE write tests ──────────────────────────────────────────────


class TestWriteCopyStage:
    """COPY_STAGE uploads Arrow IPC to a shared internal stage, then loads it
    with a single server-side INSERT...SELECT after all tasks finish."""

    def test_write_stage_no_pk(self, connection_uri, db_conn, require_stage_support):
        table = "stage_no_pk"
        _create_table(db_conn, f"CREATE TABLE {table} (id INT, name TEXT, val INT)")

        ds = ray.data.from_items(
            [{"id": i, "name": f"name{i}", "val": i * 10} for i in range(200)]
        )
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STAGE,
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 200
        assert rows[0] == {"id": 0, "name": "name0", "val": 0}
        assert rows[199] == {"id": 199, "name": "name199", "val": 1990}

    def test_write_stage_multiple_blocks_commit_once(
        self, connection_uri, db_conn, require_stage_support
    ):
        """Each block becomes its own stage file; the coordinator commits once,
        so every row must land exactly once (no duplicates from re-reads)."""
        table = "stage_multi_block"
        _create_table(db_conn, f"CREATE TABLE {table} (id INT, val INT)")

        ds = ray.data.from_items(
            [{"id": i, "val": i} for i in range(400)], override_num_blocks=8
        )
        assert ds.num_blocks() > 1, "test needs more than one block to be meaningful"
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STAGE,
        )

        with db_conn.cursor() as cur:
            cur.execute(f"SELECT count(*), count(DISTINCT id), sum(val) FROM {table}")
            count, distinct, total = cur.fetchone()
        assert count == 400
        assert distinct == 400
        assert total == sum(range(400))

    def test_write_stage_on_conflict_update(
        self, connection_uri, db_conn, require_stage_support
    ):
        table = "stage_conflict_update"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT)",
        )
        _insert_rows(db_conn, table, ["id", "name"], [(i, "old") for i in range(5)])

        ds = ray.data.from_items([{"id": i, "name": "new"} for i in range(5)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STAGE,
            on_conflict_action=OnConflictAction.UPDATE,
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 5
        assert all(r["name"] == "new" for r in rows)

    def test_write_stage_on_conflict_ignore(
        self, connection_uri, db_conn, require_stage_support
    ):
        table = "stage_conflict_ignore"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT)",
        )
        _insert_rows(db_conn, table, ["id", "name"], [(i, "old") for i in range(5)])

        ds = ray.data.from_items([{"id": i, "name": "new"} for i in range(5)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STAGE,
            on_conflict_action=OnConflictAction.IGNORE,
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 5
        assert all(r["name"] == "old" for r in rows)

    def test_write_stage_column_subset(
        self, connection_uri, db_conn, require_stage_support
    ):
        """The dataset may carry fewer columns than the table; the commit must
        select exactly those columns and leave the rest NULL."""
        table = "stage_col_subset"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, name TEXT, val INT)",
        )

        ds = ray.data.from_items([{"id": i, "name": f"n{i}"} for i in range(10)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STAGE,
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 10
        assert rows[3]["name"] == "n3"
        assert all(r["val"] is None for r in rows)

    def test_write_stage_ignores_format(
        self, connection_uri, db_conn, require_stage_support
    ):
        """COPY_STAGE always uploads Arrow IPC, so ``format`` is a no-op."""
        table = "stage_ignores_format"
        _create_table(
            db_conn,
            f"CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, val INT)",
        )

        ds = ray.data.from_items([{"id": i, "val": i * 3} for i in range(20)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STAGE,
            format="binary",
        )

        rows = _select_all(db_conn, table)
        assert len(rows) == 20
        assert rows[5]["val"] == 15

    def test_write_stage_dropped_after_write(
        self, connection_uri, db_conn, require_stage_support
    ):
        """The datasink drops its stage on success, leaving nothing behind."""
        table = "stage_cleanup"
        _create_table(db_conn, f"CREATE TABLE {table} (id INT, val INT)")

        ds = ray.data.from_items([{"id": i, "val": i} for i in range(10)])
        write_hologres(
            ds,
            table_name=table,
            connection_uri=connection_uri,
            write_mode=WriteMode.COPY_STAGE,
        )

        # The datasink names its stage ray_{schema}_{table}_{uuid}, so scope the
        # check to this table instead of asserting on unrelated stages.
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT stage_name FROM hologres.hg_internal_stages "
                "WHERE stage_name LIKE %s",
                (f"ray_public_{table}_%",),
            )
            leaked = [r[0] for r in cur.fetchall()]
        assert leaked == [], f"stage(s) left behind after write: {leaked}"


# ── Full round-trip ─────────────────────────────────────────────────────


class TestRoundTrip:
    def test_write_then_read(self, connection_uri, db_conn):
        table = "round_trip"
        _create_table(
            db_conn,
            f"""CREATE TABLE {table} (
                id INT NOT NULL PRIMARY KEY,
                name TEXT,
                flag BOOL,
                ratio FLOAT8,
                ts TIMESTAMP
            )""",
        )

        base_ts = datetime.datetime(2024, 1, 1, 0, 0, 0)
        source = [
            {
                "id": i,
                "name": f"name{i}",
                "flag": i % 2 == 0,
                "ratio": float(i) / 2,
                "ts": base_ts + datetime.timedelta(seconds=i),
            }
            for i in range(50)
        ]

        write_hologres(
            ray.data.from_items(source),
            table_name=table,
            connection_uri=connection_uri,
            on_conflict_action=OnConflictAction.UPDATE,
        )

        ds = read_hologres(table_name=table, connection_uri=connection_uri)
        rows = _rows_from_dataset(ds)

        assert len(rows) == 50
        assert rows[10]["name"] == "name10"
        # bool comes back Arrow/numpy-backed, so compare by value not identity
        assert bool(rows[10]["flag"]) is True  # id 10 is even
        assert bool(rows[11]["flag"]) is False  # id 11 is odd
        assert abs(rows[7]["ratio"] - 3.5) < 1e-6
