"""Tests for Session, hg.connect, catalog helpers, and error wrapping."""

from unittest.mock import patch

import pytest

import hologres.dataframe as hg
from hologres.dataframe.exceptions import ExecutionError, InvalidArgumentError
from hologres.dataframe.session import Session, _session_from_connection, connect
from tests._fake_conn import (
    OID_INT4,
    OID_TEXT,
    FakeColumnInfo,
    FakeConnection,
    make_reconnectable_session,
    make_session,
)


class TestConnectValidation:
    """hg.connect() argument checks (no network I/O)."""

    def _kwargs(self, **overrides):
        base = dict(host="h", port=80, dbname="d", user="u", password="p")
        base.update(overrides)
        return base

    @pytest.mark.parametrize("field", ["host", "dbname", "user", "password"])
    def test_missing_required_string_rejected(self, field):
        with pytest.raises(InvalidArgumentError, match=f"{field} must be a non-empty"):
            connect(**self._kwargs(**{field: ""}))

    def test_port_must_be_positive_int(self):
        with pytest.raises(InvalidArgumentError, match="port must be a positive int"):
            connect(**self._kwargs(port=-1))

    def test_port_bool_rejected(self):
        with pytest.raises(InvalidArgumentError, match="port must be a positive int"):
            connect(**self._kwargs(port=True))

    def test_extra_kwargs_forwarded(self):
        # Non-pool kwargs go through **extra and land in the conninfo.
        from hologres.dataframe import session as session_mod

        captured = {}

        class SpyPool:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def close(self):
                pass

        from unittest.mock import patch

        with patch.object(session_mod, "ConnectionPool", SpyPool):
            s = connect(
                host="h",
                port=80,
                dbname="d",
                user="u",
                password="p",
                sslmode="require",
            )
        assert "sslmode=require" in captured["conninfo"]
        s.close()

    def test_extra_options_forwarded(self):
        """Server-side GUCs via libpq `options` must land in the conninfo.

        Regression guard: prevents future refactors from silently dropping
        the `**extra` passthrough for the `options` keyword.
        """
        from unittest.mock import patch

        from hologres.dataframe import session as session_mod

        captured = {}

        class SpyPool:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def close(self):
                pass

        with patch.object(session_mod, "ConnectionPool", SpyPool):
            s = connect(
                host="h",
                port=80,
                dbname="d",
                user="u",
                password="p",
                sslmode="require",
                options="-c statement_timeout=5000",
            )
        # Both TLS and options must survive.
        assert "sslmode=require" in captured["conninfo"]
        # libpq quotes values containing '=' or spaces; assert on the key + payload.
        assert "options=" in captured["conninfo"]
        assert "statement_timeout=5000" in captured["conninfo"]
        s.close()

    def test_fc_config_is_validated_before_opening_pool(self):
        from hologres.dataframe import session as session_mod

        with patch.object(
            session_mod,
            "ConnectionPool",
            side_effect=AssertionError("pool must not open"),
        ):
            with pytest.raises(InvalidArgumentError, match="internal FC"):
                connect(
                    **self._kwargs(),
                    fc_config={"endpoint": "public.example.com", "region": "r"},
                )

    def test_fc_config_is_not_forwarded_to_libpq(self):
        from hologres.dataframe import session as session_mod

        captured = {}

        class SpyPool:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def close(self):
                pass

        config = {
            "endpoint": "123.cn-hangzhou-internal.fc.aliyuncs.com",
            "region": "cn-hangzhou",
        }
        with patch.object(session_mod, "ConnectionPool", SpyPool):
            session = connect(**self._kwargs(), fc_config=config)
        assert "fc_config" not in captured["conninfo"]
        assert session._fc_config["endpoint"] == config["endpoint"]
        session.close()


class TestConnectSuccess:
    """Verify connect() actually opens a pool. Uses env vars if provided."""

    def test_pool_creation_is_attempted(self):
        # Without env vars, we just make sure the guard path returns the pool
        # construction to psycopg_pool without touching the network prematurely.
        # We do that by monkeypatching ConnectionPool to a spy.
        from hologres.dataframe import session as session_mod

        captured = {}

        class SpyPool:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def close(self):
                captured["closed"] = True

        with patch.object(session_mod, "ConnectionPool", SpyPool):
            s = connect(
                host="h",
                port=80,
                dbname="d",
                user="u",
                password="p",
                max_size=7,
                application_name="ut",
            )
        assert isinstance(s, Session)
        assert "conninfo" in captured
        assert "host=h" in captured["conninfo"]
        assert "port=80" in captured["conninfo"]
        assert "dbname=d" in captured["conninfo"]
        assert "application_name=ut" in captured["conninfo"]
        assert captured["max_size"] == 7
        assert captured["kwargs"] == {"autocommit": True}
        s.close()
        assert captured.get("closed") is True


class TestSessionDataFrameSources:
    def test_session_sources_bind_the_session(self):
        session, _ = make_session()

        from_dict = session.from_dict({"id": [1], "name": ["a"]})
        from_records = session.from_records([(1, "a")], columns=["id", "name"])

        assert from_dict._session is session
        assert from_records._session is session
        assert from_dict.columns == ["id", "name"]
        assert from_records.columns == ["id", "name"]

    def test_session_from_pandas_binds_the_session(self):
        import pandas as pd

        session, conn = make_session()
        frame = session.from_pandas(pd.DataFrame({"id": [1], "name": ["a"]}))

        assert frame._session is session
        assert frame.columns == ["id", "name"]
        assert conn.recorded == []


class TestSessionLifecycle:
    def test_context_manager_closes(self):
        session, conn = make_session()
        with session as s:
            assert s.closed is False
        assert session.closed is True
        assert conn.closed is True

    def test_close_is_idempotent(self):
        session, _ = make_session()
        session.close()
        session.close()  # should not raise
        assert session.closed is True

    def test_execute_after_close_rejected(self):
        from psycopg import sql

        session, _ = make_session()
        session.close()
        with pytest.raises(InvalidArgumentError, match="closed"):
            session.execute(sql.SQL("SELECT 1"))


class TestExecute:
    def test_returns_rows_and_schema(self):
        from psycopg import sql

        session, conn = make_session(
            script=[
                (
                    [(1, "a"), (2, "b")],
                    [FakeColumnInfo("id", OID_INT4), FakeColumnInfo("name", OID_TEXT)],
                )
            ]
        )
        rows, schema = session.execute(sql.SQL("SELECT id, name FROM t"))
        assert schema.names == ("id", "name")
        assert schema["id"].datatype == hg.IntegerType()
        assert schema["name"].datatype == hg.StringType()
        assert rows[0].as_dict() == {"id": 1, "name": "a"}
        assert rows[1]["name"] == "b"
        assert conn.recorded == ["SELECT id, name FROM t"]

    def test_no_fetch_returns_empty(self):
        from psycopg import sql

        session, conn = make_session(script=[([], [])])
        rows, schema = session.execute(sql.SQL("SET search_path TO x"), fetch=False)
        assert rows == []
        assert len(schema) == 0

    def test_wraps_psycopg_errors(self):
        import psycopg
        from hologres.exceptions import HoloClientException
        from psycopg import sql

        class RaisingConn(FakeConnection):
            def cursor(self, name=None):
                raise psycopg.errors.SyntaxError("bad sql")

        session = _session_from_connection(RaisingConn(), dbname="x")
        with pytest.raises(ExecutionError) as excinfo:
            session.execute(sql.SQL("SELECT bogus"))
        assert "bad sql" in str(excinfo.value)
        # The chain must point at the classified HoloClientException, not the
        # raw psycopg exception -- otherwise __cause__ contradicts .cause.
        assert isinstance(excinfo.value.cause, HoloClientException)
        assert excinfo.value.__cause__ is excinfo.value.cause

    def test_wraps_non_psycopg_errors(self):
        from psycopg import sql

        class RaisingConn(FakeConnection):
            def cursor(self, name=None):
                raise RuntimeError("something else broke")

        session = _session_from_connection(RaisingConn(), dbname="x")
        with pytest.raises(ExecutionError, match="something else broke"):
            session.execute(sql.SQL("SELECT 1"))


class TestExecuteStream:
    def test_streams_rows_with_a_plain_cursor(self):
        from psycopg import sql

        session, conn = make_session(
            script=[
                (
                    [(1,), (2,), (3,)],
                    [FakeColumnInfo("n", OID_INT4)],
                )
            ]
        )
        rows = list(session.execute_stream(sql.SQL("SELECT n FROM t"), batch_size=2))
        assert [r[0] for r in rows] == [1, 2, 3]
        # Hologres refuses DECLARE CURSOR outside a transaction, so streaming
        # batches client-side is the only workable path.
        assert conn.cursors[0].name is None

    def test_wraps_stream_errors(self):
        import psycopg
        from psycopg import sql

        class RaisingConn(FakeConnection):
            def cursor(self, name=None):
                raise psycopg.errors.SyntaxError("stream boom")

        session = _session_from_connection(RaisingConn(), dbname="x")
        with pytest.raises(ExecutionError, match="stream boom"):
            list(session.execute_stream(sql.SQL("SELECT 1")))

    def test_execute_stream_after_close_rejected_eagerly(self):
        from psycopg import sql

        session, _ = make_session()
        session.close()
        # Eager check: no need to call next() before the error surfaces.
        with pytest.raises(InvalidArgumentError, match="closed"):
            session.execute_stream(sql.SQL("SELECT 1"))


class TestTableAndSchema:
    def test_table_pins_schema_from_information_schema(self):
        session, conn = make_session(
            script=[
                (
                    [
                        ("id", "bigint", "int8"),
                        ("name", "text", "text"),
                        ("amount", "double precision", "float8"),
                    ],
                    [
                        FakeColumnInfo("column_name", OID_TEXT),
                        FakeColumnInfo("data_type", OID_TEXT),
                        FakeColumnInfo("udt_name", OID_TEXT),
                    ],
                )
            ]
        )
        df = session.table("public.orders")
        assert df.schema.names == ("id", "name", "amount")
        assert df.schema["id"].datatype == hg.LongType()
        assert df.schema["amount"].datatype == hg.DoubleType()
        # information_schema query should have been executed.
        assert "information_schema.columns" in conn.recorded[0]

    def test_table_unknown_raises(self):
        session, _ = make_session(script=[([], [])])
        with pytest.raises(ExecutionError, match="does not exist"):
            session.table("public.missing")

    def test_table_array_type(self):
        session, _ = make_session(
            script=[
                (
                    [("tags", "ARRAY", "_text")],
                    [
                        FakeColumnInfo("column_name", OID_TEXT),
                        FakeColumnInfo("data_type", OID_TEXT),
                        FakeColumnInfo("udt_name", OID_TEXT),
                    ],
                )
            ]
        )
        df = session.table("t")
        assert df.schema["tags"].datatype == hg.ArrayType(hg.StringType())

    def test_table_unknown_type_falls_back_to_text(self):
        session, _ = make_session(
            script=[
                (
                    [("weird", "unknown-thing", "unknown-udt")],
                    [
                        FakeColumnInfo("column_name", OID_TEXT),
                        FakeColumnInfo("data_type", OID_TEXT),
                        FakeColumnInfo("udt_name", OID_TEXT),
                    ],
                )
            ]
        )
        df = session.table("t")
        assert df.schema["weird"].datatype == hg.StringType()

    def test_sql_source_has_no_pinned_schema(self):
        session, _ = make_session()
        df = session.sql("SELECT 1")
        # Not pinned; a describe would be needed. We don't trigger it here.
        assert df._schema is None

    def test_describe_wraps_query_with_limit_zero(self):
        from psycopg import sql

        session, conn = make_session(
            script=[
                (
                    [],
                    [FakeColumnInfo("x", OID_INT4)],
                )
            ]
        )
        schema = session.describe(sql.SQL("SELECT * FROM t"))
        assert schema.names == ("x",)
        assert "LIMIT 0" in conn.recorded[0]


class TestCatalog:
    def test_use_schema_sets_search_path(self):
        from psycopg import sql

        session, conn = make_session(script=[([], [])])
        session._capability_cache[("sql_function", "ai_gen")] = True
        session.use_schema("analytics")
        assert conn.recorded == []
        session.execute(sql.SQL("SELECT 1"))
        assert conn.recorded[0] == 'SET search_path TO "analytics"'
        assert conn.recorded[1] == "SELECT 1"
        assert conn.recorded[2] == "RESET search_path"
        assert session._capability_cache == {}

    @pytest.mark.parametrize("bad", ["", None, 42])
    def test_use_schema_rejects_bad_name(self, bad):
        session, _ = make_session()
        with pytest.raises(InvalidArgumentError, match="non-empty"):
            session.use_schema(bad)

    def test_use_schema_applies_to_unqualified_table_names(self):
        session, conn = make_session(
            script=[
                (
                    [("id", "bigint", "int8")],
                    [
                        FakeColumnInfo("column_name", OID_TEXT),
                        FakeColumnInfo("data_type", OID_TEXT),
                        FakeColumnInfo("udt_name", OID_TEXT),
                    ],
                )
            ]
        )
        session.use_schema("analytics")

        frame = session.table("orders")

        assert frame._plan.table_name.schema_name == "analytics"
        assert "'analytics'" in conn.recorded[1]
        assert conn.recorded[-1] == "RESET search_path"

    def test_current_schema_returns_first_value(self):
        session, _ = make_session(
            script=[([("public",)], [FakeColumnInfo("current_schema", OID_TEXT)])]
        )
        assert session.current_schema() == "public"

    def test_current_schema_defaults_public_when_empty(self):
        session, _ = make_session(script=[([], [])])
        assert session.current_schema() == "public"

    def test_list_schemas(self):
        session, conn = make_session(
            script=[
                (
                    [("public",), ("analytics",)],
                    [FakeColumnInfo("schema_name", OID_TEXT)],
                )
            ]
        )
        assert session.list_schemas() == ["public", "analytics"]
        assert "information_schema.schemata" in conn.recorded[0]

    def test_list_tables_current_schema(self):
        session, conn = make_session(
            script=[
                ([("public",)], [FakeColumnInfo("current_schema", OID_TEXT)]),
                (
                    [("orders",), ("users",)],
                    [FakeColumnInfo("table_name", OID_TEXT)],
                ),
            ]
        )
        assert session.list_tables() == ["orders", "users"]

    def test_list_tables_explicit_schema(self):
        session, conn = make_session(
            script=[
                (
                    [("orders",)],
                    [FakeColumnInfo("table_name", OID_TEXT)],
                )
            ]
        )
        assert session.list_tables(schema_name="my_schema") == ["orders"]
        # No current_schema() lookup happened.
        assert len(conn.recorded) == 1
        assert "'my_schema'" in conn.recorded[0]

    def test_current_database_returns_value(self):
        session, _ = make_session(
            script=[([("prod",)], [FakeColumnInfo("current_database", OID_TEXT)])]
        )
        assert session.current_database() == "prod"

    def test_current_database_defaults_to_dbname_when_empty(self):
        session, _ = make_session(script=[([], [])])
        assert session.current_database() == "test_db"

    def test_use_database_reconnects_in_place(self):
        session, opened = make_reconnectable_session(
            {"db1": [([], [])], "db2": [([("db2",)], [FakeColumnInfo("d", OID_TEXT)])]},
            dbname="db1",
        )
        old_pool = session._pool
        session.use_database("db2")
        assert session._dbname == "db2"
        # The old pool was closed and a new one installed.
        assert old_pool._closed is True
        assert session._pool is opened[1]
        assert session._pool._closed is False
        # Subsequent queries run against the new database's script.
        assert session.current_database() == "db2"

    def test_use_database_refreshes_conninfo(self):
        from psycopg.conninfo import conninfo_to_dict

        session, _ = make_reconnectable_session(
            {"db1": [([], [])], "db2": [([], [])]}, dbname="db1"
        )
        session._conninfo = "host=h port=80 dbname=db1 user=u password=p"

        session.use_database("db2")

        assert conninfo_to_dict(session._conninfo)["dbname"] == "db2"

    def test_use_database_cleans_old_database_objects_before_switching(self):
        from hologres.table_name import TableName

        session, opened = make_reconnectable_session(
            {"db1": [([], [])], "db2": [([], [])]}, dbname="db1"
        )
        session._server_version = (3, 1, 0)
        session._register_scratch_table(TableName.valueOf("public.old_scratch"))

        session.use_database("db2")

        assert any(
            'DROP TABLE IF EXISTS "public"."old_scratch" FORCE' in query
            for query in opened[0]._conn.recorded
        )
        assert session._scratch_tables == []

    def test_use_database_stays_put_when_old_objects_cannot_be_cleaned(self):
        from hologres.table_name import TableName

        session, opened = make_reconnectable_session(
            {"db1": [([], [])], "db2": [([], [])]}, dbname="db1"
        )
        session._server_version = (3, 1, 0)
        session._register_scratch_table(TableName.valueOf("public.old_scratch"))

        def fail_cursor(*args, **kwargs):
            raise RuntimeError("cleanup failed")

        opened[0]._conn.cursor = fail_cursor

        with pytest.raises(ExecutionError, match="scratch objects"):
            session.use_database("db2")
        assert session._dbname == "db1"
        assert len(opened) == 1
        assert len(session._scratch_tables) == 1

    def test_use_database_same_name_is_a_noop(self):
        session, opened = make_reconnectable_session({"db1": [([], [])]}, dbname="db1")
        session.use_database("db1")
        # No new pool built, current one untouched.
        assert len(opened) == 1
        assert opened[0]._closed is False

    def test_use_database_without_factory_rejected(self):
        session, _ = make_session()
        with pytest.raises(InvalidArgumentError, match="created via hg.connect"):
            session.use_database("other")

    @pytest.mark.parametrize("bad", ["", None, 42])
    def test_use_database_rejects_bad_name(self, bad):
        session, _ = make_reconnectable_session({"db1": [([], [])]}, dbname="db1")
        with pytest.raises(InvalidArgumentError, match="non-empty"):
            session.use_database(bad)

    def test_use_database_on_closed_session_rejected(self):
        session, _ = make_reconnectable_session({"db1": [([], [])]}, dbname="db1")
        session.close()
        with pytest.raises(InvalidArgumentError, match="closed"):
            session.use_database("db2")

    def test_use_database_keeps_old_pool_when_factory_fails(self):
        session, _ = make_reconnectable_session({"db1": [([], [])]}, dbname="db1")
        old_pool = session._pool

        def boom(_db):
            raise RuntimeError("cannot open")

        session._pool_factory = boom
        with pytest.raises(ExecutionError):
            session.use_database("db2")
        # The session still uses the untouched original pool.
        assert session._pool is old_pool
        assert old_pool._closed is False
        assert session._dbname == "db1"


class TestRepr:
    def test_repr_shows_state(self):
        session, _ = make_session()
        assert "test_db" in repr(session)
        assert "open" in repr(session)
        session.close()
        assert "closed" in repr(session)


class TestUnknownOid:
    """The internal _oid_to_type_name fallback for OIDs we don't recognize."""

    def test_fetches_unknown_oid_as_text(self):
        from hologres.dataframe.session import _oid_to_type_name

        # 999999 is not a real OID.
        assert _oid_to_type_name(999999) == "text"

    def test_known_int_oid(self):
        from hologres.dataframe.session import _oid_to_type_name

        assert _oid_to_type_name(OID_INT4) == "int4"

    def test_array_oid_becomes_bracketed(self):
        from hologres.dataframe.session import _oid_to_type_name

        # OID 1007 is _int4 (integer array).
        assert _oid_to_type_name(1007) == "int4[]"

    def test_description_with_odd_type_still_parses(self):
        from psycopg import sql

        # Send an OID that resolves to something parse_type won't know.
        session, _ = make_session(
            script=[([("x",)], [FakeColumnInfo("v", 1560)])]  # bit
        )
        rows, schema = session.execute(sql.SQL("SELECT v FROM t"))
        # Falls back to text; parse still succeeds.
        assert schema["v"].datatype == hg.StringType()


class TestFacade:
    def test_session_and_connect_reachable(self):
        assert hg.Session is Session
        assert hg.connect is connect
        assert "Session" in hg.__all__
        assert "connect" in hg.__all__
