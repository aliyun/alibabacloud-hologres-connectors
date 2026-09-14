"""Offline tests for transaction-scoped Serverless DataFrame execution."""

from __future__ import annotations

import pytest
from hologres.table_name import TableName

import hologres.dataframe as hg
from hologres.dataframe import _plan
from hologres.dataframe.dataframe import DataFrame
from hologres.dataframe.exceptions import (
    InvalidArgumentError,
    UnsupportedOperationError,
)
from hologres.dataframe.session import _session_from_connection
from hologres.dataframe.types import ArrayType, LongType, StructField, StructType
from tests._fake_conn import OID_INT8, FakeColumnInfo, ServerlessConnection


def _session(script=None, *, fail_on=None):
    conn = ServerlessConnection(script, fail_on=fail_on)
    return _session_from_connection(conn), conn


def _df(session=None, name="public.source"):
    schema = StructType([StructField("id", LongType())])
    return DataFrame(
        _plan.TableScan(TableName.valueOf(name)),
        session=session,
        schema=schema,
    )


class TestServerlessOptions:
    def test_is_lazy_immutable_and_propagates(self):
        session, conn = _session()
        original = _df(session)

        marked = original.serverless(priority=4).filter(hg.col("id") > 0).limit(1)

        assert conn.recorded == []
        assert original._execution_options is None
        assert marked._execution_options.priority == 4
        assert marked._to_sql().as_string().endswith("LIMIT 1")

    def test_grouping_and_explode_preserve_options(self):
        grouped = _df().serverless(priority=3).group_by("id").count()
        array_df = DataFrame(
            _plan.TableScan(TableName.valueOf("public.arrays")),
            schema=StructType([StructField("tags", ArrayType(LongType()))]),
        )

        exploded = array_df.serverless(max_cores=32).explode("tags")

        assert grouped._execution_options.priority == 3
        assert exploded._execution_options.max_cores == 32

    def test_repeated_call_replaces_options_and_preserves_cached_schema(self):
        session, _ = _session()
        df = session.sql("SELECT 1 AS id")
        cached = StructType([StructField("id", LongType())])
        df._cached_schema = cached

        marked = df.serverless(priority=2).serverless(max_cores=32)

        assert marked._execution_options.priority is None
        assert marked._execution_options.max_cores == 32
        assert marked._cached_schema is cached

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"priority": 0}, "priority"),
            ({"priority": 6}, "priority"),
            ({"priority": True}, "priority"),
            ({"required_cores": -1}, "required_cores"),
            ({"required_cores": 102401}, "required_cores"),
            ({"required_cores": 1.5}, "required_cores"),
            ({"max_cores": -1}, "max_cores"),
            ({"max_cores": 102401}, "max_cores"),
            ({"max_cores": False}, "max_cores"),
            (
                {"required_cores": 65, "max_cores": 64},
                "cannot be greater",
            ),
        ],
    )
    def test_rejects_invalid_options(self, kwargs, message):
        with pytest.raises(InvalidArgumentError, match=message):
            _df().serverless(**kwargs)

    def test_zero_core_values_are_preserved(self):
        options = _df().serverless(required_cores=0, max_cores=0)._execution_options
        statements = [item.as_string() for item in options.set_local_statements()]

        assert statements[-2:] == [
            'SET LOCAL "hg_experimental_serverless_computing_required_cores" = 0',
            'SET LOCAL "hg_experimental_serverless_computing_max_cores" = 0',
        ]

    def test_join_and_union_inherit_one_marked_side(self):
        session, _ = _session()
        marked = _df(session, "public.left").serverless(priority=3)
        plain = _df(session, "public.right")

        joined = marked.join(plain, how="cross", lsuffix="_left", rsuffix="_right")
        assert joined._execution_options.priority == 3
        assert plain.union_all(marked)._execution_options.priority == 3

    @pytest.mark.parametrize("operation", ["join", "union"])
    def test_combining_conflicting_options_fails_before_schema_access(self, operation):
        class NoDescribeSession:
            def describe(self, query):
                raise AssertionError("schema lookup must not run")

        session = NoDescribeSession()
        left = DataFrame(_plan.RawSql("SELECT 1 AS id"), session).serverless(priority=1)
        right = DataFrame(_plan.RawSql("SELECT 2 AS id"), session).serverless(
            priority=2
        )

        with pytest.raises(InvalidArgumentError, match="different serverless settings"):
            if operation == "join":
                left.join(right, how="cross")
            else:
                left.union(right)


class TestServerlessExecution:
    def test_collect_sets_all_options_before_query_in_one_transaction(self):
        columns = [FakeColumnInfo("id", OID_INT8)]
        script = [([], [])] * 4 + [([(1,)], columns)]
        session, conn = _session(script)

        rows = (
            _df(session)
            .serverless(priority=4, required_cores=64, max_cores=128)
            .collect()
        )

        assert rows[0].id == 1
        assert conn.recorded == [
            "SET LOCAL hg_computing_resource = 'serverless'",
            'SET LOCAL "hg_experimental_serverless_computing_query_priority" = 4',
            'SET LOCAL "hg_experimental_serverless_computing_required_cores" = 64',
            'SET LOCAL "hg_experimental_serverless_computing_max_cores" = 128',
            'SELECT * FROM "public"."source"',
        ]
        assert conn.events == ["begin", "commit"]

    def test_count_uses_serverless_execution(self):
        session, conn = _session(
            [([], []), ([(7,)], [FakeColumnInfo("count", OID_INT8)])]
        )

        assert _df(session).serverless().count() == 7
        assert conn.recorded[0] == "SET LOCAL hg_computing_resource = 'serverless'"
        assert conn.recorded[1].startswith("SELECT COUNT(*)")

    def test_iter_rows_keeps_transaction_open_until_exhausted(self):
        columns = [FakeColumnInfo("id", OID_INT8)]
        session, conn = _session([([], []), ([(1,), (2,)], columns)])

        rows = list(_df(session).serverless().iter_rows(batch_size=1))

        assert [row.id for row in rows] == [1, 2]
        assert conn.events == ["begin", "commit"]
        assert conn.recorded[-1] == 'SELECT * FROM "public"."source"'

    def test_settings_do_not_leak_to_unmarked_query(self):
        columns = [FakeColumnInfo("id", OID_INT8)]
        session, conn = _session([([], []), ([(1,)], columns), ([(2,)], columns)])

        _df(session).serverless().collect()
        _df(session).collect()

        assert conn.recorded == [
            "SET LOCAL hg_computing_resource = 'serverless'",
            'SELECT * FROM "public"."source"',
            'SELECT * FROM "public"."source"',
        ]
        assert conn.events == ["begin", "commit"]

    def test_failed_setting_prevents_target_query(self):
        session, conn = _session(fail_on="hg_computing_resource")

        with pytest.raises(UnsupportedOperationError, match="serverless"):
            _df(session).serverless().collect()

        assert conn.recorded == ["SET LOCAL hg_computing_resource = 'serverless'"]
        assert conn.events == ["begin", "rollback"]

    def test_failed_stream_setting_prevents_target_query(self):
        session, conn = _session(fail_on="hg_computing_resource")

        with pytest.raises(UnsupportedOperationError, match="serverless"):
            list(_df(session).serverless().iter_rows())

        assert conn.recorded == ["SET LOCAL hg_computing_resource = 'serverless'"]
        assert conn.events == ["begin", "rollback"]

    def test_write_transaction_applies_options_to_every_statement(self):
        session, conn = _session()
        options = _df().serverless(priority=4)._execution_options

        session._execute_write_transaction(
            [_plan_sql("INSERT INTO target SELECT * FROM source")],
            prelude=[_plan_sql("CREATE TEMP TABLE source AS SELECT 1")],
            allow_binlog_gap=True,
            execution_options=options,
        )

        assert conn.recorded == [
            "SET LOCAL hg_computing_resource = 'serverless'",
            'SET LOCAL "hg_experimental_serverless_computing_query_priority" = 4',
            "CREATE TEMP TABLE source AS SELECT 1",
            "SET LOCAL hg_experimental_generate_binlog = off",
            "INSERT INTO target SELECT * FROM source",
        ]
        assert conn.events == ["begin", "commit"]

    def test_write_transaction_setting_failure_is_not_wrapped(self):
        session, conn = _session(fail_on="hg_computing_resource")
        options = _df().serverless()._execution_options

        with pytest.raises(UnsupportedOperationError, match="serverless"):
            session._execute_write_transaction(
                [_plan_sql("INSERT INTO target SELECT 1")],
                execution_options=options,
            )

        assert conn.recorded == ["SET LOCAL hg_computing_resource = 'serverless'"]
        assert conn.events == ["begin", "rollback"]

    def test_unmarked_dataframe_supports_legacy_session_shape(self):
        class LegacySession:
            def execute(self, query, *, fetch=True):
                return [], StructType()

            def execute_stream(self, query, *, batch_size):
                return iter(())

        df = _df(LegacySession())

        assert df.collect() == []
        assert list(df.iter_rows()) == []


def _plan_sql(text):
    from psycopg import sql

    return sql.SQL(text)
