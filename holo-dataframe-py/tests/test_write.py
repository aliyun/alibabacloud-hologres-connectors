"""Offline tests for DataFrame.write and its transactional execution helper."""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from hologres.table_name import TableName
from psycopg import sql

import hologres.dataframe as hg
from hologres.dataframe import _plan, _write
from hologres.dataframe.dataframe import DataFrame
from hologres.dataframe.exceptions import (
    ExecutionError,
    InvalidArgumentError,
    UnsupportedOperationError,
)
from hologres.dataframe.session import _session_from_connection
from hologres.dataframe.types import LongType, StringType, StructField, StructType
from tests._fake_conn import FakeConnection, FakeCursor


def _schema(*names):
    fields = []
    for name in names:
        datatype = LongType() if name == "id" else StringType()
        fields.append(StructField(name, datatype))
    return StructType(fields)


class WriteSession:
    """Minimal Session-shaped object with scriptable write metadata."""

    def __init__(
        self,
        *,
        version=(5, 1, 0),
        target_schema=None,
        primary_keys=(),
        partition_info=None,
        table_properties=(),
        binlog_query_error=False,
    ):
        self.version = version
        self.target_schema = target_schema or _schema("id", "name")
        self.primary_keys = tuple(primary_keys)
        self.partition_info = partition_info
        self.table_properties = list(table_properties)
        self.binlog_query_error = binlog_query_error
        self.executed = []
        self.transaction_calls = []
        self.capability_calls = []

    def _require_capability(self, name, *, operation=None):
        self.capability_calls.append((name, operation))

    def _fetch_table_schema(self, schema_name, table_name):
        if table_name == "missing":
            raise ExecutionError("table does not exist")
        return self.target_schema

    def server_version(self):
        return self.version

    def execute(self, query, *, fetch=True, _execution_options=None):
        text = query.as_string() if hasattr(query, "as_string") else str(query)
        if "FROM pg_index" in text:
            return [(key,) for key in self.primary_keys], StructType()
        if "pg_partitioned_table" in text:
            rows = [] if self.partition_info is None else [self.partition_info]
            return rows, StructType()
        if "hg_table_properties" in text:
            if self.binlog_query_error:
                raise ExecutionError("metadata unavailable")
            return list(self.table_properties), StructType()
        self.executed.append(text)
        if _execution_options is not None:
            self.execution_options = _execution_options
        return [], StructType()

    def _execute_write_transaction(
        self,
        statements,
        *,
        prelude=(),
        cleanup=(),
        allow_binlog_gap=False,
        execution_options=None,
    ):
        def rendered(items):
            return [item.as_string() for item in items]

        call = {
            "statements": rendered(statements),
            "prelude": rendered(prelude),
            "cleanup": rendered(cleanup),
            "allow_binlog_gap": allow_binlog_gap,
        }
        if execution_options is not None:
            call["execution_options"] = execution_options
        self.transaction_calls.append(call)


def _table_df(session, table="public.source", schema=None):
    schema = schema or _schema("id", "name")
    plan = _plan.TableScan(TableName.valueOf(table))
    return DataFrame(plan, session=session, schema=schema)


class TestAppend:
    def test_inline_values_use_insert_sql(self):
        session = WriteSession()
        df = hg.from_dict({"id": [1, 2], "name": ["a", "b"]}, session=session)

        assert df.write("public.target") is None

        query = session.executed[-1]
        assert query.startswith('INSERT INTO "public"."target" ("id", "name")')
        assert "VALUES" in query
        assert "HoloClient" not in query

    def test_server_query_uses_insert_select(self):
        session = WriteSession()
        _table_df(session).filter(hg.col("id") > 1).write("public.target")
        query = session.executed[-1]
        assert 'INSERT INTO "public"."target"' in query
        assert 'SELECT * FROM "public"."source" WHERE ("id" > 1)' in query

    def test_serverless_options_reach_append_execution(self):
        session = WriteSession()
        df = _table_df(session).serverless(priority=4)

        df.write("public.target")

        assert session.execution_options.priority == 4

    def test_target_must_have_every_source_column(self):
        session = WriteSession(target_schema=_schema("id"))
        with pytest.raises(InvalidArgumentError, match="missing DataFrame column"):
            _table_df(session).write("public.target")

    def test_duplicate_output_columns_rejected(self):
        with pytest.raises(InvalidArgumentError, match="duplicate columns"):
            _write._validate_columns(["id", "id"], ["id"])

    def test_missing_target_propagates_execution_error(self):
        session = WriteSession()
        with pytest.raises(ExecutionError, match="does not exist"):
            _table_df(session).write("public.missing")


class TestOnConflict:
    @pytest.mark.parametrize(
        ("action", "expected"),
        [
            ("ignore", "DO NOTHING"),
            ("update", 'DO UPDATE SET "name" = excluded."name"'),
            ("replace", 'DO UPDATE SET "name" = excluded."name"'),
        ],
    )
    def test_conflict_actions(self, action, expected):
        session = WriteSession(primary_keys=("id",))
        _table_df(session).write("public.target", on_conflict=(action, ["id"]))
        query = session.executed[-1]
        assert 'ON CONFLICT ("id")' in query
        assert expected in query

    def test_replace_assigns_target_columns_not_present_in_source(self):
        session = WriteSession(
            target_schema=_schema("id", "name", "note"), primary_keys=("id",)
        )
        _table_df(session).write("public.target", on_conflict=("replace", ["id"]))
        query = session.executed[-1]
        assert '("id", "name", "note") SELECT "id", "name", NULL AS "note"' in query
        assert '"note" = excluded."note"' in query

    def test_only_primary_key_falls_back_to_do_nothing(self):
        session = WriteSession(target_schema=_schema("id"), primary_keys=("id",))
        _table_df(session, schema=_schema("id")).write(
            "public.target", on_conflict=("update", ["id"])
        )
        assert session.executed[-1].endswith("DO NOTHING")

    def test_key_set_must_match_primary_key(self):
        session = WriteSession(primary_keys=("id", "tenant"))
        with pytest.raises(InvalidArgumentError, match="must match target primary key"):
            _table_df(session).write("public.target", on_conflict=("update", ["id"]))

    def test_target_without_primary_key_rejected(self):
        session = WriteSession()
        with pytest.raises(InvalidArgumentError, match="requires a target primary key"):
            _table_df(session).write("public.target", on_conflict=("ignore", ["id"]))

    def test_conflict_key_must_be_in_source(self):
        session = WriteSession(primary_keys=("id",))
        with pytest.raises(InvalidArgumentError, match="missing on_conflict key"):
            _table_df(session, schema=_schema("name")).write(
                "public.target", on_conflict=("ignore", ["id"])
            )


class TestNativeOverwrite:
    def test_non_partition_table_uses_insert_overwrite(self):
        session = WriteSession(table_properties=[])
        _table_df(session).write("public.target", overwrite=True)
        assert session.executed[-1].startswith(
            'INSERT OVERWRITE "public"."target" ("id", "name") SELECT'
        )

    def test_allow_gap_uses_transaction_helper(self):
        session = WriteSession()
        _table_df(session).write("public.target", overwrite=True, allow_binlog_gap=True)
        assert session.capability_calls == [
            ("binlog_gap_guc", "write(allow_binlog_gap=True)")
        ]
        call = session.transaction_calls[-1]
        assert call["allow_binlog_gap"] is True
        assert call["statements"][0].startswith("INSERT OVERWRITE")
        assert session.executed == []

    def test_serverless_options_reach_native_overwrite_transaction(self):
        session = WriteSession()
        df = _table_df(session).serverless(required_cores=64)

        df.write("public.target", overwrite=True, allow_binlog_gap=True)

        options = session.transaction_calls[-1]["execution_options"]
        assert options.required_cores == 64

    def test_binlog_table_requires_explicit_acknowledgement(self):
        session = WriteSession(table_properties=[("binlog.level", "replica")])
        with pytest.raises(UnsupportedOperationError, match="Binlog enabled"):
            _table_df(session).write("public.target", overwrite=True)

    @pytest.mark.parametrize(
        ("properties", "expected"),
        [
            ([("orientation", "column"), ("binlog.level", "on")], True),
            ([("binlog.level", "off")], False),
            ([("binlog.level", "replica")], True),
            ([], False),
        ],
    )
    def test_binlog_property_variants(self, properties, expected):
        assert _write._property_enables_binlog(properties) is expected

    def test_unknown_partition_metadata_blocks_overwrite(self):
        session = WriteSession(binlog_query_error=True)
        with pytest.raises(ExecutionError, match="metadata unavailable"):
            _table_df(session).write("public.target", overwrite=True)
        assert session.executed == []
        assert session.transaction_calls == []

    def test_logical_partition_clause(self):
        session = WriteSession(
            target_schema=_schema("id", "name", "dt"),
            table_properties=[
                ("is_logical_partitioned_table", "true"),
                ("logical_partition_columns", "dt"),
            ],
        )
        df = _table_df(session, schema=_schema("id", "name", "dt"))
        df.write(
            "public.target",
            overwrite=True,
            partition={"dt": "2026-08-17"},
        )
        assert "PARTITION (\"dt\" = '2026-08-17')" in session.executed[-1]

    def test_logical_partition_requires_partition_argument(self):
        session = WriteSession(
            target_schema=_schema("id", "name", "dt"),
            table_properties=[
                ("is_logical_partitioned_table", "true"),
                ("logical_partition_columns", "dt"),
            ],
        )
        with pytest.raises(UnsupportedOperationError, match="require partition"):
            _table_df(session, schema=_schema("id", "name", "dt")).write(
                "public.target", overwrite=True
            )

    def test_partition_key_must_match(self):
        session = WriteSession(
            target_schema=_schema("id", "name", "dt"),
            table_properties=[
                ("is_logical_partitioned_table", "true"),
                ("logical_partition_columns", "dt"),
            ],
        )
        with pytest.raises(InvalidArgumentError, match="must match"):
            _table_df(session, schema=_schema("id", "name", "dt")).write(
                "public.target", overwrite=True, partition={"ds": "x"}
            )

    def test_partition_rejected_for_non_partition_table(self):
        session = WriteSession(target_schema=_schema("id", "name", "dt"))
        with pytest.raises(InvalidArgumentError, match="logical partition tables"):
            _table_df(session, schema=_schema("id", "name", "dt")).write(
                "public.target", overwrite=True, partition={"dt": "x"}
            )

    def test_physical_parent_rejected(self):
        session = WriteSession(
            target_schema=_schema("id", "name", "dt"),
            partition_info=("l", "dt", True),
        )
        with pytest.raises(
            UnsupportedOperationError, match="physical partition parent"
        ):
            _table_df(session, schema=_schema("id", "name", "dt")).write(
                "public.target", overwrite=True, partition={"dt": "x"}
            )

    def test_physical_parent_without_partition_rejected_by_native_builder(self):
        session = WriteSession(partition_info=("l", "dt", False))
        with pytest.raises(
            UnsupportedOperationError, match="physical partition parent"
        ):
            _table_df(session).write("public.target", overwrite=True)

    def test_physical_parent_without_partition_rejected_on_old_server(self):
        session = WriteSession(version=(3, 0, 9), partition_info=("l", "dt", False))
        with pytest.raises(
            UnsupportedOperationError, match="physical partition parent"
        ):
            _table_df(session).write("public.target", overwrite=True)
        assert session.transaction_calls == []

    def test_partition_column_must_be_in_source_output(self):
        session = WriteSession(
            target_schema=_schema("id", "name", "dt"),
            table_properties=[
                ("is_logical_partitioned_table", "true"),
                ("logical_partition_columns", "dt"),
            ],
        )
        with pytest.raises(InvalidArgumentError, match="missing partition column"):
            _table_df(session).write(
                "public.target", overwrite=True, partition={"dt": "x"}
            )


class TestLegacyOverwrite:
    def test_full_table_uses_one_transaction(self):
        session = WriteSession(version=(3, 0, 9))
        _table_df(session).write("public.target", overwrite=True)
        call = session.transaction_calls[-1]
        assert call["statements"][0] == 'TRUNCATE TABLE "public"."target"'
        assert call["statements"][1].startswith('INSERT INTO "public"."target"')

    def test_serverless_options_reach_legacy_overwrite_transaction(self):
        session = WriteSession(version=(3, 0, 9))
        df = _table_df(session).serverless(max_cores=128)

        df.write("public.target", overwrite=True)

        options = session.transaction_calls[-1]["execution_options"]
        assert options.max_cores == 128

    def test_partition_fallback_deletes_and_filters(self):
        session = WriteSession(
            version=(3, 0, 9),
            target_schema=_schema("id", "name", "dt"),
            table_properties=[
                ("is_logical_partitioned_table", "true"),
                ("logical_partition_columns", "dt"),
            ],
        )
        df = _table_df(session, schema=_schema("id", "name", "dt"))
        df.write("public.target", overwrite=True, partition={"dt": "x"})
        call = session.transaction_calls[-1]
        assert call["statements"][0] == (
            'DELETE FROM "public"."target" WHERE "dt" = \'x\''
        )
        assert "__hg_partition_source" in call["statements"][1]

    def test_self_reference_materializes_before_truncate(self):
        session = WriteSession(version=(3, 0, 9))
        _table_df(session, table="public.target").write("public.target", overwrite=True)
        call = session.transaction_calls[-1]
        assert call["prelude"][0].startswith("CREATE TEMP TABLE")
        assert 'FROM "public"."target"' in call["prelude"][0]
        assert "__hg_write_source_" in call["statements"][1]
        assert call["cleanup"][0].startswith("DROP TABLE IF EXISTS")

    def test_raw_sql_is_materialized_before_truncate(self):
        session = WriteSession(version=(3, 0, 9))
        df = DataFrame(
            _plan.RawSql("SELECT id, name FROM public.target"),
            session=session,
            schema=_schema("id", "name"),
        )
        df.write("public.target", overwrite=True)
        call = session.transaction_calls[-1]
        assert call["prelude"][0].startswith("CREATE TEMP TABLE")
        assert "SELECT id, name FROM public.target" in call["prelude"][0]
        assert "__hg_write_source_" in call["statements"][1]

    def test_resolved_pending_source_is_checked_for_self_reference(self):
        session = WriteSession(version=(3, 0, 9))
        pending = _plan.PendingSource(lambda: None, _schema("id", "name"))
        pending.resolved = _plan.TableScan(TableName.valueOf("public.target"))
        df = DataFrame(pending, session=session, schema=_schema("id", "name"))
        df.write("public.target", overwrite=True)
        call = session.transaction_calls[-1]
        assert call["prelude"][0].startswith("CREATE TEMP TABLE")
        assert 'FROM "public"."target"' in call["prelude"][0]

    def test_unknown_server_version_blocks_overwrite(self):
        session = WriteSession(version=(0, 0, 0))
        with pytest.raises(UnsupportedOperationError, match="cannot determine"):
            _table_df(session).write("public.target", overwrite=True)
        assert session.executed == []
        assert session.transaction_calls == []

    def test_partition_restriction_rejects_missing_column_defensively(self):
        with pytest.raises(InvalidArgumentError, match="missing partition column"):
            _write._restrict_source_to_partition(
                sql.SQL("SELECT 1"),
                ["id"],
                (("dt", "x"),),
                low_version=True,
            )


class TestArgumentValidation:
    @pytest.mark.parametrize("table", ["", None, 123])
    def test_table_name(self, table):
        with pytest.raises(InvalidArgumentError, match="non-empty string"):
            _table_df(WriteSession()).write(table)

    def test_invalid_identifier(self):
        with pytest.raises(InvalidArgumentError, match="invalid table name"):
            _table_df(WriteSession()).write("bad-name")

    @pytest.mark.parametrize("name", ["overwrite", "allow_binlog_gap"])
    def test_bool_arguments(self, name):
        kwargs = {name: 1}
        with pytest.raises(InvalidArgumentError, match="must be a bool"):
            _table_df(WriteSession()).write("public.target", **kwargs)

    def test_allow_gap_requires_overwrite(self):
        with pytest.raises(InvalidArgumentError, match="only valid with overwrite"):
            _table_df(WriteSession()).write("public.target", allow_binlog_gap=True)

    def test_overwrite_and_conflict_are_mutually_exclusive(self):
        with pytest.raises(InvalidArgumentError, match="mutually exclusive"):
            _table_df(WriteSession()).write(
                "public.target",
                overwrite=True,
                on_conflict=("ignore", ["id"]),
            )

    @pytest.mark.parametrize(
        "value",
        [
            "bad",
            ("bad", ["id"]),
            ("ignore", []),
            ("ignore", [""]),
            ("ignore", ["id", "id"]),
        ],
    )
    def test_invalid_on_conflict(self, value):
        with pytest.raises(InvalidArgumentError):
            _table_df(WriteSession()).write("public.target", on_conflict=value)

    @pytest.mark.parametrize("partition", [{}, {"": "x"}])
    def test_invalid_partition(self, partition):
        with pytest.raises(InvalidArgumentError):
            _table_df(WriteSession()).write(
                "public.target", overwrite=True, partition=partition
            )

    def test_partition_requires_overwrite(self):
        with pytest.raises(InvalidArgumentError, match="only valid with overwrite"):
            _table_df(WriteSession()).write("public.target", partition={"dt": "x"})

    def test_no_session(self):
        with pytest.raises(InvalidArgumentError, match="no session"):
            _table_df(None).write("public.target")


class TransactionConnection(FakeConnection):
    def __init__(self, fail_on=None):
        super().__init__()
        self.fail_on = fail_on
        self.events = []

    def cursor(self, name=None):
        cursor = TransactionCursor(self, self.recorded, name=name)
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


class TransactionCursor(FakeCursor):
    def execute(self, query, params=None):
        text = query.as_string() if hasattr(query, "as_string") else str(query)
        if self._conn.fail_on and self._conn.fail_on in text:
            raise RuntimeError("write failed")
        super().execute(query, params)


class TestSessionWriteTransaction:
    def test_runs_prelude_transaction_guc_and_cleanup_on_one_connection(self):
        conn = TransactionConnection()
        session = _session_from_connection(conn)
        session._execute_write_transaction(
            [sql.SQL("TRUNCATE t"), sql.SQL("INSERT INTO t VALUES (1)")],
            prelude=[sql.SQL("CREATE TEMP TABLE s (id int)")],
            cleanup=[sql.SQL("DROP TABLE s")],
            allow_binlog_gap=True,
        )
        assert conn.recorded == [
            "CREATE TEMP TABLE s (id int)",
            "SET LOCAL hg_experimental_generate_binlog = off",
            "TRUNCATE t",
            "INSERT INTO t VALUES (1)",
            "DROP TABLE s",
        ]
        assert conn.events == ["begin", "commit"]

    def test_failure_rolls_back_and_still_cleans_up(self):
        conn = TransactionConnection(fail_on="INSERT")
        session = _session_from_connection(conn)
        with pytest.raises(ExecutionError, match="write failed"):
            session._execute_write_transaction(
                [sql.SQL("TRUNCATE t"), sql.SQL("INSERT INTO t VALUES (1)")],
                cleanup=[sql.SQL("DROP TABLE IF EXISTS s")],
            )
        assert conn.events == ["begin", "rollback"]
        assert conn.recorded[-1] == "DROP TABLE IF EXISTS s"
