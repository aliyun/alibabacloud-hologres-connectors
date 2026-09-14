"""Tests for client-side gating of version-sensitive Hologres features."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from hologres.table_name import TableName
from psycopg import sql

import hologres.dataframe as hg
from hologres.dataframe import _capabilities, _expr, _plan
from hologres.dataframe.dataframe import DataFrame
from hologres.dataframe.exceptions import UnsupportedOperationError
from hologres.dataframe.types import (
    ArrayType,
    LongType,
    StructField,
    StructType,
    VectorType,
)
from tests._fake_conn import OID_BOOL, OID_TEXT, FakeColumnInfo, make_session


def _df(session):
    return DataFrame(
        _plan.TableScan(TableName.valueOf("public.source")),
        session=session,
        schema=StructType([StructField("id", LongType())]),
    )


def _scripted_session(script=()):
    session, conn = make_session(script=script)
    session._server_version = (5, 1, 0)
    return session, conn


def test_external_files_rejects_old_server_before_sql():
    session, conn = make_session()
    session._server_version = (4, 0, 12)
    frame = session.read_files("oss://bucket/data.csv", format="csv")

    with pytest.raises(UnsupportedOperationError, match="Hologres 4.1.0"):
        frame.columns
    assert conn.recorded == []


def test_write_files_rejects_old_server_before_source_compilation():
    session, conn = make_session()
    session._server_version = (4, 0, 12)

    with pytest.raises(UnsupportedOperationError, match="write_files"):
        _df(session).write_files("oss://bucket/export/")
    assert conn.recorded == []


def test_dynamic_table_rejects_disabled_driver_interface():
    session, conn = _scripted_session(
        [([("off",)], [FakeColumnInfo("setting", OID_TEXT)])]
    )

    with pytest.raises(UnsupportedOperationError, match="driver-based"):
        _df(session).as_dynamic_table("public.result", freshness="1 minute")
    assert len(conn.recorded) == 1
    assert "CREATE DYNAMIC TABLE" not in conn.recorded[0]


def test_dynamic_table_accepts_backport_when_driver_interface_is_enabled():
    session, conn = make_session(
        script=[
            ([("on",)], [FakeColumnInfo("setting", OID_TEXT)]),
            ([], []),
        ]
    )
    session._server_version = (4, 0, 0)

    _df(session).as_dynamic_table("public.result", freshness="1 minute")
    assert conn.recorded[-1].startswith("CREATE DYNAMIC TABLE")


def test_remote_udf_rejects_missing_function_compute_before_deploy():
    session, conn = _scripted_session(
        [([(False,)], [FakeColumnInfo("exists", OID_BOOL)])]
    )
    session._fc_config = {
        "endpoint": "123.cn-hangzhou-internal.fc.aliyuncs.com",
        "region": "cn-hangzhou",
    }

    @hg.udf(name="capability_test_udf")
    def capability_test_udf(value: int) -> str:
        return str(value)

    with pytest.raises(UnsupportedOperationError, match="function_compute"):
        _df(session).select(capability_test_udf("id")).collect()
    assert len(conn.recorded) == 1


def test_ai_function_is_checked_but_generic_function_is_not():
    missing, conn = _scripted_session(
        [([(False,)], [FakeColumnInfo("exists", OID_BOOL)])]
    )
    with pytest.raises(UnsupportedOperationError, match="ai_gen"):
        _df(missing).select(hg.ai.gen("id").alias("answer")).collect()
    assert len(conn.recorded) == 1

    generic, generic_conn = _scripted_session([([], [])])
    _df(generic).select(hg.function("ai_gen", hg.col("id")).alias("answer")).collect()
    assert len(generic_conn.recorded) == 1
    assert generic_conn.recorded[0].startswith("SELECT ai_gen")


def test_vector_expression_is_checked_before_query():
    session, conn = _scripted_session(
        [([(False,)], [FakeColumnInfo("exists", OID_BOOL)])]
    )
    frame = _df(session).select(
        hg.col("id").cast(VectorType(float, 3)).alias("embedding")
    )

    with pytest.raises(UnsupportedOperationError, match="pgvector"):
        frame.collect()
    assert len(conn.recorded) == 1


def test_resolved_stage_is_still_checked_on_later_actions():
    session, conn = make_session()
    session._server_version = (4, 0, 0)
    schema = StructType([StructField("id", LongType())])
    pending = _plan.PendingSource(lambda: _plan.StageScan("stage", schema), schema)
    pending.resolved = _plan.StageScan("stage", schema)

    with pytest.raises(UnsupportedOperationError, match="Hologres 4.1.0"):
        DataFrame(pending, session=session, schema=schema).collect()
    assert conn.recorded == []


def test_unresolved_source_is_gated_before_loader_runs():
    session, conn = make_session()
    session._server_version = (4, 0, 0)
    schema = StructType([StructField("id", LongType())])
    loaded = []

    def load():
        loaded.append(True)
        return _plan.StageScan("stage", schema)

    pending = _plan.PendingSource(
        load,
        schema,
        requirements=(("capability", "external_files"),),
    )

    with pytest.raises(UnsupportedOperationError, match="Hologres 4.1.0"):
        DataFrame(pending, session=session, schema=schema).collect()
    assert loaded == []
    assert conn.recorded == []


def test_object_and_dynamic_refresh_validate_object_type():
    object_session, object_conn = _scripted_session(
        [([(False,)], [FakeColumnInfo("exists", OID_BOOL)])]
    )
    with pytest.raises(UnsupportedOperationError, match="Object Table"):
        object_session.object_table("public.regular", refresh=False)
    assert not any("REFRESH OBJECT TABLE" in query for query in object_conn.recorded)

    dynamic_session, dynamic_conn = _scripted_session(
        [
            ([("on",)], [FakeColumnInfo("setting", OID_TEXT)]),
            ([(False,)], [FakeColumnInfo("exists", OID_BOOL)]),
        ]
    )
    with pytest.raises(UnsupportedOperationError, match="Dynamic Table"):
        dynamic_session.refresh_table("public.regular")
    assert not any("REFRESH TABLE" in query for query in dynamic_conn.recorded)


def test_unknown_binlog_gap_setting_is_rejected():
    session, conn = _scripted_session(
        [([(None,)], [FakeColumnInfo("setting", OID_TEXT)])]
    )

    with pytest.raises(UnsupportedOperationError, match="generate_binlog"):
        session._require_capability("binlog_gap_guc")
    assert len(conn.recorded) == 1


def test_capability_scan_handles_nested_types_values_and_expression_nodes():
    required = _expr.FunctionCall(
        "ai_gen", [_expr.Literal("x")], required_capability="ai_gen"
    )
    expressions = [
        _expr.BinaryExpr(required, "+", _expr.Literal("y")),
        _expr.UnaryExpr("NOT", required),
        _expr.PredicateExpr(sql.SQL("({} IS NOT NULL)"), [required]),
        _expr.CastExpr(required, "text"),
        _expr.SortExpr(required),
        _expr.WindowExpr("first_value", [required], [_expr.Literal(1)]),
    ]
    for expression in expressions:
        requirements = list(_capabilities._requirements_in_expression(expression))
        assert ("sql_function", "ai_gen") in requirements

    node = _plan.Project(
        _plan.InlineValues(
            ["embedding"],
            [([0.1, 0.2],)],
            [ArrayType(VectorType(float, 2))],
        ),
        [hg.col("embedding")],
    )
    requirements = list(_capabilities.iter_plan_requirements(node))
    assert requirements == [("capability", "vector_type")]
    assert _capabilities._contains_vector(
        StructType([StructField("embedding", VectorType(float, 2))])
    )

    value_requirements = list(
        _capabilities._requirements_in_value({"nested": hg.ai.gen(hg.lit("x"))})
    )
    assert value_requirements == [("sql_function", "ai_gen")]


def test_capability_scan_handles_string_slots():
    class StringSlotPlan(_plan.LogicalPlan):
        __slots__ = "value"

        def __init__(self):
            self.value = "plain"

    assert list(_capabilities._plan_values(StringSlotPlan())) == ["plain"]


def test_capability_probe_errors_and_cached_sql_function():
    session, conn = _scripted_session(
        [([(True,)], [FakeColumnInfo("exists", OID_BOOL)])]
    )
    session._require_sql_function("ai_gen")
    session._require_sql_function("ai_gen")
    assert len(conn.recorded) == 1

    failing, _ = _scripted_session()

    with patch.object(
        type(failing), "execute", side_effect=hg.ExecutionError("probe failed")
    ):
        with pytest.raises(UnsupportedOperationError, match="capability probe failed"):
            failing._require_capability("dynamic_table")

    with pytest.raises(hg.InvalidArgumentError, match="unknown internal capability"):
        session._require_capability("not_a_capability")
