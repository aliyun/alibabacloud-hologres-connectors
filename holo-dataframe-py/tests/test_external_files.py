"""Offline tests for OSS EXTERNAL_FILES reads and writes."""

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
from hologres.dataframe.types import LongType, StringType, StructField, StructType
from tests._fake_conn import OID_INT8, FakeColumnInfo, make_session


def _schema():
    return StructType(
        [StructField("id", LongType()), StructField("name", StringType())]
    )


def _supported_session(script=None):
    session, conn = make_session(script=script)
    session._server_version = (5, 1, 0)
    return session, conn


def _table_df(session):
    return DataFrame(
        _plan.TableScan(TableName.valueOf("public.source")),
        session=session,
        schema=_schema(),
    )


class TestOssReadFiles:
    def test_explicit_schema_compiles_without_execution(self):
        session, conn = _supported_session()
        df = session.read_files(
            "oss://bucket/data/",
            format="csv",
            schema=_schema(),
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            role_arn="acs:ram::123:role/holo-role",
            csv_skip_header=True,
            csv_delimiter=",",
        )

        assert isinstance(df._plan, _plan.ExternalFilesScan)
        assert conn.recorded == []
        assert df.columns == ["id", "name"]
        assert df._to_sql().as_string() == (
            "SELECT * FROM external_files(path='oss://bucket/data/', format='csv', "
            "csv_skip_header='true', csv_delimiter=',', "
            "oss_endpoint='oss-cn-hangzhou-internal.aliyuncs.com', "
            "role_arn='acs:ram::123:role/holo-role') "
            'AS ("id" bigint, "name" text)'
        )

    def test_schema_is_described_lazily_when_omitted(self):
        session, conn = _supported_session(
            script=[([], [FakeColumnInfo("id", OID_INT8)])]
        )
        df = session.read_files(
            "oss://bucket/data.parquet",
            format="parquet",
        )

        assert conn.recorded == []
        assert df.columns == ["id"]
        assert "external_files(" in conn.recorded[0]
        assert " AS (" not in conn.recorded[0]

    def test_source_composes_with_dataframe_operators(self):
        session, _ = _supported_session()
        out = (
            session.read_files("oss://bucket/data.orc", format="orc", schema=_schema())
            .filter(hg.col("id") > 1)
            .select("name")
            ._to_sql()
            .as_string()
        )
        assert out.startswith('SELECT "name" FROM external_files(')
        assert 'WHERE ("id" > 1)' in out

    def test_literal_values_are_escaped(self):
        session, _ = _supported_session()
        out = (
            session.read_files(
                "oss://bucket/a'b.csv",
                format="csv",
                schema=_schema(),
                role_arn="acs:ram::1:role/a'b",
            )
            ._to_sql()
            .as_string()
        )
        assert "a''b.csv" in out
        assert "role/a''b" in out

    def test_schema_deduction_options(self):
        session, _ = _supported_session()
        out = (
            session.read_files(
                "oss://bucket/data/",
                format="parquet",
                schema_deduce_file_num=3,
                schema_deduce_file_order="latest_first",
            )
            ._to_sql()
            .as_string()
        )
        assert "schema_deduce_file_num='3'" in out
        assert "schema_deduce_file_order='latest_first'" in out

    @pytest.mark.parametrize("format", [None, "json", "arrow_v1"])
    def test_invalid_read_format(self, format):
        session, _ = _supported_session()
        with pytest.raises(InvalidArgumentError, match="OSS read format"):
            session.read_files("oss://bucket/data", format=format)

    @pytest.mark.parametrize("path", ["oss://", ["oss://bucket/data.csv"]])
    def test_invalid_oss_path(self, path):
        session, _ = _supported_session()
        with pytest.raises(InvalidArgumentError):
            session.read_files(path, format="csv")

    def test_table_name_is_local_only(self):
        session, _ = _supported_session()
        with pytest.raises(InvalidArgumentError, match="table_name"):
            session.read_files(
                "oss://bucket/data.csv", format="csv", table_name="public.tmp"
            )

    @pytest.mark.parametrize("name", ["oss_endpoint", "role_arn"])
    def test_oss_credentials_require_oss_path(self, name, tmp_path):
        session, _ = _supported_session()
        path = tmp_path / "data.csv"
        path.write_text("id\n1\n")
        with pytest.raises(InvalidArgumentError, match="require an oss:// path"):
            session.read_files(str(path), **{name: "value"})

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"schema": "id bigint"}, "StructType"),
            ({"oss_endpoint": ""}, "oss_endpoint"),
            ({"role_arn": 1}, "role_arn"),
            ({"csv_skip_header": "true"}, "must be a bool"),
            ({"csv_delimiter": "||"}, "one character"),
            ({"schema_deduce_file_num": 0}, "positive int"),
            ({"schema_deduce_file_num": True}, "positive int"),
            ({"schema_deduce_file_order": "newest"}, "latest_first"),
            ({"unknown": 1}, "unknown OSS read option"),
        ],
    )
    def test_invalid_read_options(self, kwargs, message):
        session, _ = _supported_session()
        with pytest.raises(InvalidArgumentError, match=message):
            session.read_files("oss://bucket/data.csv", format="csv", **kwargs)

    def test_csv_options_rejected_for_other_formats(self):
        session, _ = _supported_session()
        with pytest.raises(InvalidArgumentError, match="require format='csv'"):
            session.read_files(
                "oss://bucket/data.parquet",
                format="parquet",
                csv_skip_header=True,
            )


class TestWriteFiles:
    def test_multi_file_csv_write_executes_immediately(self):
        session, conn = _supported_session()
        result = _table_df(session).write_files(
            "oss://bucket/export/",
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            role_arn="acs:ram::123:role/holo-role",
            target_file_size_mb=64,
        )

        assert result is None
        assert conn.recorded[-1] == (
            "INSERT INTO external_files(path='oss://bucket/export/', format='csv', "
            "oss_endpoint='oss-cn-hangzhou-internal.aliyuncs.com', "
            "role_arn='acs:ram::123:role/holo-role', "
            'target_file_size_mb=\'64\') SELECT * FROM "public"."source"'
        )

    def test_single_file_omits_target_size(self):
        session, conn = _supported_session()
        _table_df(session).write_files("oss://bucket/export/file.csv", single_file=True)
        query = conn.recorded[-1]
        assert "single_file='true'" in query
        assert "target_file_size_mb" not in query

    def test_serverless_options_are_applied_to_export_query(self):
        from hologres.dataframe.session import _session_from_connection
        from tests._fake_conn import ServerlessConnection

        conn = ServerlessConnection()
        session = _session_from_connection(conn)
        session._server_version = (5, 1, 0)

        _table_df(session).serverless(priority=3).write_files("oss://bucket/export/")

        assert conn.recorded[0] == "SET LOCAL hg_computing_resource = 'serverless'"
        assert 'query_priority" = 3' in conn.recorded[1]
        assert conn.recorded[2].startswith("INSERT INTO external_files")

    def test_inline_values_source_is_supported(self):
        session, conn = _supported_session()
        hg.from_dict({"id": [1], "name": ["a"]}, session=session).write_files(
            "oss://bucket/export/"
        )
        assert "VALUES" in conn.recorded[-1]

    def test_pending_source_materializes_once_before_each_export(self):
        session, conn = _supported_session()
        calls = []

        def load():
            calls.append("load")
            return _plan.StageScan("stage_1", _schema())

        df = DataFrame(
            _plan.PendingSource(load, _schema()),
            session=session,
            schema=_schema(),
        )
        df.write_files("oss://bucket/export/")
        df.write_files("oss://bucket/export-again/")

        assert calls == ["load"]
        assert all("internal_stage://stage_1" in query for query in conn.recorded)

    @pytest.mark.parametrize("path", ["/tmp/out.csv", "", None])
    def test_path_must_be_oss(self, path):
        with pytest.raises(InvalidArgumentError, match="oss://"):
            _table_df(_supported_session()[0]).write_files(path)

    @pytest.mark.parametrize("format", ["parquet", "orc", "json"])
    def test_only_csv_is_supported(self, format):
        with pytest.raises(UnsupportedOperationError, match="only supports"):
            _table_df(_supported_session()[0]).write_files(
                "oss://bucket/export/", format=format
            )

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"oss_endpoint": ""}, "oss_endpoint"),
            ({"role_arn": 1}, "role_arn"),
            ({"single_file": 1}, "single_file"),
            ({"target_file_size_mb": 0}, "positive int"),
            ({"target_file_size_mb": True}, "positive int"),
        ],
    )
    def test_invalid_write_options(self, kwargs, message):
        with pytest.raises(InvalidArgumentError, match=message):
            _table_df(_supported_session()[0]).write_files(
                "oss://bucket/export/", **kwargs
            )

    def test_requires_session(self):
        with pytest.raises(InvalidArgumentError, match="no session"):
            _table_df(None).write_files("oss://bucket/export/")
