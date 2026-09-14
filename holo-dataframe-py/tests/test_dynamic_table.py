"""Offline tests for Dynamic Table creation and refresh."""

from __future__ import annotations

import math

import pytest
from hologres.table_name import TableName

import hologres.dataframe as hg
from hologres.dataframe import _plan
from hologres.dataframe.dataframe import DataFrame
from hologres.dataframe.types import LongType, StructField, StructType
from tests._fake_conn import OID_BOOL, OID_TEXT, FakeColumnInfo, make_session


class DynamicSession:
    def __init__(self):
        self.executed = []
        self.transactions = []

    def execute(self, query, *, fetch=True):
        self.executed.append(query.as_string())
        return [], StructType()

    def _execute_write_transaction(self, statements, **kwargs):
        self.transactions.append(
            {
                "statements": [statement.as_string() for statement in statements],
                "kwargs": kwargs,
            }
        )


def _df(session=None):
    schema = StructType([StructField("id", LongType())])
    return DataFrame(
        _plan.TableScan(TableName.valueOf("public.source")),
        session=session,
        schema=schema,
    )


class TestAsDynamicTable:
    def test_ignore_is_default_and_executes_immediately(self):
        session = DynamicSession()

        assert (
            _df(session).as_dynamic_table("analytics.result", freshness="1 minute")
            is None
        )

        assert session.transactions == []
        assert session.executed == [
            'CREATE DYNAMIC TABLE IF NOT EXISTS "analytics"."result" '
            "WITH (freshness = '1 minute', auto_refresh_mode = 'auto', "
            'auto_refresh_enable = \'true\') AS SELECT * FROM "public"."source"'
        ]

    def test_serverless_marker_does_not_override_refresh_resource(self):
        session = DynamicSession()

        _df(session).serverless(priority=4).as_dynamic_table(
            "analytics.result",
            freshness="1 minute",
            options={"computing_resource": "local"},
        )

        assert "computing_resource = 'local'" in session.executed[0]

    def test_errorifexists_uses_plain_create(self):
        session = DynamicSession()
        _df(session).as_dynamic_table(
            "result",
            freshness="5 minutes",
            refresh_mode="full",
            auto_refresh=False,
            mode="errorifexists",
        )

        query = session.executed[-1]
        assert query.startswith('CREATE DYNAMIC TABLE "public"."result"')
        assert "IF NOT EXISTS" not in query
        assert "auto_refresh_mode = 'full'" in query
        assert "auto_refresh_enable = 'false'" in query

    def test_replace_uses_one_transaction(self):
        session = DynamicSession()
        _df(session).as_dynamic_table(
            "public.result", freshness="1 hour", mode="replace"
        )

        assert session.executed == []
        assert session.transactions == [
            {
                "statements": [
                    'DROP TABLE IF EXISTS "public"."result"',
                    'CREATE DYNAMIC TABLE "public"."result" '
                    "WITH (freshness = '1 hour', auto_refresh_mode = 'auto', "
                    "auto_refresh_enable = 'true') AS SELECT * FROM "
                    '"public"."source"',
                ],
                "kwargs": {},
            }
        ]

    def test_extra_options_are_validated_mapped_and_escaped(self):
        session = DynamicSession()
        _df(session).as_dynamic_table(
            "result",
            freshness="10 minutes",
            options={
                "cdc_format": "bin'log",
                "computing_resource": "local",
                "state_orientation": "row",
                "parallelism": 4,
                "ratio": 0.5,
                "enabled": False,
            },
        )

        query = session.executed[-1]
        assert "base_table_cdc_format = 'bin''log'" in query
        assert "computing_resource = 'local'" in query
        assert "state_orientation = 'row'" in query
        assert "parallelism = 4" in query
        assert "ratio = 0.5" in query
        assert "enabled = 'false'" in query

    def test_pending_source_materializes_before_replace_transaction(self):
        session = DynamicSession()
        events = []
        schema = StructType([StructField("id", LongType())])

        def load():
            events.append("load")
            return _plan.TableScan(TableName.valueOf("public.loaded"))

        df = DataFrame(
            _plan.PendingSource(load, schema), session=session, schema=schema
        )
        df.as_dynamic_table("result", freshness="1 minute", mode="replace")

        assert events == ["load"]
        assert session.transactions[0]["statements"][1].endswith(
            'AS SELECT * FROM "public"."loaded"'
        )

    def test_requires_session(self):
        with pytest.raises(hg.InvalidArgumentError, match="no session"):
            _df().as_dynamic_table("result", freshness="1 minute")

    @pytest.mark.parametrize("name", ["", None, 3, "a.b.c"])
    def test_invalid_name(self, name):
        with pytest.raises(hg.InvalidArgumentError, match="name|table name"):
            _df(DynamicSession()).as_dynamic_table(name, freshness="1 minute")

    @pytest.mark.parametrize("freshness", ["", "   ", None, 1])
    def test_invalid_freshness(self, freshness):
        with pytest.raises(hg.InvalidArgumentError, match="freshness"):
            _df(DynamicSession()).as_dynamic_table("result", freshness=freshness)

    @pytest.mark.parametrize("refresh_mode", [None, "stream", "INCREMENTAL"])
    def test_invalid_refresh_mode(self, refresh_mode):
        with pytest.raises(hg.InvalidArgumentError, match="refresh_mode"):
            _df(DynamicSession()).as_dynamic_table(
                "result", freshness="1 minute", refresh_mode=refresh_mode
            )

    @pytest.mark.parametrize("auto_refresh", [None, 0, "true"])
    def test_invalid_auto_refresh(self, auto_refresh):
        with pytest.raises(hg.InvalidArgumentError, match="auto_refresh"):
            _df(DynamicSession()).as_dynamic_table(
                "result", freshness="1 minute", auto_refresh=auto_refresh
            )

    @pytest.mark.parametrize("mode", [None, "overwrite", "IGNORE"])
    def test_invalid_mode(self, mode):
        with pytest.raises(hg.InvalidArgumentError, match="mode"):
            _df(DynamicSession()).as_dynamic_table(
                "result", freshness="1 minute", mode=mode
            )

    @pytest.mark.parametrize("options", [[], "x", 1])
    def test_options_must_be_dict(self, options):
        with pytest.raises(hg.InvalidArgumentError, match="options"):
            _df(DynamicSession()).as_dynamic_table(
                "result", freshness="1 minute", options=options
            )

    @pytest.mark.parametrize("name", ["", "Bad-Name", "a.b", 1])
    def test_invalid_option_name(self, name):
        with pytest.raises(hg.InvalidArgumentError, match="option names"):
            _df(DynamicSession()).as_dynamic_table(
                "result", freshness="1 minute", options={name: "x"}
            )

    @pytest.mark.parametrize(
        "name", ["freshness", "auto_refresh_mode", "auto_refresh_enable"]
    )
    def test_named_arguments_cannot_be_repeated_in_options(self, name):
        with pytest.raises(hg.InvalidArgumentError, match="named argument"):
            _df(DynamicSession()).as_dynamic_table(
                "result", freshness="1 minute", options={name: "x"}
            )

    def test_option_alias_collision_is_rejected(self):
        with pytest.raises(hg.InvalidArgumentError, match="more than once"):
            _df(DynamicSession()).as_dynamic_table(
                "result",
                freshness="1 minute",
                options={"cdc_format": "binlog", "base_table_cdc_format": "stream"},
            )

    @pytest.mark.parametrize("value", ["", None, [], math.inf, -math.inf, math.nan])
    def test_invalid_option_value(self, value):
        with pytest.raises(hg.InvalidArgumentError, match="option"):
            _df(DynamicSession()).as_dynamic_table(
                "result", freshness="1 minute", options={"custom": value}
            )


class TestRefreshTable:
    def test_executes_refresh(self):
        session, conn = make_session(
            script=[
                ([("on",)], [FakeColumnInfo("current_setting", OID_TEXT)]),
                ([(True,)], [FakeColumnInfo("exists", OID_BOOL)]),
                ([], []),
            ]
        )
        session._server_version = (5, 1, 0)

        assert session.refresh_table("analytics.result") is None
        assert conn.recorded[-1] == 'REFRESH TABLE "analytics"."result"'

    @pytest.mark.parametrize("name", ["", None, "a.b.c"])
    def test_invalid_name(self, name):
        session, _ = make_session()
        with pytest.raises(hg.InvalidArgumentError):
            session.refresh_table(name)


def test_materialize_alias_is_not_exposed():
    assert not hasattr(DataFrame, "materialize")
