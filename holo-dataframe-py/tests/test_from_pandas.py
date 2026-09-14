"""Tests for from_pandas and the two materialization channels.

No database is involved: a FakeConnection scripts the server responses, the
FixedFE upload is stubbed, and the version probe is scripted so both channels
can be exercised. What matters here is *which* statements are issued, in what
order, and that the data is only ever uploaded once.
"""

import pytest

import hologres.dataframe as hg
from hologres.dataframe import _plan, _sources
from hologres.dataframe.exceptions import InvalidArgumentError
from tests._fake_conn import OID_TEXT, FakeColumnInfo, make_session

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")


def version_response(version: str):
    """Script one `SELECT hg_version()` reply."""
    return (
        [(f"Hologres {version} (tag: {version})",)],
        [FakeColumnInfo("v", OID_TEXT)],
    )


def make_frame():
    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})


class _Upload:
    """Records what would have been sent to a stage over FixedFE."""

    def __init__(self):
        self.statements = []
        self.payloads = []

    def install(self, monkeypatch):
        """Patch the client writer's FixedFE connection to capture uploads."""
        upload = self

        class _Copy:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                return False

            def write(self_inner, data):
                upload.payloads.append(data)

        class _Cursor:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                return False

            def copy(self_inner, statement):
                upload.statements.append(
                    statement.as_string()
                    if hasattr(statement, "as_string")
                    else str(statement)
                )
                return _Copy()

        class _Conn:
            def cursor(self_inner):
                return _Cursor()

            def close(self_inner):
                pass

        monkeypatch.setattr(
            "hologres.copy_stage._create_fixed_fe_conn", lambda config: _Conn()
        )
        return upload


@pytest.fixture
def stage_session(monkeypatch):
    """A session that reports Hologres 5.1.0, with the stage upload captured."""
    session, conn = make_session(script=[version_response("5.1.0")])
    session._conninfo = "host=h port=80 dbname=d user=u password=p"
    upload = _Upload().install(monkeypatch)
    return session, conn, upload


class TestServerVersion:
    def test_parses_a_modern_version(self):
        session, _ = make_session(script=[version_response("5.1.0")])
        assert session.server_version() == (5, 1, 0)

    def test_parses_an_old_version(self):
        session, _ = make_session(script=[version_response("0.9.29")])
        assert session.server_version() == (0, 9, 29)

    @pytest.mark.parametrize(
        ("version", "expected"),
        [
            ("Hologres 4.1", (4, 1, 0)),
            ("hologres 4.1.0", (4, 1, 0)),
            ("Hologres 4.1.25 (tag: x)", (4, 1, 25)),
        ],
    )
    def test_parses_case_and_optional_patch(self, version, expected):
        response = ([(version,)], [FakeColumnInfo("v", OID_TEXT)])
        session, _ = make_session(script=[response])
        assert session.server_version() == expected

    def test_is_cached(self):
        session, conn = make_session(script=[version_response("4.1.25")])
        assert session.server_version() == (4, 1, 25)
        assert session.server_version() == (4, 1, 25)
        assert sum("hg_version" in q for q in conn.recorded) == 1

    def test_unparseable_output_falls_back_to_zero(self):
        session, _ = make_session(
            script=[([("PostgreSQL 11.3",)], [FakeColumnInfo("v", OID_TEXT)])]
        )
        assert session.server_version() == (0, 0, 0)

    def test_unparseable_output_is_retried(self):
        unparseable = ([("PostgreSQL 11.3",)], [FakeColumnInfo("v", OID_TEXT)])
        session, _ = make_session(script=[unparseable, version_response("5.1.0")])
        assert session.server_version() == (0, 0, 0)
        assert session.server_version() == (5, 1, 0)

    def test_empty_result_falls_back_to_zero(self):
        session, _ = make_session(script=[([], [])])
        assert session.server_version() == (0, 0, 0)

    def test_execution_error_falls_back_to_zero(self):
        import psycopg

        session, conn = make_session()

        def boom(*args, **kwargs):
            raise psycopg.errors.UndefinedFunction("no hg_version()")

        conn.cursor = boom
        assert session.server_version() == (0, 0, 0)

    def test_execution_error_is_retried(self):
        import psycopg

        session, conn = make_session(script=[version_response("5.1.0")])
        calls = 0
        original_cursor = conn.cursor

        def flaky(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise psycopg.errors.ConnectionTimeout("temporary failure")
            return original_cursor(*args, **kwargs)

        conn.cursor = flaky
        assert session.server_version() == (0, 0, 0)
        assert session.server_version() == (5, 1, 0)
        assert calls == 2

    def test_use_database_clears_the_cache(self):
        from tests._fake_conn import make_reconnectable_session

        session, _ = make_reconnectable_session(
            {"db1": [version_response("5.1.0")], "db2": [version_response("4.0.1")]},
            dbname="db1",
        )
        assert session.server_version() == (5, 1, 0)
        session.use_database("db2")
        assert session.server_version() == (4, 0, 1)


class TestLaziness:
    def test_from_pandas_sends_nothing(self):
        session, conn = make_session()
        df = hg.from_pandas(make_frame(), session=session)
        assert isinstance(df._plan, _plan.PendingSource)
        assert conn.recorded == []

    def test_schema_is_known_without_a_round_trip(self):
        session, conn = make_session()
        df = hg.from_pandas(make_frame(), session=session)
        assert df.columns == ["id", "name"]
        assert df.schema["id"].datatype == hg.LongType()
        assert df.schema["name"].datatype == hg.StringType()
        assert conn.recorded == []

    def test_repr_does_not_resolve_or_upload(self):
        session, conn = make_session()
        frame = hg.from_pandas(make_frame(), session=session)

        rendered = repr(frame.filter(hg.col("id") > 1))

        assert "pending_source=(id bigint, name text)" in rendered
        assert frame._plan.resolved is None
        assert session._stages == []
        assert session._scratch_tables == []
        assert conn.recorded == []

    def test_transformations_stay_lazy(self):
        session, conn = make_session()
        df = hg.from_pandas(make_frame(), session=session)
        df.filter(hg.col("id") > 1).select("name").sort("name")
        assert conn.recorded == []

    def test_upload_happens_once_across_actions(self, stage_session):
        session, conn, upload = stage_session
        df = hg.from_pandas(make_frame(), session=session)
        df._to_sql()
        df._to_sql()
        df._to_sql()
        assert len(upload.payloads) == 1
        assert sum("hg_create_internal_stage" in q for q in conn.recorded) == 1


class TestStageChannel:
    """Hologres >= 4.1: upload Arrow, query the stage in place, create no table."""

    def test_creates_a_stage_and_uploads_arrow(self, stage_session):
        session, conn, upload = stage_session
        hg.from_pandas(make_frame(), session=session)._to_sql()
        assert any("hg_create_internal_stage" in q for q in conn.recorded)
        assert len(upload.payloads) == 1
        assert "copy external_files(path=" in upload.statements[0]
        assert ".arrow" in upload.statements[0]

    def test_uploaded_payload_is_readable_arrow(self, stage_session):
        session, _, upload = stage_session
        hg.from_pandas(make_frame(), session=session)._to_sql()
        table = pa.ipc.open_stream(pa.BufferReader(upload.payloads[0])).read_all()
        assert table.to_pydict() == {"id": [1, 2, 3], "name": ["a", "b", "c"]}

    def test_large_string_is_cast_to_client_target_schema(self, stage_session):
        session, _, upload = stage_session
        table = pa.table([pa.array(["a", "b"], type=pa.large_string())], names=["name"])
        _sources.from_arrow(table, session=session)._to_sql()

        uploaded = pa.ipc.open_stream(pa.BufferReader(upload.payloads[0])).read_all()
        assert uploaded.schema.field("name").type == pa.string()
        assert uploaded.to_pydict() == {"name": ["a", "b"]}

    def test_schema_override_renames_columns_before_client_write(self, stage_session):
        session, _, upload = stage_session
        schema = hg.StructType(
            [
                hg.StructField("renamed_id", hg.LongType()),
                hg.StructField("renamed_name", hg.StringType()),
            ]
        )
        hg.from_pandas(
            make_frame()[["id", "name"]], session=session, schema=schema
        )._to_sql()

        uploaded = pa.ipc.open_stream(pa.BufferReader(upload.payloads[0])).read_all()
        assert uploaded.column_names == ["renamed_id", "renamed_name"]
        assert uploaded.to_pydict() == {
            "renamed_id": [1, 2, 3],
            "renamed_name": ["a", "b", "c"],
        }

    def test_creates_no_table(self, stage_session):
        session, conn, _ = stage_session
        hg.from_pandas(make_frame(), session=session)._to_sql()
        assert not any("CREATE TABLE" in q for q in conn.recorded)

    def test_reads_back_through_external_files(self, stage_session):
        session, _, _ = stage_session
        out = hg.from_pandas(make_frame(), session=session)._to_sql().as_string()
        assert "external_files(path='internal_stage://_hg_df_tmp_" in out
        assert '("id" bigint, "name" text)' in out

    def test_resolves_to_a_stage_scan(self, stage_session):
        session, _, _ = stage_session
        df = hg.from_pandas(make_frame(), session=session)
        df._to_sql()
        assert isinstance(df._plan.resolved, _plan.StageScan)

    def test_stage_is_registered_for_cleanup(self, stage_session):
        session, _, _ = stage_session
        hg.from_pandas(make_frame(), session=session)._to_sql()
        assert len(session._stages) == 1
        assert session._stages[0].startswith("_hg_df_tmp_")

    def test_operators_compose_over_the_stage(self, stage_session):
        session, _, _ = stage_session
        df = hg.from_pandas(make_frame(), session=session)
        out = df.filter(hg.col("id") > 1).select("name")._to_sql().as_string()
        assert out.startswith('SELECT "name" FROM external_files(')
        assert 'WHERE ("id" > 1)' in out


class TestTableChannel:
    """Older Hologres: create a plain table and COPY into it."""

    def setup_method(self):
        self.session, self.conn = make_session(script=[version_response("4.0.1")])

    def test_creates_a_table_with_the_right_columns(self):
        df = hg.from_pandas(make_frame(), session=self.session)
        df._to_sql()
        created = [q for q in self.conn.recorded if "CREATE TABLE" in q]
        assert len(created) == 1
        assert '"id" bigint' in created[0] and '"name" text' in created[0]

    def test_copies_rows_in(self):
        df = hg.from_pandas(make_frame(), session=self.session)
        df._to_sql()
        copies = [q for q in self.conn.recorded if q.startswith("COPY ")]
        assert len(copies) == 1
        assert '("id", "name") FROM STDIN' in copies[0]
        assert self.conn.copied_rows == [(1, "a"), (2, "b"), (3, "c")]

    def test_creates_no_stage(self):
        df = hg.from_pandas(make_frame(), session=self.session)
        df._to_sql()
        assert not any("internal_stage" in q for q in self.conn.recorded)

    def test_resolves_to_a_table_scan(self):
        df = hg.from_pandas(make_frame(), session=self.session)
        out = df._to_sql().as_string()
        assert isinstance(df._plan.resolved, _plan.TableScan)
        assert '"public"."_hg_df_tmp_' in out

    def test_table_is_registered_for_cleanup(self):
        df = hg.from_pandas(make_frame(), session=self.session)
        df._to_sql()
        assert len(self.session._scratch_tables) == 1

    def test_unknown_version_uses_the_table_channel(self):
        session, conn = make_session(
            script=[([("PostgreSQL 11.3",)], [FakeColumnInfo("v", OID_TEXT)])]
        )
        hg.from_pandas(make_frame(), session=session)._to_sql()
        assert any("CREATE TABLE" in q for q in conn.recorded)


class TestExplicitTableName:
    def test_uses_the_given_table_even_on_a_new_server(self):
        session, conn = make_session(script=[version_response("5.1.0")])
        df = hg.from_pandas(make_frame(), session=session, table_name="public.mine")
        out = df._to_sql().as_string()
        assert '"public"."mine"' in out
        assert any("CREATE TABLE" in q for q in conn.recorded)

    def test_is_not_registered_for_cleanup(self):
        session, _ = make_session(script=[version_response("5.1.0")])
        hg.from_pandas(
            make_frame(), session=session, table_name="public.mine"
        )._to_sql()
        assert session._scratch_tables == []


class TestSchemaOverride:
    def test_replaces_names_and_types(self):
        session, _ = make_session(script=[version_response("4.0.1")])
        schema = hg.StructType(
            [
                hg.StructField("a", hg.IntegerType()),
                hg.StructField("b", hg.StringType()),
            ]
        )
        df = hg.from_pandas(make_frame(), session=session, schema=schema)
        assert df.columns == ["a", "b"]

    def test_field_count_must_match(self):
        session, _ = make_session()
        schema = hg.StructType([hg.StructField("a", hg.IntegerType())])
        with pytest.raises(InvalidArgumentError, match="schema has 1 field"):
            hg.from_pandas(make_frame(), session=session, schema=schema)

    def test_must_be_a_struct_type(self):
        session, _ = make_session()
        with pytest.raises(InvalidArgumentError, match="must be a StructType"):
            hg.from_pandas(make_frame(), session=session, schema="id bigint")


class TestValidation:
    def test_session_is_required(self):
        with pytest.raises(InvalidArgumentError, match="requires session"):
            hg.from_pandas(make_frame(), session=None)

    def test_non_dataframe_rejected(self):
        session, _ = make_session()
        with pytest.raises(InvalidArgumentError, match="expects a pandas DataFrame"):
            hg.from_pandas({"id": [1]}, session=session)

    def test_empty_frame_rejected(self):
        session, _ = make_session()
        with pytest.raises(InvalidArgumentError, match="no rows"):
            hg.from_pandas(pd.DataFrame({"id": []}), session=session)


class TestArrowSource:
    def test_arrow_table_is_handed_over_untouched(self):
        table = pa.table({"id": [1, 2]})
        source = _sources._ArrowSource(table)
        assert source.arrow_table() is table

    def test_row_iter_yields_tuples(self):
        table = pa.table({"id": [1, 2], "s": ["a", "b"]})
        source = _sources._ArrowSource(table)
        assert list(source.row_iter()) == [(1, "a"), (2, "b")]


class TestClientSchemaAdapter:
    def test_preserves_parameterized_types_and_nullability(self):
        schema = hg.StructType(
            [
                hg.StructField("label", hg.StringType(32), nullable=False),
                hg.StructField("amount", hg.DecimalType(10, 2)),
                hg.StructField("unbounded", hg.DecimalType(None, None)),
            ]
        )

        client_schema = _sources._client_table_schema(schema, "stage_name")
        label, amount, unbounded = client_schema.columns

        assert (label.type_name, label.allow_null) == ("varchar", False)
        assert (amount.type_name, amount.precision, amount.scale) == (
            "numeric",
            10,
            2,
        )
        assert (unbounded.precision, unbounded.scale) == (38, 0)


class TestCleanup:
    def test_close_drops_stage_and_table(self, stage_session):
        from hologres.table_name import TableName

        session, conn, _ = stage_session
        hg.from_pandas(make_frame(), session=session)._to_sql()
        session._register_scratch_table(TableName.valueOf("public.leftover"))
        session.close()
        assert any(
            "DROP TABLE IF EXISTS" in q and q.endswith(" FORCE") for q in conn.recorded
        )
        assert any("hg_drop_internal_stage" in q for q in conn.recorded)

    def test_drop_scratch_uses_force_on_hologres_3_1_or_newer(self):
        from hologres.table_name import TableName

        session, conn = make_session(script=[version_response("3.1.0")])
        session._register_scratch_table(TableName.valueOf("public.scratch"))
        session.close()
        assert any(q.endswith(" FORCE") for q in conn.recorded)

    def test_drop_scratch_omits_force_before_hologres_3_1(self):
        from hologres.table_name import TableName

        session, conn = make_session(script=[version_response("3.0.9")])
        session._register_scratch_table(TableName.valueOf("public.scratch"))
        session.close()
        drops = [q for q in conn.recorded if "DROP TABLE IF EXISTS" in q]
        assert drops and all(not q.endswith(" FORCE") for q in drops)

    def test_drop_scratch_clears_the_registries(self, stage_session):
        session, _, _ = stage_session
        hg.from_pandas(make_frame(), session=session)._to_sql()
        session.drop_scratch()
        assert session._stages == [] and session._scratch_tables == []

    def test_cleanup_failure_does_not_break_close(self, stage_session):
        session, conn, _ = stage_session
        hg.from_pandas(make_frame(), session=session)._to_sql()

        def boom(*args, **kwargs):
            raise RuntimeError("cannot drop")

        conn.cursor = boom
        session.close()  # must not raise
        assert session.closed


class TestFixedFeRequirement:
    def test_test_injected_session_cannot_upload_to_a_stage(self):
        from hologres.dataframe.exceptions import ExecutionError

        session, _ = make_session(script=[version_response("5.1.0")])
        df = hg.from_pandas(make_frame(), session=session)
        # No conninfo to derive FixedFE from, so the stage upload cannot proceed;
        # the failure surfaces as an ExecutionError like any other write error.
        with pytest.raises(ExecutionError, match="requires a session created"):
            df._to_sql()

    def test_derives_client_config_from_the_session_conninfo(self, monkeypatch):
        from hologres.dataframe.session import Session, _SingleConnectionPool
        from tests._fake_conn import FakeConnection

        captured = {}

        class _Writer:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def write_table(self, table):
                captured["table"] = table

        monkeypatch.setattr("hologres.CopyStageWriter", _Writer)
        session = Session(
            _SingleConnectionPool(FakeConnection()),
            dbname="d",
            conninfo=(
                "host=h port=80 dbname=d user=u password=p "
                "application_name=df-test sslmode=require"
            ),
        )
        _sources._materialize_via_stage(
            session,
            _sources._ArrowSource(pa.table({"id": [1]})),
            hg.StructType([hg.StructField("id", hg.LongType())]),
        )

        config = captured["config"]
        assert (config.host, config.port, config.database) == ("h", 80, "d")
        assert (config.username, config.password, config.app_name) == (
            "u",
            "p",
            "df-test",
        )
        assert captured["auto_commit"] is False
        assert "sslmode=require" in config.fixed_fe_conninfo
        assert config.fixed_fe_conninfo.endswith("options=type=fixed")


class TestFailureHandling:
    def test_copy_failure_is_wrapped(self, monkeypatch):
        from hologres.dataframe.exceptions import ExecutionError
        from tests._fake_conn import FakeCursor

        session, _ = make_session(script=[version_response("4.0.1")])

        def boom(self, statement):
            raise RuntimeError("copy stream broke")

        monkeypatch.setattr(FakeCursor, "copy", boom)
        df = hg.from_pandas(make_frame(), session=session)
        with pytest.raises(ExecutionError, match="copy stream broke"):
            df._to_sql()

    def test_scratch_table_drop_failure_is_logged_not_raised(self, caplog):
        from hologres.table_name import TableName

        session, conn = make_session()
        session._register_scratch_table(TableName.valueOf("public.gone"))

        def boom(*args, **kwargs):
            raise RuntimeError("cannot drop table")

        conn.cursor = boom
        with caplog.at_level("WARNING"):
            session.drop_scratch()
        assert "failed to drop scratch table" in caplog.text
        assert len(session._scratch_tables) == 1
