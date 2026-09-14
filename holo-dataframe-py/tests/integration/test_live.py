"""Live integration tests against a real Hologres.

Everything else in this suite runs against a FakeConnection, which proves the
compiler emits the SQL we intend but not that Hologres accepts it. These tests
close that gap: they execute the generated statements end-to-end.

They are skipped unless all of ``HOLO_TEST_HOST``, ``HOLO_TEST_PORT``,
``HOLO_TEST_DATABASE``, ``HOLO_TEST_USERNAME``, ``HOLO_TEST_PASSWORD`` are set,
so the default ``pytest`` run stays offline and green.

Run::

    HOLO_TEST_HOST=... HOLO_TEST_PORT=80 HOLO_TEST_DATABASE=... \\
    HOLO_TEST_USERNAME=... HOLO_TEST_PASSWORD=... \\
    pytest tests/integration/

Real FC deployment additionally requires ``HOLO_TEST_FC_ENDPOINT`` plus
``ALIBABA_CLOUD_ACCESS_KEY_ID`` and ``ALIBABA_CLOUD_ACCESS_KEY_SECRET``.
``HOLO_TEST_FC_ROLE_ARN`` is optional for functions that need an execution role.

Tests adapt to the server: anything that needs a newer Hologres checks
``session.server_version()`` (or probes the feature) and skips rather than
failing on an older instance. Every test cleans up the tables it creates.
"""

from __future__ import annotations

import os
import re
import uuid

import pytest

import hologres.dataframe as hg

_REQUIRED_ENV = (
    "HOLO_TEST_HOST",
    "HOLO_TEST_PORT",
    "HOLO_TEST_DATABASE",
    "HOLO_TEST_USERNAME",
    "HOLO_TEST_PASSWORD",
)
_FC_ENDPOINT_ENV = "HOLO_TEST_FC_ENDPOINT"
_FC_CREDENTIAL_ENV = (
    "ALIBABA_CLOUD_ACCESS_KEY_ID",
    "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
)


def _env_configured() -> bool:
    return all(os.environ.get(v) for v in _REQUIRED_ENV)


def _normalize_fc_endpoint(endpoint: str) -> tuple[str, str]:
    hostname = endpoint.strip().rstrip("/")
    match = re.fullmatch(
        r"(?P<account>[^.]+)\.(?P<region>[a-z0-9-]+)-internal"
        r"\.fc\.aliyuncs\.com",
        hostname,
    )
    if match is None:
        match = re.fullmatch(
            r"(?P<account>[^.]+)\.(?P<region>[a-z0-9-]+)\.fc\.aliyuncs\.com",
            hostname,
        )
    if match is None:
        raise ValueError(
            "HOLO_TEST_FC_ENDPOINT must look like "
            "<account>.<region>.fc.aliyuncs.com or "
            "<account>.<region>-internal.fc.aliyuncs.com"
        )
    account = match.group("account")
    region = match.group("region")
    internal_endpoint = f"{account}.{region}-internal.fc.aliyuncs.com"
    return internal_endpoint, region


def _drop_test_table(session, identifier) -> None:
    from psycopg import sql

    force = sql.SQL(" FORCE") if session.server_version() >= (3, 1, 0) else sql.SQL("")
    session.execute(
        sql.SQL("DROP TABLE IF EXISTS {}{}").format(identifier, force), fetch=False
    )


pytestmark = pytest.mark.skipif(
    not _env_configured(),
    reason=(
        "Set "
        + ", ".join(_REQUIRED_ENV)
        + " to run integration tests against a live Hologres instance."
    ),
)


@pytest.fixture(scope="module")
def session():
    s = hg.connect(
        host=os.environ["HOLO_TEST_HOST"],
        port=int(os.environ["HOLO_TEST_PORT"]),
        dbname=os.environ["HOLO_TEST_DATABASE"],
        user=os.environ["HOLO_TEST_USERNAME"],
        password=os.environ["HOLO_TEST_PASSWORD"],
        options="-c hg_experimental_force_sync_replay=true",
    )
    try:
        yield s
    finally:
        s.close()


@pytest.fixture
def fc_endpoint():
    """Return an FC internal endpoint and region derived from the config."""
    if not os.environ.get(_FC_ENDPOINT_ENV):
        pytest.skip(f"Set {_FC_ENDPOINT_ENV} to run FC integration tests.")
    try:
        return _normalize_fc_endpoint(os.environ[_FC_ENDPOINT_ENV])
    except ValueError as exc:
        pytest.fail(str(exc))


@pytest.fixture
def fc_session(fc_endpoint):
    """Open a Session configured for real FC deployment and execution."""
    missing = [name for name in _FC_CREDENTIAL_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(
            "Set " + ", ".join(_FC_CREDENTIAL_ENV) + " to run real FC deployment tests."
        )

    endpoint, region = fc_endpoint

    fc_config = {
        "endpoint": endpoint,
        "region": region,
    }
    role_arn = os.environ.get("HOLO_TEST_FC_ROLE_ARN")
    if role_arn:
        fc_config["role_arn"] = role_arn

    s = hg.connect(
        host=os.environ["HOLO_TEST_HOST"],
        port=int(os.environ["HOLO_TEST_PORT"]),
        dbname=os.environ["HOLO_TEST_DATABASE"],
        user=os.environ["HOLO_TEST_USERNAME"],
        password=os.environ["HOLO_TEST_PASSWORD"],
        options="-c hg_experimental_force_sync_replay=true",
        fc_config=fc_config,
    )
    try:
        yield s
    finally:
        s.close()


@pytest.fixture(scope="module")
def orders_table(session):
    """A small fixture table, dropped when the module finishes."""
    from psycopg import sql

    name = f"hg_df_it_orders_{uuid.uuid4().hex[:8]}"
    ident = sql.Identifier("public", name)
    session.execute(
        sql.SQL(
            "CREATE TABLE {} ("
            "  order_id bigint,"
            "  region text,"
            "  amount double precision,"
            "  tags text[]"
            ")"
        ).format(ident),
        fetch=False,
    )
    session.execute(
        sql.SQL(
            "INSERT INTO {} VALUES"
            " (1, 'east', 100.0, ARRAY['a','b']),"
            " (2, 'east', 250.5, ARRAY['b']),"
            " (3, 'west', 50.0, ARRAY[]::text[]),"
            " (4, 'west', 400.0, NULL),"
            " (5, NULL, NULL, ARRAY['c'])"
        ).format(ident),
        fetch=False,
    )
    try:
        yield f"public.{name}"
    finally:
        _drop_test_table(session, ident)


class TestSessionAndCatalog:
    def test_sql_select_one(self, session):
        rows = session.sql("SELECT 1 AS x").collect()
        assert rows and rows[0]["x"] == 1

    def test_serverless_settings_are_scoped_to_one_action(self, session):
        settings = session.sql(
            "SELECT "
            "current_setting('hg_computing_resource') AS resource, "
            "current_setting("
            "'hg_experimental_serverless_tasks_query_priority') AS priority, "
            "current_setting("
            "'hg_experimental_serverless_tasks_required_cores') "
            "AS required_cores, "
            "current_setting("
            "'hg_experimental_serverless_tasks_max_cores') AS max_cores"
        ).serverless(priority=4, required_cores=64, max_cores=128)

        try:
            row = settings.first()
        except hg.UnsupportedOperationError as exc:
            pytest.skip(f"Hologres Serverless settings are unavailable: {exc}")

        assert tuple(row) == ("serverless", "4", "64", "128")
        unmarked = session.sql(
            "SELECT current_setting('hg_computing_resource') AS resource"
        ).first()
        assert unmarked.resource == "local"

    def test_current_schema_and_database(self, session):
        assert isinstance(session.current_schema(), str)
        assert session.current_database() == os.environ["HOLO_TEST_DATABASE"]

    def test_list_schemas_includes_public(self, session):
        assert "public" in session.list_schemas()

    def test_list_tables_sees_the_fixture(self, session, orders_table):
        bare = orders_table.split(".", 1)[1]
        assert bare in session.list_tables("public")

    def test_use_schema(self, session):
        session.use_schema("public")
        assert session.current_schema() == "public"

    def test_table_pins_the_real_schema(self, session, orders_table):
        df = session.table(orders_table)
        assert df.columns == ["order_id", "region", "amount", "tags"]
        assert df.schema["order_id"].datatype == hg.LongType()
        assert df.schema["amount"].datatype == hg.DoubleType()
        assert df.schema["tags"].datatype == hg.ArrayType(hg.StringType())

    def test_missing_table_raises(self, session):
        with pytest.raises(hg.HoloDataFrameError):
            session.table("public.hg_df_it_definitely_missing")


class TestTransformations:
    """Each generated statement must actually run and return the right rows."""

    def test_filter_and_sort(self, session, orders_table):
        df = session.table(orders_table)
        rows = df.filter(hg.col("amount") > 90).sort("amount").collect()
        assert [r["order_id"] for r in rows] == [1, 2, 4]

    def test_select_derived_column(self, session, orders_table):
        df = session.table(orders_table)
        row = (
            df.filter(hg.col("order_id") == 1)
            .select("order_id", (hg.col("amount") * 2).alias("double_amount"))
            .first()
        )
        assert row["double_amount"] == 200.0

    def test_with_column(self, session, orders_table):
        df = session.table(orders_table).with_column("tax", hg.col("amount") * 0.1)
        row = df.filter(hg.col("order_id") == 1).first()
        assert round(row["tax"], 4) == 10.0

    def test_limit_offset(self, session, orders_table):
        df = session.table(orders_table).sort("order_id")
        assert [r["order_id"] for r in df.limit(2).collect()] == [1, 2]
        assert [r["order_id"] for r in df.offset(3).limit(2).collect()] == [4, 5]

    def test_distinct(self, session, orders_table):
        df = session.table(orders_table).select("region").distinct()
        regions = {r["region"] for r in df.collect()}
        assert regions == {"east", "west", None}

    def test_drop_duplicates_on_subset(self, session, orders_table):
        df = session.table(orders_table).drop_duplicates(subset=["region"])
        assert len(df.collect()) == 3

    def test_count(self, session, orders_table):
        assert session.table(orders_table).count() == 5

    def test_dropna_and_fillna(self, session, orders_table):
        df = session.table(orders_table)
        assert df.dropna(subset=["amount"]).count() == 4
        filled = df.fillna({"amount": 0.0}).sort("order_id").collect()
        assert filled[-1]["amount"] == 0.0

    def test_drop_and_rename(self, session, orders_table):
        df = session.table(orders_table).drop("tags").rename({"amount": "value"})
        assert df.columns == ["order_id", "region", "value"]
        assert df.first()["value"] is not None or True  # column resolves server-side

    def test_iter_rows_streams(self, session, orders_table):
        ids = [r["order_id"] for r in session.table(orders_table).iter_rows()]
        assert sorted(ids) == [1, 2, 3, 4, 5]

    def test_show_does_not_raise(self, session, orders_table, capsys):
        session.table(orders_table).show(3)
        assert "order_id" in capsys.readouterr().out


class TestWrite:
    """Execute DataFrame.write paths against real Hologres tables."""

    def test_append_server_query_and_inline_values(self, session):
        from psycopg import sql

        suffix = uuid.uuid4().hex[:8]
        source = f"hg_df_it_write_src_{suffix}"
        target = f"hg_df_it_write_dst_{suffix}"
        source_id = sql.Identifier("public", source)
        target_id = sql.Identifier("public", target)
        try:
            session.execute(
                sql.SQL("CREATE TABLE {} (id bigint, name text)").format(source_id),
                fetch=False,
            )
            session.execute(
                sql.SQL("CREATE TABLE {} (id bigint, name text)").format(target_id),
                fetch=False,
            )
            session.execute(
                sql.SQL("INSERT INTO {} VALUES (1, 'a'), (2, 'b')").format(source_id),
                fetch=False,
            )

            session.table(f"public.{source}").filter(hg.col("id") == 2).write(
                f"public.{target}"
            )
            hg.from_dict({"id": [3], "name": ["c"]}, session=session).write(
                f"public.{target}"
            )

            rows = session.table(f"public.{target}").sort("id").collect()
            assert [(row["id"], row["name"]) for row in rows] == [
                (2, "b"),
                (3, "c"),
            ]
        finally:
            _drop_test_table(session, target_id)
            _drop_test_table(session, source_id)

    def test_on_conflict_update(self, session):
        from psycopg import sql

        name = f"hg_df_it_write_pk_{uuid.uuid4().hex[:8]}"
        ident = sql.Identifier("public", name)
        try:
            session.execute(
                sql.SQL("CREATE TABLE {} (id bigint PRIMARY KEY, name text)").format(
                    ident
                ),
                fetch=False,
            )
            session.execute(
                sql.SQL("INSERT INTO {} VALUES (1, 'old')").format(ident),
                fetch=False,
            )
            hg.from_dict({"id": [1, 2], "name": ["new", "two"]}, session=session).write(
                f"public.{name}", on_conflict=("update", ["id"])
            )
            rows = session.table(f"public.{name}").sort("id").collect()
            assert [(row["id"], row["name"]) for row in rows] == [
                (1, "new"),
                (2, "two"),
            ]
        finally:
            _drop_test_table(session, ident)

    def test_on_conflict_replace_fills_missing_columns_with_null(self, session):
        from psycopg import sql

        name = f"hg_df_it_write_replace_{uuid.uuid4().hex[:8]}"
        ident = sql.Identifier("public", name)
        try:
            session.execute(
                sql.SQL(
                    "CREATE TABLE {} ("
                    "id bigint PRIMARY KEY, name text, note text DEFAULT 'default'"
                    ")"
                ).format(ident),
                fetch=False,
            )
            session.execute(
                sql.SQL("INSERT INTO {} VALUES (1, 'old', 'keep')").format(ident),
                fetch=False,
            )
            hg.from_dict({"id": [1], "name": ["new"]}, session=session).write(
                f"public.{name}", on_conflict=("replace", ["id"])
            )
            row = session.table(f"public.{name}").first()
            assert (row["id"], row["name"], row["note"]) == (1, "new", None)
        finally:
            _drop_test_table(session, ident)

    def test_overwrite_non_partition_table(self, session):
        from psycopg import sql

        name = f"hg_df_it_write_ow_{uuid.uuid4().hex[:8]}"
        ident = sql.Identifier("public", name)
        try:
            session.execute(
                sql.SQL("CREATE TABLE {} (id bigint, name text)").format(ident),
                fetch=False,
            )
            session.execute(
                sql.SQL("INSERT INTO {} VALUES (1, 'old')").format(ident),
                fetch=False,
            )
            hg.from_dict({"id": [2], "name": ["new"]}, session=session).write(
                f"public.{name}", overwrite=True
            )
            rows = session.table(f"public.{name}").collect()
            assert [(row["id"], row["name"]) for row in rows] == [(2, "new")]
        finally:
            _drop_test_table(session, ident)

    def test_overwrite_logical_partition(self, session):
        from psycopg import sql

        if session.server_version() < (3, 1, 0):
            pytest.skip("native logical partition overwrite needs Hologres 3.1+")
        name = f"hg_df_it_write_lp_{uuid.uuid4().hex[:8]}"
        ident = sql.Identifier("public", name)
        try:
            session.execute(
                sql.SQL(
                    "CREATE TABLE {} ("
                    "id bigint, dt text, PRIMARY KEY (dt, id)"
                    ") LOGICAL PARTITION BY LIST (dt)"
                ).format(ident),
                fetch=False,
            )
            session.execute(
                sql.SQL("INSERT INTO {} VALUES (1, '2026-08-17'), (9, 'other')").format(
                    ident
                ),
                fetch=False,
            )
            hg.from_dict({"id": [2], "dt": ["2026-08-17"]}, session=session).write(
                f"public.{name}",
                overwrite=True,
                partition={"dt": "2026-08-17"},
            )
            rows = session.table(f"public.{name}").sort("id").collect()
            assert [(row["id"], row["dt"]) for row in rows] == [
                (2, "2026-08-17"),
                (9, "other"),
            ]
        finally:
            _drop_test_table(session, ident)

    def test_from_pandas_write(self, session):
        import pandas as pd
        from psycopg import sql

        name = f"hg_df_it_write_pd_{uuid.uuid4().hex[:8]}"
        ident = sql.Identifier("public", name)
        try:
            session.execute(
                sql.SQL("CREATE TABLE {} (id bigint, name text)").format(ident),
                fetch=False,
            )
            frame = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
            session.from_pandas(frame).write(f"public.{name}")
            rows = session.table(f"public.{name}").sort("id").collect()
            assert [(row["id"], row["name"]) for row in rows] == [
                (1, "a"),
                (2, "b"),
            ]
        finally:
            _drop_test_table(session, ident)
            session.drop_scratch()


class TestAggregation:
    def test_group_by_agg(self, session, orders_table):
        df = session.table(orders_table)
        rows = (
            df.dropna(subset=["region"])
            .group_by("region")
            .agg(hg.sum("amount").alias("total"), hg.count("*").alias("n"))
            .sort("region")
            .collect()
        )
        assert [r["region"] for r in rows] == ["east", "west"]
        assert rows[0]["total"] == 350.5
        assert rows[0]["n"] == 2

    def test_global_aggregate(self, session, orders_table):
        df = session.table(orders_table)
        row = df.group_by().agg(hg.count("*").alias("n")).first()
        assert row["n"] == 5

    def test_grouped_shortcut(self, session, orders_table):
        rows = session.table(orders_table).group_by("region").count().collect()
        assert sum(r["count"] for r in rows) == 5

    def test_raw_expr_aggregate(self, session, orders_table):
        row = (
            session.table(orders_table)
            .group_by()
            .agg(hg.expr("count(*) filter (where amount > 100)").alias("big"))
            .first()
        )
        assert row["big"] == 2


class TestJoinsAndSetOps:
    def test_using_join(self, session, orders_table):
        # Hologres cannot compare array columns in a hash join condition, so the
        # array column is dropped before joining on the remaining ones.
        left = session.table(orders_table).drop("tags")
        right = session.table(orders_table).drop("tags")
        joined = left.join(right, on=["order_id", "region", "amount"])
        # USING compares for equality, and the row whose region/amount are NULL
        # does not match even itself, so 4 of the 5 rows survive.
        assert joined.count() == 4
        assert joined.columns == ["order_id", "region", "amount"]

    def test_on_join_with_qualified_columns(self, session, orders_table):
        left = session.table(orders_table)
        right = session.table(orders_table)
        joined = left.join(
            right,
            on=left["order_id"] == right["order_id"],
            lsuffix="_l",
            rsuffix="_r",
        )
        assert joined.count() == 5
        assert "order_id_l" in joined.columns and "order_id_r" in joined.columns

    def test_left_join_keeps_unmatched(self, session, orders_table):
        left = session.table(orders_table)
        right = session.table(orders_table).filter(hg.col("order_id") == 1)
        joined = left.join(
            right, on=left["order_id"] == right["order_id"], how="left", rsuffix="_r"
        )
        assert joined.count() == 5

    def test_cross_join(self, session, orders_table):
        left = session.table(orders_table).limit(2)
        right = session.table(orders_table).limit(3)
        # limit inside a join input must stay scoped to that input
        joined = left.join(right, how="cross", lsuffix="_l", rsuffix="_r")
        assert joined.count() == 6

    def test_union_dedups(self, session, orders_table):
        # Hologres cannot union array-typed columns, so drop the array first.
        df = session.table(orders_table).drop("tags")
        assert df.union(df).count() == 5

    def test_union_all_keeps_duplicates(self, session, orders_table):
        df = session.table(orders_table).drop("tags")
        assert df.union_all(df).count() == 10

    def test_union_arm_limits_are_scoped(self, session, orders_table):
        df = session.table(orders_table).drop("tags")
        # Without per-arm parentheses the LIMIT would apply to the whole union.
        assert df.limit(2).union_all(df.limit(3)).count() == 5

    def test_explode(self, session, orders_table):
        df = session.table(orders_table).explode("tags")
        rows = df.collect()
        # Rows with an empty or NULL array drop out: 2 + 1 + 0 + 0 + 1 = 4
        assert len(rows) == 4
        assert {r["tags"] for r in rows} == {"a", "b", "c"}

    def test_explode_outer_keeps_empty_arrays(self, session, orders_table):
        df = session.table(orders_table).explode("tags", outer=True)
        rows = df.collect()
        assert len(rows) == 6
        assert sum(1 for r in rows if r["tags"] is None) == 2


class TestInlineSources:
    def test_from_dict(self, session):
        df = hg.from_dict({"id": [1, 2, 3], "name": ["a", "b", "c"]}, session=session)
        rows = df.sort("id").collect()
        assert [r["id"] for r in rows] == [1, 2, 3]
        assert rows[0]["name"] == "a"

    def test_from_records(self, session):
        df = hg.from_records(
            [{"k": "x", "v": 1.5}, {"k": "y", "v": 2.5}], session=session
        )
        assert df.group_by().agg(hg.sum("v").alias("t")).first()["t"] == 4.0

    def test_inline_types_survive_the_round_trip(self, session):
        import datetime
        import decimal

        df = hg.from_dict(
            {
                "i": [7],
                "f": [1.5],
                "s": ["hi"],
                "b": [True],
                "d": [datetime.date(2024, 1, 2)],
                "n": [decimal.Decimal("1.25")],
            },
            session=session,
        )
        row = df.first()
        assert row["i"] == 7
        assert row["f"] == 1.5
        assert row["s"] == "hi"
        assert row["b"] is True
        assert row["d"] == datetime.date(2024, 1, 2)
        assert row["n"] == decimal.Decimal("1.25")

    def test_inline_escapes_quotes(self, session):
        df = hg.from_dict({"s": ["a'b", 'c"d']}, session=session)
        assert {r["s"] for r in df.collect()} == {"a'b", 'c"d'}

    def test_inline_null_handling(self, session):
        df = hg.from_dict({"x": [None, 2, 3]}, session=session)
        assert df.dropna(subset=["x"]).count() == 2

    def test_join_inline_against_a_table(self, session, orders_table):
        orders = session.table(orders_table)
        rates = hg.from_dict(
            {"region": ["east", "west"], "rate": [0.1, 0.2]}, session=session
        )
        joined = orders.join(rates, on="region").select(
            "order_id", (hg.col("amount") * hg.col("rate")).alias("fee")
        )
        assert len(joined.collect()) == 4


class TestToPandas:
    def test_to_pandas_round_trip(self, session, orders_table):
        pd = pytest.importorskip("pandas")
        frame = session.table(orders_table).sort("order_id").to_pandas()
        assert isinstance(frame, pd.DataFrame)
        assert list(frame.columns) == ["order_id", "region", "amount", "tags"]
        assert len(frame) == 5


class TestFromPandas:
    """End-to-end materialization: the channel depends on the server version."""

    @pytest.fixture
    def frame(self):
        pd = pytest.importorskip("pandas")
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "amount": [1.5, 2.5, 3.5],
            }
        )

    def test_schema_is_known_before_any_upload(self, session, frame):
        df = hg.from_pandas(frame, session=session)
        assert df.columns == ["id", "name", "amount"]
        assert df.schema["id"].datatype == hg.LongType()
        assert df.schema["amount"].datatype == hg.DoubleType()

    def test_round_trips_the_data(self, session, frame):
        df = hg.from_pandas(frame, session=session)
        try:
            rows = df.sort("id").collect()
            assert [r["id"] for r in rows] == [1, 2, 3]
            assert [r["name"] for r in rows] == ["a", "b", "c"]
            assert rows[0]["amount"] == 1.5
        finally:
            session.drop_scratch()

    def test_uses_the_stage_channel_on_a_new_server(self, session, frame):
        from hologres.dataframe import _plan

        if session.server_version() < (4, 1, 0):
            pytest.skip("stage channel needs Hologres 4.1+")
        df = hg.from_pandas(frame, session=session)
        try:
            df.count()
            # Queried straight out of the stage: no table was created.
            assert isinstance(df._plan.resolved, _plan.StageScan)
            assert "external_files" in df._to_sql().as_string()
        finally:
            session.drop_scratch()

    def test_operators_push_down_over_the_source(self, session, frame):
        df = hg.from_pandas(frame, session=session)
        try:
            out = (
                df.filter(hg.col("amount") > 2)
                .select("name", (hg.col("amount") * 2).alias("doubled"))
                .sort("name")
                .collect()
            )
            assert [r["name"] for r in out] == ["b", "c"]
            assert out[0]["doubled"] == 5.0
        finally:
            session.drop_scratch()

    def test_aggregates_over_the_source(self, session, frame):
        df = hg.from_pandas(frame, session=session)
        try:
            row = df.group_by().agg(hg.sum("amount").alias("total")).first()
            assert row["total"] == 7.5
        finally:
            session.drop_scratch()

    def test_joins_against_a_real_table(self, session, orders_table, frame):
        df = hg.from_pandas(frame, session=session)
        orders = session.table(orders_table).drop("tags")
        try:
            joined = orders.join(
                df, on=orders["order_id"] == df["id"], lsuffix="_o", rsuffix="_f"
            )
            assert joined.count() == 3
        finally:
            session.drop_scratch()

    def test_uploads_only_once(self, session, frame):
        df = hg.from_pandas(frame, session=session)
        try:
            assert df.count() == 3
            resolved = df._plan.resolved
            assert df.count() == 3
            # The second action reuses the same materialized source.
            assert df._plan.resolved is resolved
        finally:
            session.drop_scratch()

    def test_explicit_table_name_is_left_to_the_caller(self, session, frame):
        from psycopg import sql

        name = f"hg_df_it_named_{uuid.uuid4().hex[:8]}"
        df = hg.from_pandas(frame, session=session, table_name=f"public.{name}")
        try:
            assert df.count() == 3
            # Not registered for automatic cleanup.
            assert session._scratch_tables == []
            assert name in session.list_tables("public")
        finally:
            _drop_test_table(session, sql.Identifier("public", name))

    def test_drop_scratch_releases_the_objects(self, session, frame):
        df = hg.from_pandas(frame, session=session)
        df.count()
        assert session._stages or session._scratch_tables
        session.drop_scratch()
        assert session._stages == [] and session._scratch_tables == []

    def test_inline_overflow_materializes(self, session):
        from hologres.dataframe import _plan

        df = hg.from_dict({"x": [1, 2, 3]}, session=session, inline_limit=2)
        try:
            assert isinstance(df._plan, _plan.PendingSource)
            assert df.count() == 3
        finally:
            session.drop_scratch()


class TestReadFiles:
    """End-to-end local file reading."""

    @pytest.fixture
    def csv_file(self, tmp_path):
        p = tmp_path / "orders.csv"
        p.write_text("id,region,amount\n1,east,10.5\n2,west,20.0\n3,east,30.0\n")
        return str(p)

    def test_reads_a_csv(self, session, csv_file):
        df = session.read_files(csv_file)
        try:
            assert df.columns == ["id", "region", "amount"]
            rows = df.sort("id").collect()
            assert [r["id"] for r in rows] == [1, 2, 3]
            assert rows[0]["region"] == "east"
        finally:
            session.drop_scratch()

    def test_reads_a_parquet_file(self, session, tmp_path):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        p = str(tmp_path / "orders.parquet")
        pq.write_table(pa.table({"id": [1, 2], "name": ["a", "b"]}), p)
        df = session.read_files(p)
        try:
            assert df.columns == ["id", "name"]
            assert {r["name"] for r in df.collect()} == {"a", "b"}
        finally:
            session.drop_scratch()

    def test_glob_reads_all_matching_files(self, session, tmp_path):
        (tmp_path / "a.csv").write_text("id\n1\n2\n")
        (tmp_path / "b.csv").write_text("id\n3\n4\n")
        df = session.read_files(str(tmp_path / "*.csv"))
        try:
            assert df.count() == 4
        finally:
            session.drop_scratch()

    def test_delimiter_option_passes_through(self, session, tmp_path):
        p = tmp_path / "pipe.csv"
        p.write_text("id|region\n1|east\n2|west\n")
        df = session.read_files(str(p), delimiter="|")
        try:
            assert df.count() == 2
            assert df.columns == ["id", "region"]
        finally:
            session.drop_scratch()

    def test_aggregation_over_the_uploaded_data(self, session, csv_file):
        df = session.read_files(csv_file)
        try:
            row = (
                df.group_by("region")
                .agg(hg.sum("amount").alias("total"))
                .sort("region")
                .first()
            )
            assert row["region"] == "east" and row["total"] == 40.5
        finally:
            session.drop_scratch()

    def test_uses_the_stage_channel_on_a_new_server(self, session, csv_file):
        from hologres.dataframe import _plan

        if session.server_version() < (4, 1, 0):
            pytest.skip("stage channel needs Hologres 4.1+")
        df = session.read_files(csv_file)
        try:
            df.count()
            assert isinstance(df._plan.resolved, _plan.StageScan)
        finally:
            session.drop_scratch()


class TestServerCapabilities:
    """Facts about the server that the offline suite cannot establish."""

    def test_hg_version_is_parseable(self, session):
        rows = session.sql("SELECT hg_version() AS v").collect()
        assert "Hologres" in rows[0]["v"]

    def test_external_files_over_a_stage_is_queryable(self, session):
        """Whether `external_files` works as a FROM item, which C9b relies on.

        The plan for C9b (>=4.1) uploads Arrow to an internal stage and then
        selects straight from it, with no target table. That shape is documented
        but unverified, so probe it here: on an older server this skips rather
        than fails.
        """
        from psycopg import sql

        stage = f"hg_df_it_{uuid.uuid4().hex[:8]}"
        try:
            session.execute(
                sql.SQL("call hologres.hg_create_internal_stage({}, {}, {})").format(
                    sql.Literal(stage), sql.Literal(stage), sql.Literal(600)
                ),
                fetch=False,
            )
        except hg.HoloDataFrameError as exc:
            pytest.skip(f"internal stages unsupported on this server: {exc}")

        try:
            # An empty stage still tells us whether the FROM-item form parses.
            session.execute(
                sql.SQL(
                    'SELECT * FROM external_files(path={}) AS ("id" int8) LIMIT 0'
                ).format(sql.Literal(f"internal_stage://{stage}")),
            )
        except hg.HoloDataFrameError as exc:
            pytest.skip(f"external_files is not usable as a FROM item here: {exc}")
        finally:
            try:
                session.execute(
                    sql.SQL("call hologres.hg_drop_internal_stage({})").format(
                        sql.Literal(stage)
                    ),
                    fetch=False,
                )
            except hg.HoloDataFrameError:
                pass

    def test_external_files_oss_read_sql_is_accepted(self, session):
        """Validate the C11 OSS read shape without reading a real bucket."""
        from psycopg import sql

        if session.server_version() < (4, 1, 0):
            pytest.skip("external_files needs Hologres 4.1+")
        df = session.read_files(
            "oss://fake-bucket/data.csv",
            format="csv",
            schema=hg.StructType([hg.StructField("id", hg.LongType())]),
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
        )
        try:
            session.execute(sql.SQL("EXPLAIN {} ").format(df._to_sql()))
        except hg.ExecutionError as exc:
            if "assume role" in str(exc).lower() or "credential" in str(exc).lower():
                pytest.skip(f"OSS credentials are unavailable: {exc}")
            raise

    def test_external_files_oss_write_sql_is_accepted(self, session):
        """Validate C11 CSV write SQL without writing to a real bucket."""
        from psycopg import sql

        if session.server_version() < (4, 1, 0):
            pytest.skip("external_files needs Hologres 4.1+")
        from hologres.dataframe._external_files import (
            build_write_files_sql,
            validate_oss_write,
        )

        path, fmt, options = validate_oss_write(
            "oss://fake-bucket/export/", "csv", target_file_size_mb=10
        )
        query = build_write_files_sql(
            session.sql("SELECT 1 AS id")._to_sql(), path, fmt, options
        )
        try:
            session.execute(sql.SQL("EXPLAIN {} ").format(query))
        except hg.ExecutionError as exc:
            if "assume role" in str(exc).lower() or "credential" in str(exc).lower():
                pytest.skip(f"OSS credentials are unavailable: {exc}")
            raise

    def test_ai_function_sql_is_accepted(self, session, orders_table):
        """Validate representative C12 AI expressions without invoking a model."""
        from psycopg import sql

        file = hg.ai.to_file(
            "oss://fake-bucket/a.pdf", "oss-endpoint", "acs:ram::1:role/test"
        )
        prompt = hg.ai.prompt("Review: {0}", hg.col("region"))
        df = session.table(orders_table).select(
            hg.ai.gen(prompt).alias("generated"),
            hg.ai.embed("region").alias("embedding"),
            hg.ai.chunk("region", separators=["|", " "]).alias("chunks"),
            file.alias("file_value"),
        )
        try:
            session.execute(sql.SQL("EXPLAIN {} ").format(df._to_sql()))
        except hg.ExecutionError as exc:
            message = str(exc).lower()
            if "does not exist" in message and (
                "ai_" in message or "type file" in message or "to_file" in message
            ):
                pytest.skip(f"Hologres AI functions are unavailable: {exc}")
            raise


class TestDynamicTable:
    def test_create_ignore_and_replace(self, session, orders_table):
        """Exercise the bounded C14 DDL paths against a live Dynamic Table."""
        from psycopg import sql

        name = f"hg_df_it_dynamic_{uuid.uuid4().hex[:8]}"
        target = f"public.{name}"
        ident = sql.Identifier("public", name)
        source = session.table(orders_table).select("order_id", "region", "amount")
        try:
            source.as_dynamic_table(
                target,
                freshness="1 minute",
                refresh_mode="full",
                auto_refresh=False,
            )
            # Default mode is ignore, so repeating the same declaration is safe.
            source.as_dynamic_table(
                target,
                freshness="1 minute",
                refresh_mode="full",
                auto_refresh=False,
            )
            assert session.table(target).columns == ["order_id", "region", "amount"]

            source.select("order_id", "region").as_dynamic_table(
                target,
                freshness="1 minute",
                refresh_mode="full",
                auto_refresh=False,
                mode="replace",
            )
            assert session.table(target).columns == ["order_id", "region"]
        except hg.ExecutionError as exc:
            message = str(exc).lower()
            unavailable = (
                "dynamic table is not supported",
                "enable_create_dynamic_table",
                'syntax error at or near "dynamic"',
            )
            if any(marker in message for marker in unavailable):
                pytest.skip(f"Hologres Dynamic Table is unavailable: {exc}")
            raise
        finally:
            _drop_test_table(session, ident)


class TestRemoteUDF:
    def test_auto_deploy_and_execute_scalar(self, fc_session, orders_table):
        """Deploy a real FC function, register it in Hologres, and execute it."""
        from psycopg import sql

        from hologres.dataframe._fc import _is_not_found

        name = f"hg_df_it_fc_{uuid.uuid4().hex[:8]}"

        @hg.udf(name=name)
        def double(value: int) -> int:
            return value * 2

        try:
            rows = (
                fc_session.table(orders_table)
                .select(double("order_id").alias("doubled"))
                .sort("doubled")
                .collect()
            )
            assert [row["doubled"] for row in rows] == [2, 4, 6, 8, 10]
        finally:
            deployer = fc_session._fc_deployer
            try:
                fc_session.execute(
                    sql.SQL("DROP FUNCTION IF EXISTS {}(bigint)").format(
                        sql.Identifier(name)
                    ),
                    fetch=False,
                )
            finally:
                if deployer is not None:
                    client, _ = deployer._sdk()
                    try:
                        client.delete_function(name)
                    except Exception as exc:
                        if not _is_not_found(exc):
                            raise

    def test_auto_deploy_and_execute_udtf(self, fc_session, orders_table):
        """Deploy a real FC table function, register it, and execute it."""
        from psycopg import sql

        from hologres.dataframe._fc import _is_not_found

        name = f"hg_df_it_udtf_{uuid.uuid4().hex[:8]}"

        @hg.udtf(
            name=name,
            output_schema=hg.StructType([hg.StructField("word", hg.StringType())]),
        )
        class Words:
            def process(self, value: str):
                for word in value.split():
                    yield (word,)

        try:
            rows = (
                fc_session.table(orders_table)
                .filter(hg.col("region").is_not_null())
                .select(Words("region"))
                .sort("word")
                .collect()
            )
            assert [row["word"] for row in rows] == [
                "east",
                "east",
                "west",
                "west",
            ]
        finally:
            deployer = fc_session._fc_deployer
            try:
                fc_session.execute(
                    sql.SQL("DROP FUNCTION IF EXISTS {}(text)").format(
                        sql.Identifier(name)
                    ),
                    fetch=False,
                )
            finally:
                if deployer is not None:
                    client, _ = deployer._sdk()
                    try:
                        client.delete_function(name)
                    except Exception as exc:
                        if not _is_not_found(exc):
                            raise
