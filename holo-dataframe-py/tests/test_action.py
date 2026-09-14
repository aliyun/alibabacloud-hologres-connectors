"""Tests for DataFrame actions and schema pinning/propagation."""

import io
from contextlib import redirect_stdout

import pytest

import hologres.dataframe as hg
from hologres.dataframe.exceptions import ExecutionError, InvalidArgumentError
from tests._fake_conn import (
    OID_FLOAT8,
    OID_INT4,
    OID_INT8,
    OID_TEXT,
    FakeColumnInfo,
    make_session,
)

# Schema returned by information_schema.columns for the fixture table below.
_ORDERS_SCHEMA_ROWS = [
    ("id", "bigint", "int8"),
    ("region", "text", "text"),
    ("amount", "double precision", "float8"),
]
_ORDERS_SCHEMA_COLS = [
    FakeColumnInfo("column_name", OID_TEXT),
    FakeColumnInfo("data_type", OID_TEXT),
    FakeColumnInfo("udt_name", OID_TEXT),
]


def orders_table_script():
    """The first cursor response for `session.table('public.orders')`."""
    return (_ORDERS_SCHEMA_ROWS, _ORDERS_SCHEMA_COLS)


class TestSchemaPinning:
    def test_table_pins_schema(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders")
        assert df.schema.names == ("id", "region", "amount")
        assert df._schema is df.schema

    def test_columns_returns_names(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders")
        assert df.columns == ["id", "region", "amount"]

    def test_filter_propagates_schema_untouched(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").filter(hg.col("amount") > 100)
        assert df.schema.names == ("id", "region", "amount")

    def test_sort_limit_offset_propagate_schema(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").sort("amount").limit(10).offset(5)
        assert df.schema.names == ("id", "region", "amount")

    def test_select_narrows_schema(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").select("id", "amount")
        assert df.schema.names == ("id", "amount")
        assert df.schema["id"].datatype == hg.LongType()
        assert df.schema["amount"].datatype == hg.DoubleType()

    def test_select_preserves_types_from_parent(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").select("region")
        # region was text upstream, must still be text.
        assert df.schema["region"].datatype == hg.StringType()

    def test_select_derived_column_gets_placeholder_type(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").select(
            (hg.col("amount") * 0.06).alias("tax")
        )
        # A pure derived column doesn't know its true type yet; placeholder.
        assert df.schema.names == ("tax",)
        assert df.schema["tax"].datatype == hg.StringType()

    def test_select_unnamed_derived_column_rejected(self):
        session, _ = make_session(script=[orders_table_script()])
        # No .alias() -> ambiguous name for the result column -> reject.
        with pytest.raises(hg.InvalidArgumentError, match=r"\.alias"):
            session.table("public.orders").select(hg.col("amount") * 0.06)

    def test_select_unknown_column_rejected(self):
        session, _ = make_session(script=[orders_table_script()])
        with pytest.raises(hg.InvalidArgumentError, match="not in schema"):
            session.table("public.orders").select("no_such_col")

    def test_select_can_rename_via_alias(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").select(hg.col("region").alias("r"))
        assert df.schema.names == ("r",)
        # Type is preserved from the source column.
        assert df.schema["r"].datatype == hg.StringType()

    def test_select_rename_unknown_source_rejected(self):
        session, _ = make_session(script=[orders_table_script()])
        with pytest.raises(hg.InvalidArgumentError, match="not in schema"):
            session.table("public.orders").select(hg.col("nope").alias("n"))

    def test_with_column_appends_placeholder(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").with_column("tax", hg.col("amount") * 0.06)
        assert df.schema.names == ("id", "region", "amount", "tax")
        assert df.schema["amount"].datatype == hg.DoubleType()

    def test_with_columns_appends_all_keys(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").with_columns(
            {"tax": hg.col("amount") * 0.06, "big": hg.col("amount") > 100}
        )
        assert df.schema.names == (
            "id",
            "region",
            "amount",
            "tax",
            "big",
        )

    def test_no_session_and_no_pin_schema_errors(self):
        from hologres.table_name import TableName

        from hologres.dataframe import _plan
        from hologres.dataframe.dataframe import DataFrame

        df = DataFrame(_plan.TableScan(TableName.valueOf("t")))  # no session, no pin
        with pytest.raises(InvalidArgumentError, match="without a session"):
            _ = df.schema

    def test_sql_source_lazy_describe_and_caches(self):
        # First response: the describe for LIMIT 0. Second: not needed.
        session, conn = make_session(
            script=[
                (
                    [],
                    [FakeColumnInfo("a", OID_INT4), FakeColumnInfo("b", OID_TEXT)],
                )
            ]
        )
        df = session.sql("SELECT a, b FROM t")
        # First access issues describe.
        s1 = df.schema
        assert s1.names == ("a", "b")
        first_calls = len(conn.recorded)
        # Second access uses the cached schema; no extra query.
        s2 = df.schema
        assert s2 is s1
        assert len(conn.recorded) == first_calls


class TestSchemaPropagationRoundTrip:
    def test_drop_duplicates_keeps_schema(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").drop_duplicates(["region"])
        assert df.schema.names == ("id", "region", "amount")

    def test_distinct_keeps_schema(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").distinct()
        assert df.schema.names == ("id", "region", "amount")

    def test_dropna_keeps_schema(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders").dropna(["amount"])
        assert df.schema.names == ("id", "region", "amount")


class TestCollect:
    def test_returns_rows_with_field_names(self):
        session, _ = make_session(
            script=[
                orders_table_script(),
                (
                    [(1, "east", 12.5), (2, "west", 33.0)],
                    [
                        FakeColumnInfo("id", OID_INT8),
                        FakeColumnInfo("region", OID_TEXT),
                        FakeColumnInfo("amount", OID_FLOAT8),
                    ],
                ),
            ]
        )
        rows = session.table("public.orders").collect()
        assert len(rows) == 2
        assert rows[0]["region"] == "east"
        assert rows[1].amount == 33.0

    def test_without_session_errors(self):
        from hologres.table_name import TableName

        from hologres.dataframe import _plan
        from hologres.dataframe.dataframe import DataFrame

        df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
        with pytest.raises(InvalidArgumentError, match="no session"):
            df.collect()


class TestShow:
    def _prepared(self, rows, cols=None):
        cols = cols or [
            FakeColumnInfo("id", OID_INT8),
            FakeColumnInfo("region", OID_TEXT),
        ]
        return make_session(script=[orders_table_script(), (rows, cols)])

    def test_prints_table_frame(self):
        session, _ = self._prepared([(1, "east"), (2, "west")])
        buf = io.StringIO()
        with redirect_stdout(buf):
            session.table("public.orders").select("id", "region").show()
        out = buf.getvalue()
        assert "east" in out and "west" in out
        # frame markers
        assert "+---" in out and "|" in out
        assert "(2 rows)" in out

    def test_singular_row_wording(self):
        session, _ = self._prepared([(1, "east")])
        buf = io.StringIO()
        with redirect_stdout(buf):
            session.table("public.orders").select("id", "region").show()
        assert "(1 row)" in buf.getvalue()

    def test_empty_result(self):
        session, _ = self._prepared([])
        buf = io.StringIO()
        with redirect_stdout(buf):
            session.table("public.orders").select("id", "region").show()
        assert "(0 rows)" in buf.getvalue()

    def test_truncates_long_values(self):
        session, _ = self._prepared([(1, "x" * 100)])
        buf = io.StringIO()
        with redirect_stdout(buf):
            session.table("public.orders").select("id", "region").show(truncate=10)
        # 10 chars max = 7 x's + "..."
        assert "xxxxxxx..." in buf.getvalue()

    def test_no_truncation_when_truncate_zero(self):
        session, _ = self._prepared([(1, "x" * 30)])
        buf = io.StringIO()
        with redirect_stdout(buf):
            session.table("public.orders").select("id", "region").show(truncate=0)
        assert "x" * 30 in buf.getvalue()

    def test_null_values_shown_as_NULL(self):
        session, _ = self._prepared([(1, None)])
        buf = io.StringIO()
        with redirect_stdout(buf):
            session.table("public.orders").select("id", "region").show()
        assert "NULL" in buf.getvalue()

    @pytest.mark.parametrize("bad", [-1, "3", True])
    def test_bad_n_rejected(self, bad):
        session, _ = make_session(script=[orders_table_script()])
        with pytest.raises(InvalidArgumentError, match="n must be a non-negative int"):
            session.table("public.orders").show(n=bad)

    @pytest.mark.parametrize("bad", [-1, "5", True])
    def test_bad_truncate_rejected(self, bad):
        session, _ = make_session(script=[orders_table_script()])
        with pytest.raises(
            InvalidArgumentError, match="truncate must be a non-negative int"
        ):
            session.table("public.orders").show(truncate=bad)


class TestCount:
    def test_compiles_count_query(self):
        session, conn = make_session(
            script=[
                orders_table_script(),
                ([(42,)], [FakeColumnInfo("c", OID_INT8)]),
            ]
        )
        n = session.table("public.orders").count()
        assert n == 42
        # The count SQL wraps the source.
        assert "SELECT COUNT(*)" in conn.recorded[-1]

    def test_no_session_errors(self):
        from hologres.table_name import TableName

        from hologres.dataframe import _plan
        from hologres.dataframe.dataframe import DataFrame

        df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
        with pytest.raises(InvalidArgumentError, match="no session"):
            df.count()


class TestFirstAndTake:
    def test_first_returns_row(self):
        session, _ = make_session(
            script=[
                orders_table_script(),
                (
                    [(1, "east", 12.5)],
                    [
                        FakeColumnInfo("id", OID_INT8),
                        FakeColumnInfo("region", OID_TEXT),
                        FakeColumnInfo("amount", OID_FLOAT8),
                    ],
                ),
            ]
        )
        row = session.table("public.orders").first()
        assert row["id"] == 1

    def test_first_empty_returns_none(self):
        session, _ = make_session(
            script=[
                orders_table_script(),
                (
                    [],
                    [
                        FakeColumnInfo("id", OID_INT8),
                        FakeColumnInfo("region", OID_TEXT),
                        FakeColumnInfo("amount", OID_FLOAT8),
                    ],
                ),
            ]
        )
        assert session.table("public.orders").first() is None

    def test_take_returns_list(self):
        session, conn = make_session(
            script=[
                orders_table_script(),
                (
                    [(1,), (2,)],
                    [FakeColumnInfo("id", OID_INT8)],
                ),
            ]
        )
        rows = session.table("public.orders").select("id").take(2)
        assert len(rows) == 2
        # Take injects a LIMIT.
        assert "LIMIT 2" in conn.recorded[-1]


class TestToPandas:
    def test_returns_pandas_dataframe(self):
        pytest.importorskip("pandas")
        session, _ = make_session(
            script=[
                orders_table_script(),
                (
                    [(1, "east", 12.5), (2, "west", 33.0)],
                    [
                        FakeColumnInfo("id", OID_INT8),
                        FakeColumnInfo("region", OID_TEXT),
                        FakeColumnInfo("amount", OID_FLOAT8),
                    ],
                ),
            ]
        )
        pdf = session.table("public.orders").to_pandas()
        assert list(pdf.columns) == ["id", "region", "amount"]
        assert pdf.iloc[1]["amount"] == 33.0

    def test_missing_pandas_raises_helpful_message(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pandas":
                raise ImportError("No module named 'pandas'")
            return real_import(name, *args, **kwargs)

        session, _ = make_session(
            script=[
                orders_table_script(),
                ([], [FakeColumnInfo("id", OID_INT8)]),
            ]
        )
        df = session.table("public.orders").select("id")
        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(ImportError, match="pandas is required"):
            df.to_pandas()


class TestIterRows:
    def test_iterates_rows_in_batches(self):
        session, conn = make_session(
            script=[
                orders_table_script(),
                (
                    [(1,), (2,), (3,)],
                    [FakeColumnInfo("id", OID_INT8)],
                ),
            ]
        )
        rows = list(session.table("public.orders").select("id").iter_rows())
        assert [r[0] for r in rows] == [1, 2, 3]
        # No server-side cursor: Hologres only allows DECLARE CURSOR inside a
        # transaction, and pooled connections here are autocommit.
        assert all(cur.name is None for cur in conn.cursors)

    def test_small_batch_size_spans_several_fetches(self):
        session, _ = make_session(
            script=[
                orders_table_script(),
                ([(1,), (2,), (3,)], [FakeColumnInfo("id", OID_INT8)]),
            ]
        )
        df = session.table("public.orders").select("id")
        assert [r[0] for r in df.iter_rows(batch_size=2)] == [1, 2, 3]

    @pytest.mark.parametrize("bad", [0, -1])
    def test_non_positive_batch_size_rejected(self, bad):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders")
        with pytest.raises(InvalidArgumentError, match="must be positive"):
            df.iter_rows(batch_size=bad)

    def test_non_int_batch_size_rejected(self):
        session, _ = make_session(script=[orders_table_script()])
        df = session.table("public.orders")
        with pytest.raises(InvalidArgumentError, match="must be an int"):
            df.iter_rows(batch_size="10")

    def test_no_session_errors(self):
        from hologres.table_name import TableName

        from hologres.dataframe import _plan
        from hologres.dataframe.dataframe import DataFrame

        df = DataFrame(_plan.TableScan(TableName.valueOf("t")))
        with pytest.raises(InvalidArgumentError, match="no session"):
            next(df.iter_rows())


class TestErrorPropagation:
    def test_execute_error_surfaces_as_execution_error(self):
        import psycopg

        from hologres.dataframe.session import _session_from_connection

        class Raising:
            closed = False

            def cursor(self, name=None):
                raise psycopg.errors.UndefinedTable("relation does not exist")

            def close(self):
                self.closed = True

        session = _session_from_connection(Raising(), dbname="d")
        with pytest.raises(ExecutionError, match="relation does not exist"):
            session.table("public.nope")
