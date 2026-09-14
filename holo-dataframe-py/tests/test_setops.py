"""Tests for set operations and lateral expansion: union / union_all / explode.

Everything runs off pinned schemas built from a FakeConnection, so no database
is involved. Fixture tables:

    public.a(id bigint, name text)
    public.b(id bigint, name text)         -- same shape as a
    public.wide(id bigint, name text, extra text)   -- one more column than a
    public.docs(id bigint, tags text[])    -- an array column to explode
"""

import pytest

import hologres.dataframe as hg
from hologres.dataframe import _expr, _plan
from hologres.dataframe.dataframe import DataFrame
from hologres.dataframe.exceptions import InvalidArgumentError
from tests._fake_conn import OID_TEXT, FakeColumnInfo, make_session

_INFO_COLS = [
    FakeColumnInfo("column_name", OID_TEXT),
    FakeColumnInfo("data_type", OID_TEXT),
    FakeColumnInfo("udt_name", OID_TEXT),
]

_TABLES = {
    "a": [("id", "bigint", "int8"), ("name", "text", "text")],
    "b": [("id", "bigint", "int8"), ("name", "text", "text")],
    "wide": [
        ("id", "bigint", "int8"),
        ("name", "text", "text"),
        ("extra", "text", "text"),
    ],
    "renamed": [("uid", "bigint", "int8"), ("label", "text", "text")],
    "docs": [("id", "bigint", "int8"), ("tags", "ARRAY", "_text")],
    "multi": [
        ("id", "bigint", "int8"),
        ("tags", "ARRAY", "_text"),
        ("scores", "ARRAY", "_int8"),
    ],
}


def tables(*names):
    """Pin the named tables from one shared session, in order."""
    session, _ = make_session(script=[(_TABLES[n], _INFO_COLS) for n in names])
    return [session.table(f"public.{n}") for n in names]


def sql_of(df: DataFrame) -> str:
    return df._to_sql().as_string()


def bare(name: str) -> DataFrame:
    """A DataFrame with no session and no pinned schema."""
    from hologres.table_name import TableName

    return DataFrame(_plan.TableScan(TableName.valueOf(f"public.{name}")))


class TestUnion:
    def test_union_dedups(self):
        a, b = tables("a", "b")
        out = sql_of(a.union(b))
        assert out == (
            'SELECT * FROM ((SELECT * FROM "public"."a")'
            ' UNION (SELECT * FROM "public"."b")) "t0"'
        )

    def test_union_all_keeps_duplicates(self):
        a, b = tables("a", "b")
        out = sql_of(a.union_all(b))
        assert '(SELECT * FROM "public"."a") UNION ALL (SELECT' in out

    def test_each_arm_is_parenthesized_to_scope_limit(self):
        a, b = tables("a", "b")
        out = sql_of(a.limit(5).union_all(b))
        # The LIMIT must bind to the left arm, so it sits inside that arm's
        # parentheses rather than trailing the whole union.
        assert "LIMIT 5)" in out
        assert out.count("LIMIT") == 1

    def test_schema_taken_from_left(self):
        a, renamed = tables("a", "renamed")
        # Different names on the right are accepted; the output uses left names.
        out = a.union(renamed)
        assert out.columns == ["id", "name"]

    def test_mismatched_column_count_rejected(self):
        a, wide = tables("a", "wide")
        with pytest.raises(InvalidArgumentError, match="matching column counts"):
            a.union(wide)

    def test_unknown_schema_skips_the_count_check(self):
        joined = bare("a").union(bare("wide"))
        assert joined._schema is None
        assert "UNION" in sql_of(joined)

    def test_unknown_schema_is_not_described_during_construction(self):
        session, conn = make_session()
        left = session.sql("SELECT 1 AS id")
        right = session.sql("SELECT 2 AS id")

        joined = left.union(right)

        assert joined._schema is None
        assert conn.recorded == []

    def test_filter_fuses_into_the_outer_select(self):
        a, b = tables("a", "b")
        out = sql_of(a.union(b).filter(hg.col("id") > 1))
        assert out.count("UNION") == 1
        assert out.rstrip().endswith('WHERE ("id" > 1)')

    def test_select_fuses_into_the_outer_select(self):
        a, b = tables("a", "b")
        out = sql_of(a.union(b).select("id"))
        assert out.startswith('SELECT "id" FROM ((SELECT')

    def test_chained_unions(self):
        a, b, c = tables("a", "b", "a")
        out = sql_of(a.union(b).union_all(c))
        assert "UNION" in out and "UNION ALL" in out

    def test_other_must_be_a_dataframe(self):
        (a,) = tables("a")
        with pytest.raises(InvalidArgumentError, match="expects a DataFrame"):
            a.union("b")

    def test_different_sessions_rejected(self):
        (a,) = tables("a")
        (b,) = tables("b")
        with pytest.raises(InvalidArgumentError, match="different sessions"):
            a.union(b)

    def test_session_inherited_from_either_side(self):
        (a,) = tables("a")
        assert bare("b").union(a)._session is a._session
        assert a.union(bare("b"))._session is a._session

    def test_immutability(self):
        a, b = tables("a", "b")
        before = sql_of(a)
        a.union(b)
        assert sql_of(a) == before


class TestExplode:
    def test_compiles_to_select_list_unnest(self):
        (docs,) = tables("docs")
        out = sql_of(docs.explode("tags"))
        assert out == ('SELECT "id", unnest("tags") AS "tags" FROM "public"."docs"')

    def test_outer_substitutes_a_one_element_null_array(self):
        (docs,) = tables("docs")
        out = sql_of(docs.explode("tags", outer=True))
        # LATERAL is unavailable, so an empty/NULL array is replaced with a
        # single NULL element to keep the row.
        assert (
            "unnest(coalesce(nullif(\"tags\", ('{}')::text[]), "
            "('{NULL}')::text[])) AS \"tags\"" in out
        )

    def test_column_order_and_name_preserved(self):
        (docs,) = tables("docs")
        assert docs.explode("tags").columns == ["id", "tags"]

    def test_element_type_replaces_array_type(self):
        (docs,) = tables("docs")
        assert docs.explode("tags").schema["tags"].datatype == hg.StringType()

    def test_outer_marks_the_column_nullable(self):
        (docs,) = tables("docs")
        assert docs.explode("tags", outer=True).schema["tags"].nullable is True

    def test_unknown_column(self):
        (docs,) = tables("docs")
        with pytest.raises(InvalidArgumentError, match="not in schema"):
            docs.explode("missing")

    def test_non_array_column(self):
        (docs,) = tables("docs")
        with pytest.raises(InvalidArgumentError, match="needs an array column"):
            docs.explode("id")

    def test_requires_pinned_schema(self):
        with pytest.raises(InvalidArgumentError, match="needs a pinned schema"):
            bare("docs").explode("tags")

    def test_no_lateral_is_emitted(self):
        # Hologres rejects LATERAL in any form, including `FROM t, unnest(...)`.
        (docs,) = tables("docs")
        for out in (
            sql_of(docs.explode("tags")),
            sql_of(docs.explode("tags", outer=True)),
        ):
            assert "LATERAL" not in out.upper()

    def test_filter_after_explode_nests(self):
        (docs,) = tables("docs")
        # explode emits an explicit projection, so a filter on top follows the
        # conservative rule and wraps it in an outer SELECT.
        out = sql_of(docs.explode("tags").filter(hg.col("tags") == "x"))
        assert out.count("SELECT") == 2
        assert out.rstrip().endswith("WHERE (\"tags\" = 'x')")

    def test_explode_fuses_with_an_upstream_filter(self):
        (docs,) = tables("docs")
        # The projection sits directly on the filter, and WHERE runs before the
        # SELECT list, so filtering base rows then expanding needs no subquery.
        out = sql_of(docs.filter(hg.col("id") > 1).explode("tags"))
        assert out == (
            'SELECT "id", unnest("tags") AS "tags" '
            'FROM "public"."docs" WHERE ("id" > 1)'
        )

    def test_two_successive_explodes(self):
        (multi,) = tables("multi")
        out = sql_of(multi.explode("tags").explode("scores"))
        # Each explode gets its own SELECT level, so the expansions compose
        # instead of being zipped as two SRFs in one SELECT list would be.
        assert out.count("unnest(") == 2
        assert out.count("SELECT") == 2
        assert multi.explode("tags").explode("scores").columns == [
            "id",
            "tags",
            "scores",
        ]

    def test_immutability(self):
        (docs,) = tables("docs")
        before = sql_of(docs)
        docs.explode("tags")
        assert sql_of(docs) == before


class TestPlanNodes:
    def test_union_node_fields(self):
        left, right = bare("a")._plan, bare("b")._plan
        node = _plan.Union(left, right, distinct=True)
        assert node.left is left and node.right is right and node.distinct is True

    def test_iter_nodes_walks_union(self):
        left, right = bare("a")._plan, bare("b")._plan
        node = _plan.Union(left, right, distinct=False)
        assert list(_plan.iter_nodes(node)) == [node, left, right]

    def test_window_expr_renders_partition_by(self):
        node = _expr.WindowExpr("row_number", [], [_expr.ColumnRef("region")])
        assert node.to_sql().as_string() == (
            'row_number() OVER (PARTITION BY "region")'
        )

    def test_window_expr_rewrites_relations(self):
        rel = object()
        node = _expr.WindowExpr(
            "row_number", [], [_expr.ColumnRef("region", relation=rel)]
        )
        bound = _expr.rewrite_relations(node, {id(rel): "t1"})
        assert bound.to_sql().as_string() == (
            'row_number() OVER (PARTITION BY "t1"."region")'
        )
