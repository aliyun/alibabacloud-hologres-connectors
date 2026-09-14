"""Tests for the internal expression tree (hologres.dataframe._expr)."""

import pytest
from psycopg import sql

from hologres.dataframe import _expr as e


def rendered(node) -> str:
    return node.to_sql().as_string()


class TestColumnRef:
    def test_unqualified(self):
        assert rendered(e.ColumnRef("amount")) == '"amount"'

    def test_qualified(self):
        assert rendered(e.ColumnRef("user_id", "a")) == '"a"."user_id"'

    def test_output_name(self):
        assert e.ColumnRef("amount").output_name == "amount"
        assert e.ColumnRef("x", "t").output_name == "x"

    def test_special_chars_are_quoted(self):
        assert rendered(e.ColumnRef('weird "name"')) == '"weird ""name"""'


class TestLiteral:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (100, "100"),
            (0.06, "0.06"),
            (True, "true"),
            (False, "false"),
            (None, "NULL"),
            ("east", "'east'"),
            ("a'b", "'a''b'"),
        ],
    )
    def test_scalars(self, value, expected):
        assert rendered(e.Literal(value)) == expected

    def test_list_becomes_array(self):
        assert rendered(e.Literal([1, 2, 3])) == "'{1,2,3}'::int2[]"

    def test_output_name_is_none(self):
        assert e.Literal(1).output_name is None


class TestBinaryExpr:
    def test_parenthesized(self):
        node = e.BinaryExpr(e.ColumnRef("a"), "+", e.Literal(1))
        assert rendered(node) == '("a" + 1)'

    def test_nested(self):
        inner = e.BinaryExpr(e.ColumnRef("a"), "+", e.ColumnRef("b"))
        outer = e.BinaryExpr(inner, "*", e.Literal(2))
        assert rendered(outer) == '(("a" + "b") * 2)'

    def test_output_name_is_none(self):
        assert e.BinaryExpr(e.ColumnRef("a"), "+", e.Literal(1)).output_name is None


class TestUnaryExpr:
    def test_prefix(self):
        assert rendered(e.UnaryExpr("NOT", e.ColumnRef("flag"))) == '(NOT "flag")'

    def test_postfix(self):
        node = e.UnaryExpr("IS NULL", e.ColumnRef("a"), postfix=True)
        assert rendered(node) == '("a" IS NULL)'


class TestPredicateExpr:
    def test_between(self):
        node = e.PredicateExpr(
            sql.SQL("({} BETWEEN {} AND {})"),
            [e.ColumnRef("a"), e.Literal(1), e.Literal(9)],
        )
        assert rendered(node) == '("a" BETWEEN 1 AND 9)'

    def test_zero_operand_template(self):
        assert rendered(e.PredicateExpr(sql.SQL("FALSE"), [])) == "FALSE"


class TestCastExpr:
    def test_simple(self):
        assert rendered(e.CastExpr(e.ColumnRef("b"), "integer")) == '("b")::integer'

    def test_parametric_type(self):
        node = e.CastExpr(e.ColumnRef("x"), "numeric(10,2)")
        assert rendered(node) == '("x")::numeric(10,2)'


class TestSortExpr:
    def test_default_ascending(self):
        assert rendered(e.SortExpr(e.ColumnRef("a"))) == '"a" ASC'

    def test_descending(self):
        assert rendered(e.SortExpr(e.ColumnRef("amount"), ascending=False)) == (
            '"amount" DESC'
        )

    def test_nulls_first(self):
        assert rendered(e.SortExpr(e.ColumnRef("a"), nulls="first")) == (
            '"a" ASC NULLS FIRST'
        )

    def test_nulls_last(self):
        assert (
            rendered(e.SortExpr(e.ColumnRef("a"), ascending=False, nulls="last"))
            == '"a" DESC NULLS LAST'
        )
