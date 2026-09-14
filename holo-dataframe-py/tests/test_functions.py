"""Tests for col() and lit()."""

import pytest

import hologres.dataframe as hg
from hologres.dataframe.column import Column
from hologres.dataframe.functions import col, lit


class TestCol:
    def test_returns_column(self):
        assert isinstance(col("amount"), Column)

    def test_renders_identifier(self):
        assert col("amount")._to_sql().as_string() == '"amount"'

    def test_case_sensitive_quoting(self):
        assert col("Amount")._to_sql().as_string() == '"Amount"'

    def test_output_name(self):
        assert col("amount").output_name == "amount"

    @pytest.mark.parametrize("bad", ["", None, 123])
    def test_rejects_bad_name(self, bad):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty string"):
            col(bad)


class TestLit:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (100, "100"),
            (0.06, "0.06"),
            (True, "true"),
            (None, "NULL"),
            ("region", "'region'"),
        ],
    )
    def test_renders_literal(self, value, expected):
        assert lit(value)._to_sql().as_string() == expected

    def test_returns_column(self):
        assert isinstance(lit(1), Column)

    def test_string_value_vs_column_name(self):
        # lit("region") is the text value; col("region") is the column.
        assert lit("region")._to_sql().as_string() == "'region'"
        assert col("region")._to_sql().as_string() == '"region"'

    def test_standalone_constant_column(self):
        assert lit(1).alias("const")._to_projection_sql().as_string() == '1 AS "const"'

    def test_reachable_from_facade(self):
        assert hg.col is col
        assert hg.lit is lit
