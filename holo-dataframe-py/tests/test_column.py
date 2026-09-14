"""Tests for the public Column API: operators, methods, and guards."""

import pytest

import hologres.dataframe as hg
from hologres.dataframe.column import Column
from hologres.dataframe.functions import col, lit


def sql_of(column: Column) -> str:
    return column._to_sql().as_string()


class TestArithmeticOperators:
    @pytest.mark.parametrize(
        "column, expected",
        [
            (col("a") + col("b"), '("a" + "b")'),
            (col("a") - col("b"), '("a" - "b")'),
            (col("a") * col("b"), '("a" * "b")'),
            (col("a") / col("b"), '("a" / "b")'),
            (col("a") % col("b"), '("a" % "b")'),
        ],
    )
    def test_binary_between_columns(self, column, expected):
        assert sql_of(column) == expected

    @pytest.mark.parametrize(
        "column, expected",
        [
            (col("amount") * 0.06, '("amount" * 0.06)'),
            (col("a") + 1, '("a" + 1)'),
            (col("a") - 2, '("a" - 2)'),
        ],
    )
    def test_bare_value_auto_promoted(self, column, expected):
        assert sql_of(column) == expected

    @pytest.mark.parametrize(
        "column, expected",
        [
            (1 + col("a"), '(1 + "a")'),
            (10 - col("a"), '(10 - "a")'),
            (2 * col("a"), '(2 * "a")'),
            (100 / col("a"), '(100 / "a")'),
            (7 % col("a"), '(7 % "a")'),
        ],
    )
    def test_reflected_operators(self, column, expected):
        assert sql_of(column) == expected

    def test_unary_negation(self):
        assert sql_of(-col("a")) == '(- "a")'

    def test_chained_arithmetic_nests(self):
        assert sql_of((col("a") + col("b")) * 2) == '(("a" + "b") * 2)'


class TestComparisonOperators:
    @pytest.mark.parametrize(
        "column, expected",
        [
            (col("a") == 1, '("a" = 1)'),
            (col("a") != 1, '("a" <> 1)'),
            (col("a") > 1, '("a" > 1)'),
            (col("a") >= 1, '("a" >= 1)'),
            (col("a") < 1, '("a" < 1)'),
            (col("a") <= 1, '("a" <= 1)'),
        ],
    )
    def test_comparisons(self, column, expected):
        assert sql_of(column) == expected

    def test_string_value_is_literal_not_column(self):
        assert sql_of(col("region") == "east") == "(\"region\" = 'east')"

    def test_comparison_returns_column_not_bool(self):
        result = col("a") == col("b")
        assert isinstance(result, Column)
        assert sql_of(result) == '("a" = "b")'


class TestLogicalOperators:
    def test_and(self):
        expr = (col("amount") > 100) & (col("region") == "east")
        assert sql_of(expr) == '(("amount" > 100) AND ("region" = \'east\'))'

    def test_or(self):
        expr = (col("a") > 1) | (col("b") < 2)
        assert sql_of(expr) == '(("a" > 1) OR ("b" < 2))'

    def test_not(self):
        assert sql_of(~(col("a") > 1)) == '(NOT ("a" > 1))'

    def test_reflected_and(self):
        # Bare value on the left dispatches to Column.__rand__.
        assert sql_of(True & (col("a") > 1)) == '(true AND ("a" > 1))'

    def test_reflected_or(self):
        assert sql_of(False | (col("b") < 2)) == '(false OR ("b" < 2))'

    def test_nested_logic(self):
        expr = (col("a") > 1) & ((col("b") < 2) | (col("c") == 3))
        assert sql_of(expr) == ('(("a" > 1) AND (("b" < 2) OR ("c" = 3)))')


class TestGuards:
    def test_bool_raises_with_guidance(self):
        with pytest.raises(hg.InvalidArgumentError, match=r"& \| ~"):
            bool(col("a"))

    def test_and_keyword_triggers_bool_guard(self):
        # `col('a') and col('b')` evaluates bool(col('a')) first.
        with pytest.raises(hg.InvalidArgumentError):
            col("a") and col("b")

    def test_column_is_unhashable(self):
        with pytest.raises(TypeError):
            hash(col("a"))

    def test_column_not_usable_in_set(self):
        with pytest.raises(TypeError):
            {col("a")}


class TestAlias:
    def test_projection_with_alias(self):
        assert col("amount").alias("amt")._to_projection_sql().as_string() == (
            '"amount" AS "amt"'
        )

    def test_projection_without_alias_is_bare(self):
        assert col("amount")._to_projection_sql().as_string() == '"amount"'

    def test_expression_projection_with_alias(self):
        assert (col("a") * 0.06).alias("tax")._to_projection_sql().as_string() == (
            '("a" * 0.06) AS "tax"'
        )

    def test_output_name_prefers_alias(self):
        assert col("a").alias("b").output_name == "b"
        assert (col("a") + 1).alias("s").output_name == "s"
        assert (col("a") + 1).output_name is None

    @pytest.mark.parametrize("bad", ["", None, 5])
    def test_alias_rejects_bad_name(self, bad):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty string"):
            col("a").alias(bad)


class TestSort:
    def test_asc(self):
        assert col("a").asc()._to_order_sql().as_string() == '"a" ASC'

    def test_desc(self):
        assert col("amount").desc()._to_order_sql().as_string() == '"amount" DESC'

    @pytest.mark.parametrize(
        "column, expected",
        [
            (col("a").asc_nulls_first(), '"a" ASC NULLS FIRST'),
            (col("a").asc_nulls_last(), '"a" ASC NULLS LAST'),
            (col("a").desc_nulls_first(), '"a" DESC NULLS FIRST'),
            (col("a").desc_nulls_last(), '"a" DESC NULLS LAST'),
        ],
    )
    def test_nulls_variants(self, column, expected):
        assert column._to_order_sql().as_string() == expected

    def test_order_sql_default_direction(self):
        # A plain column with no explicit sort obeys the compiler's default.
        assert col("a")._to_order_sql(default_ascending=False).as_string() == '"a" DESC'
        assert col("a")._to_order_sql(default_ascending=True).as_string() == '"a" ASC'

    def test_explicit_sort_overrides_default(self):
        assert col("a").desc()._to_order_sql(default_ascending=True).as_string() == (
            '"a" DESC'
        )

    def test_resort_replaces_previous_direction(self):
        assert col("a").asc().desc()._to_order_sql().as_string() == '"a" DESC'

    def test_output_name_preserved_after_sort(self):
        assert col("a").asc().output_name == "a"
        assert col("a").desc().output_name == "a"

    @pytest.mark.parametrize(
        "operation",
        [
            lambda c: c + 1,
            lambda c: c > 1,
            lambda c: c & col("b"),
            lambda c: -c,
            lambda c: ~c,
            lambda c: c.alias("x"),
            lambda c: c.cast(hg.IntegerType()),
            lambda c: c.is_null(),
            lambda c: c.is_not_null(),
            lambda c: c.isin(1, 2),
            lambda c: c.like("a%"),
            lambda c: c.ilike("a%"),
            lambda c: c.between(1, 2),
        ],
    )
    def test_sort_column_rejected_in_other_operations(self, operation):
        sorted_col = col("a").desc()
        with pytest.raises(hg.InvalidArgumentError, match="sort column"):
            operation(sorted_col)


class TestNullTests:
    def test_is_null(self):
        assert sql_of(col("a").is_null()) == '("a" IS NULL)'

    def test_is_not_null(self):
        assert sql_of(col("a").is_not_null()) == '("a" IS NOT NULL)'


class TestCast:
    def test_cast_to_integer(self):
        assert sql_of(col("b").cast(hg.IntegerType())) == '("b")::integer'

    def test_cast_byte_arithmetic(self):
        # ByteType stores as "char"; casting enables arithmetic.
        assert sql_of(col("b").cast(hg.IntegerType()) + 1) == '(("b")::integer + 1)'

    def test_cast_parametric(self):
        assert sql_of(col("x").cast(hg.DecimalType(10, 2))) == '("x")::numeric(10,2)'

    def test_cast_requires_datatype(self):
        with pytest.raises(hg.InvalidArgumentError, match="requires a DataType"):
            col("a").cast("integer")


class TestPredicates:
    def test_isin(self):
        assert sql_of(col("region").isin("east", "west")) == (
            "(\"region\" IN ('east', 'west'))"
        )

    def test_isin_numeric(self):
        assert sql_of(col("id").isin(1, 2, 3)) == '("id" IN (1, 2, 3))'

    def test_isin_splat_collection(self):
        ids = [10, 20]
        assert sql_of(col("id").isin(*ids)) == '("id" IN (10, 20))'

    def test_isin_single_list_unwrapped(self):
        assert sql_of(col("id").isin([1, 2, 3])) == '("id" IN (1, 2, 3))'

    def test_isin_single_tuple_unwrapped(self):
        assert sql_of(col("id").isin((1, 2, 3))) == '("id" IN (1, 2, 3))'

    def test_isin_empty_is_false(self):
        assert sql_of(col("id").isin()) == "FALSE"

    def test_like(self):
        assert sql_of(col("name").like("a%")) == "(\"name\" LIKE 'a%')"

    def test_ilike(self):
        assert sql_of(col("name").ilike("A%")) == "(\"name\" ILIKE 'A%')"

    def test_between_values(self):
        assert sql_of(col("amount").between(10, 100)) == (
            '("amount" BETWEEN 10 AND 100)'
        )

    def test_between_columns(self):
        assert sql_of(col("x").between(col("lo"), col("hi"))) == (
            '("x" BETWEEN "lo" AND "hi")'
        )


class TestImmutabilityAndRepr:
    def test_operations_return_new_columns(self):
        base = col("a")
        derived = base + 1
        assert derived is not base
        assert base._to_sql().as_string() == '"a"'

    def test_repr_shows_sql(self):
        assert repr(col("a") + 1) == 'Column<("a" + 1)>'

    def test_repr_shows_alias(self):
        assert repr(col("a").alias("b")) == 'Column<"a" AS "b">'

    def test_no_dict_thanks_to_slots(self):
        assert not hasattr(col("a"), "__dict__")


class TestEndToEnd:
    def test_doc_example_chain(self):
        # Mirrors the design doc's headline example expression.
        rate = 100 * 0.0006  # plain Python: 0.06
        tax = (col("amount") * rate).alias("tax")
        assert tax._to_projection_sql().as_string() == '("amount" * 0.06) AS "tax"'

    def test_filter_predicate(self):
        pred = (col("amount") > 100) & (col("region") == "east")
        assert pred._to_sql().as_string() == (
            '(("amount" > 100) AND ("region" = \'east\'))'
        )

    def test_lit_in_expression(self):
        assert sql_of(col("a") + lit(5)) == '("a" + 5)'
