"""Tests for aggregate helpers, group_by, and GroupedDataFrame."""

import pytest
from hologres.table_name import TableName

import hologres.dataframe as hg
from hologres.dataframe import _plan
from hologres.dataframe.dataframe import DataFrame
from hologres.dataframe.grouped import GroupedDataFrame
from hologres.dataframe.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def orders_df():
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("region", StringType()),
            StructField("amount", DoubleType()),
        ]
    )
    return DataFrame(_plan.TableScan(TableName.valueOf("public.orders")), schema=schema)


def sql_of(df):
    return df._to_sql().as_string()


# ---------------------------------------------------------------------------
# Aggregate helpers (hg.sum / hg.count / hg.function / hg.expr)
# ---------------------------------------------------------------------------


class TestAggregateHelpers:
    def test_sum_str_and_column(self):
        assert hg.sum("amount")._to_sql().as_string() == 'sum("amount")'
        assert hg.sum(hg.col("amount"))._to_sql().as_string() == 'sum("amount")'

    def test_avg_max_min(self):
        assert hg.avg("amount")._to_sql().as_string() == 'avg("amount")'
        assert hg.max("amount")._to_sql().as_string() == 'max("amount")'
        assert hg.min("amount")._to_sql().as_string() == 'min("amount")'

    def test_count_star(self):
        assert hg.count("*")._to_sql().as_string() == "count(*)"
        assert hg.count("*").output_name == "count"

    def test_count_col_star_also_renders_star(self):
        # hg.count(hg.col("*")) should be treated as COUNT(*) too, not
        # COUNT("*") which would look up a column literally named "*".
        assert hg.count(hg.col("*"))._to_sql().as_string() == "count(*)"
        assert hg.count(hg.col("*")).output_name == "count"

    def test_count_column(self):
        assert hg.count("id")._to_sql().as_string() == 'count("id")'
        assert hg.count(hg.col("id")).output_name == "count(id)"

    def test_output_names(self):
        assert hg.sum(hg.col("amount")).output_name == "sum(amount)"
        assert hg.avg(hg.col("x")).output_name == "avg(x)"

    def test_multi_arg_function_has_no_auto_name(self):
        expr = hg.function("string_agg", hg.col("name"), hg.lit(","))
        assert expr.output_name is None

    def test_function_single_column_has_auto_name(self):
        assert hg.function("stddev", hg.col("x")).output_name == "stddev(x)"

    def test_function_renders(self):
        expr = hg.function("stddev", hg.col("x"))
        assert expr._to_sql().as_string() == 'stddev("x")'

    def test_function_with_literal_arg(self):
        expr = hg.function("string_agg", hg.col("name"), hg.lit(","))
        assert expr._to_sql().as_string() == "string_agg(\"name\", ',')"

    def test_function_rejects_bad_name(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty"):
            hg.function("")
        with pytest.raises(hg.InvalidArgumentError, match="non-empty"):
            hg.function(None)

    @pytest.mark.parametrize(
        "bad_name",
        [
            "stddev; DROP TABLE users --",
            "foo bar",
            "with space",
            "1abc",
            "'ok'",
            "count()",
        ],
    )
    def test_function_rejects_injection_names(self, bad_name):
        with pytest.raises(hg.InvalidArgumentError, match="bare SQL identifier"):
            hg.function(bad_name, hg.col("x"))

    def test_function_accepts_schema_qualified(self):
        out = hg.function("my_schema.my_udf", hg.col("x"))._to_sql().as_string()
        assert out == 'my_schema.my_udf("x")'

    def test_function_rejects_bad_arg(self):
        with pytest.raises(hg.InvalidArgumentError, match="column name"):
            hg.function("stddev", 5)

    @pytest.mark.parametrize("fn", [hg.sum, hg.avg, hg.max, hg.min])
    def test_simple_aggs_reject_bad_arg(self, fn):
        with pytest.raises(hg.InvalidArgumentError, match="column name"):
            fn(5)

    @pytest.mark.parametrize("fn", [hg.sum, hg.avg, hg.max, hg.min, hg.count])
    def test_simple_aggs_reject_empty_string(self, fn):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty"):
            fn("")

    def test_expr_renders_verbatim(self):
        out = hg.expr("count(*) filter (where paid)")._to_sql().as_string()
        assert out == "count(*) filter (where paid)"

    def test_expr_has_no_auto_name(self):
        assert hg.expr("count(*) filter (where paid)").output_name is None

    def test_expr_rejects_empty(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty"):
            hg.expr("")
        with pytest.raises(hg.InvalidArgumentError, match="non-empty"):
            hg.expr(None)


# ---------------------------------------------------------------------------
# DataFrame.group_by and GroupedDataFrame.agg
# ---------------------------------------------------------------------------


class TestGroupBy:
    def test_returns_grouped_dataframe(self):
        assert isinstance(orders_df().group_by("region"), GroupedDataFrame)

    def test_rejects_unknown_key(self):
        with pytest.raises(hg.InvalidArgumentError, match="not in schema"):
            orders_df().group_by("nope")

    def test_derived_key_skips_schema_check(self):
        # A derived expression (col + 1) can't be name-checked; accept it.
        gdf = orders_df().group_by(hg.col("amount") + 1)
        assert isinstance(gdf, GroupedDataFrame)

    def test_aliased_key_skips_schema_check(self):
        # Aliased ColumnRef is a rename; we let it through and the server
        # will complain if the alias doesn't work in a GROUP BY position.
        gdf = orders_df().group_by(hg.col("amount").alias("amt"))
        assert isinstance(gdf, GroupedDataFrame)

    def test_repr(self):
        assert "GroupedDataFrame" in repr(orders_df().group_by("region"))
        assert "global" in repr(orders_df().group_by())


class TestAgg:
    def test_single_agg_with_alias(self):
        assert sql_of(
            orders_df().group_by("region").agg(hg.sum("amount").alias("total"))
        ) == (
            'SELECT "region", sum("amount") AS "total" '
            'FROM "public"."orders" GROUP BY "region"'
        )

    def test_single_agg_auto_named(self):
        # No .alias() -> auto-name from FunctionCall.output_name.
        assert sql_of(orders_df().group_by("region").agg(hg.sum("amount"))) == (
            'SELECT "region", sum("amount") AS "sum(amount)" '
            'FROM "public"."orders" GROUP BY "region"'
        )

    def test_multiple_aggs(self):
        out = sql_of(
            orders_df()
            .group_by("region")
            .agg(hg.sum("amount").alias("total"), hg.count("*").alias("n"))
        )
        assert (
            'SELECT "region", sum("amount") AS "total", count(*) AS "n" '
            'FROM "public"."orders" GROUP BY "region"'
        ) == out

    def test_multiple_group_keys(self):
        assert sql_of(orders_df().group_by("region", "id").agg(hg.sum("amount"))) == (
            'SELECT "region", "id", sum("amount") AS "sum(amount)" '
            'FROM "public"."orders" GROUP BY "region", "id"'
        )

    def test_global_aggregation_no_group_by(self):
        out = sql_of(orders_df().group_by().agg(hg.count("*").alias("n")))
        assert out == 'SELECT count(*) AS "n" FROM "public"."orders"'
        assert "GROUP BY" not in out

    def test_rejects_no_agg(self):
        with pytest.raises(hg.InvalidArgumentError, match="at least one"):
            orders_df().group_by("region").agg()

    def test_rejects_non_column_arg(self):
        with pytest.raises(hg.InvalidArgumentError, match="Column instances"):
            orders_df().group_by("region").agg("amount")

    def test_rejects_unnamed_expr(self):
        # hg.expr has no auto-name; must .alias(...).
        with pytest.raises(hg.InvalidArgumentError, match="must have a name"):
            orders_df().group_by("region").agg(hg.expr("count(*) filter (where paid)"))

    def test_rejects_unnamed_multi_arg_function(self):
        with pytest.raises(hg.InvalidArgumentError, match="must have a name"):
            orders_df().group_by("region").agg(
                hg.function("string_agg", hg.col("id"), hg.lit(","))
            )

    def test_rejects_sort_column(self):
        # Passing a sort-decorated column into agg() is a user mistake; make
        # sure the error message names agg(), not the internal .alias() step.
        with pytest.raises(hg.InvalidArgumentError, match="sort column"):
            orders_df().group_by("region").agg(hg.sum("amount").desc())

    def test_rejects_duplicate_auto_names(self):
        with pytest.raises(hg.InvalidArgumentError, match="duplicate"):
            orders_df().group_by("region").agg(hg.sum("amount"), hg.sum("amount"))

    def test_rejects_duplicate_explicit_alias(self):
        with pytest.raises(hg.InvalidArgumentError, match="duplicate"):
            orders_df().group_by("region").agg(
                hg.sum("amount").alias("x"), hg.count("*").alias("x")
            )

    def test_multi_arg_function_with_alias_ok(self):
        out = sql_of(
            orders_df()
            .group_by("region")
            .agg(
                hg.function("string_agg", hg.col("id"), hg.lit(",")).alias("ids"),
            )
        )
        assert 'string_agg("id", \',\') AS "ids"' in out


class TestShortcuts:
    def test_count_shortcut(self):
        assert sql_of(orders_df().group_by("region").count()) == (
            'SELECT "region", count(*) AS "count" '
            'FROM "public"."orders" GROUP BY "region"'
        )

    def test_sum_shortcut_str(self):
        assert sql_of(orders_df().group_by("region").sum("amount")) == (
            'SELECT "region", sum("amount") AS "sum(amount)" '
            'FROM "public"."orders" GROUP BY "region"'
        )

    def test_sum_shortcut_column(self):
        assert sql_of(orders_df().group_by("region").sum(hg.col("amount"))).endswith(
            'AS "sum(amount)" FROM "public"."orders" GROUP BY "region"'
        )

    def test_avg_max_min_shortcuts(self):
        assert 'avg("amount")' in sql_of(orders_df().group_by("region").avg("amount"))
        assert 'max("amount")' in sql_of(orders_df().group_by("region").max("amount"))
        assert 'min("amount")' in sql_of(orders_df().group_by("region").min("amount"))


# ---------------------------------------------------------------------------
# Fusion / nesting behavior in the compiler
# ---------------------------------------------------------------------------


class TestFusionAndNesting:
    def test_filter_before_agg_fuses(self):
        # WHERE before GROUP BY -> single SELECT.
        out = sql_of(
            orders_df()
            .filter(hg.col("amount") > 100)
            .group_by("region")
            .agg(hg.sum("amount").alias("total"))
        )
        assert (
            'SELECT "region", sum("amount") AS "total" '
            'FROM "public"."orders" WHERE ("amount" > 100) '
            'GROUP BY "region"'
        ) == out
        assert "(SELECT" not in out.split("FROM ", 1)[1]  # no subquery

    def test_agg_then_sort_and_limit_stay_in_same_select(self):
        out = sql_of(
            orders_df()
            .group_by("region")
            .agg(hg.sum("amount").alias("total"))
            .sort(hg.col("total").desc())
            .limit(10)
        )
        assert ('GROUP BY "region" ORDER BY "total" DESC LIMIT 10') in out
        # No subquery introduced.
        assert "(SELECT" not in out

    def test_filter_after_agg_nests(self):
        out = sql_of(
            orders_df()
            .group_by("region")
            .agg(hg.sum("amount").alias("total"))
            .filter(hg.col("total") > 1000)
        )
        # HAVING isn't implemented yet; conservative nest.
        assert (
            'SELECT * FROM (SELECT "region", sum("amount") AS "total" '
            'FROM "public"."orders" GROUP BY "region") "t0" '
            'WHERE ("total" > 1000)'
        ) == out

    def test_agg_over_agg_nests(self):
        outer = (
            orders_df()
            .group_by("region")
            .agg(hg.sum("amount").alias("total"))
            .group_by()  # global aggregation over the grouped result
            .agg(hg.count("*").alias("n_regions"))
        )
        assert (
            'SELECT count(*) AS "n_regions" FROM '
            '(SELECT "region", sum("amount") AS "total" '
            'FROM "public"."orders" GROUP BY "region") "t0"'
        ) == sql_of(outer)

    def test_distinct_after_agg_nests(self):
        # DISTINCT after aggregate is unusual; must nest.
        out = sql_of(
            orders_df()
            .group_by("region")
            .agg(hg.sum("amount").alias("total"))
            .distinct()
        )
        assert "SELECT DISTINCT * FROM (SELECT" in out


class TestSchemaPropagation:
    def test_columns_reflect_keys_and_aliased_aggs(self):
        df = orders_df().group_by("region").agg(hg.sum("amount").alias("total"))
        assert df.columns == ["region", "total"]

    def test_columns_use_auto_name(self):
        df = orders_df().group_by("region").agg(hg.sum("amount"))
        assert df.columns == ["region", "sum(amount)"]

    def test_columns_after_count_shortcut(self):
        df = orders_df().group_by("region").count()
        assert df.columns == ["region", "count"]

    def test_global_agg_columns(self):
        df = orders_df().group_by().agg(hg.count("*").alias("n"))
        assert df.columns == ["n"]

    def test_key_type_preserved(self):
        df = orders_df().group_by("region").agg(hg.sum("amount"))
        assert df.schema["region"].datatype == hg.StringType()

    def test_derived_key_without_alias_rejected_at_agg(self):
        # group_by accepts a derived key without alias, but agg's schema
        # derivation needs a name for it.
        with pytest.raises(hg.InvalidArgumentError, match="alias"):
            orders_df().group_by(hg.col("amount") + 1).agg(hg.count("*").alias("n"))

    def test_derived_key_with_alias_ok(self):
        df = (
            orders_df()
            .group_by((hg.col("amount") + 1).alias("amt_plus_one"))
            .agg(hg.count("*").alias("n"))
        )
        assert df.columns == ["amt_plus_one", "n"]

    def test_without_pinned_schema_still_compiles(self):
        # A session-sql-derived DataFrame has no pinned schema; agg should
        # still work but produce no derived schema (None).
        plan = _plan.RawSql("SELECT region, amount FROM orders")
        df = DataFrame(plan)
        out = df.group_by("region").agg(hg.sum("amount").alias("total"))
        assert "GROUP BY" in sql_of(out)
        # Without a pinned schema we can't propagate one.
        assert out._schema is None


# ---------------------------------------------------------------------------
# Facade exports
# ---------------------------------------------------------------------------


class TestFacade:
    def test_top_level_aggs_exported(self):
        for name in ("sum", "avg", "max", "min", "count", "function", "expr"):
            assert hasattr(hg, name), name
            assert name in hg.__all__, name

    def test_grouped_dataframe_exported(self):
        assert hg.GroupedDataFrame is GroupedDataFrame
        assert "GroupedDataFrame" in hg.__all__
