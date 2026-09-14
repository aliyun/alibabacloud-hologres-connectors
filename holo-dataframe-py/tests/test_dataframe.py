"""Tests for DataFrame transformation operators."""

import pytest
from hologres.table_name import TableName

import hologres.dataframe as hg
from hologres.dataframe import _plan
from hologres.dataframe.dataframe import DataFrame, _to_column


def df():
    return DataFrame(_plan.TableScan(TableName.valueOf("public.orders")))


def sql_of(dataframe: DataFrame) -> str:
    return dataframe._to_sql().as_string()


class TestToColumn:
    def test_string_becomes_column(self):
        assert _to_column("amount")._to_sql().as_string() == '"amount"'

    def test_column_passes_through(self):
        c = hg.col("amount")
        assert _to_column(c) is c

    @pytest.mark.parametrize("bad", [123, 1.5, None, ["a"]])
    def test_rejects_other_types(self, bad):
        with pytest.raises(hg.InvalidArgumentError, match="column name"):
            _to_column(bad)


class TestSelect:
    def test_by_name(self):
        assert sql_of(df().select("a", "b")) == (
            'SELECT "a", "b" FROM "public"."orders"'
        )

    def test_with_expression(self):
        assert sql_of(df().select("order_id", hg.col("amount") * 0.06)) == (
            'SELECT "order_id", ("amount" * 0.06) FROM "public"."orders"'
        )

    def test_with_alias(self):
        assert sql_of(df().select((hg.col("amount") * 0.06).alias("tax"))) == (
            'SELECT ("amount" * 0.06) AS "tax" FROM "public"."orders"'
        )

    def test_requires_at_least_one(self):
        with pytest.raises(hg.InvalidArgumentError, match="at least one column"):
            df().select()

    def test_rejects_bad_arg(self):
        with pytest.raises(hg.InvalidArgumentError):
            df().select(123)


class TestWithColumn:
    def test_adds_derived_column(self):
        assert sql_of(df().with_column("tax", hg.col("amount") * 0.06)) == (
            'SELECT *, ("amount" * 0.06) AS "tax" FROM "public"."orders"'
        )

    def test_constant_via_lit(self):
        assert sql_of(df().with_column("one", hg.lit(1))) == (
            'SELECT *, 1 AS "one" FROM "public"."orders"'
        )

    def test_appends_not_replaces(self):
        # Reusing an existing name appends a duplicate column (documented).
        out = sql_of(df().with_column("amount", hg.col("amount") * 2))
        assert out.startswith("SELECT *,")
        assert 'AS "amount"' in out

    @pytest.mark.parametrize("bad_name", ["", None, 5])
    def test_rejects_bad_name(self, bad_name):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty string"):
            df().with_column(bad_name, hg.col("a"))

    def test_rejects_non_column_value(self):
        with pytest.raises(hg.InvalidArgumentError, match="must be a Column"):
            df().with_column("x", 5)


class TestWithColumns:
    def test_adds_multiple_in_order(self):
        result = df().with_columns(
            {"tax": hg.col("amount") * 0.06, "big": hg.col("amount") > 100}
        )
        assert sql_of(result) == (
            'SELECT *, ("amount" * 0.06) AS "tax", '
            '("amount" > 100) AS "big" FROM "public"."orders"'
        )

    def test_rejects_empty(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty dict"):
            df().with_columns({})

    def test_rejects_non_dict(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty dict"):
            df().with_columns([("a", hg.col("a"))])

    def test_rejects_bad_key(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty strings"):
            df().with_columns({"": hg.col("a")})

    def test_rejects_non_column_value(self):
        with pytest.raises(hg.InvalidArgumentError, match="must be a Column"):
            df().with_columns({"x": 5})


class TestFilter:
    def test_filter(self):
        assert sql_of(df().filter(hg.col("amount") > 100)) == (
            'SELECT * FROM "public"."orders" WHERE ("amount" > 100)'
        )

    def test_where_is_alias(self):
        assert sql_of(df().where(hg.col("a") == 1)) == (
            'SELECT * FROM "public"."orders" WHERE ("a" = 1)'
        )

    def test_compound_predicate(self):
        pred = (hg.col("amount") > 100) & (hg.col("region") == "east")
        assert sql_of(df().filter(pred)) == (
            'SELECT * FROM "public"."orders" '
            'WHERE (("amount" > 100) AND ("region" = \'east\'))'
        )

    def test_rejects_non_column(self):
        with pytest.raises(hg.InvalidArgumentError, match="must be a Column"):
            df().filter("amount > 100")


class TestDropna:
    def test_single_column(self):
        assert sql_of(df().dropna(["amount"])) == (
            'SELECT * FROM "public"."orders" '
            'WHERE (("amount" IS NOT NULL) AND ("amount" <> \'NaN\'::float8))'
        )

    def test_multiple_columns_anded(self):
        assert sql_of(df().dropna(["a", "b"])) == (
            'SELECT * FROM "public"."orders" WHERE '
            '((("a" IS NOT NULL) AND ("a" <> \'NaN\'::float8)) '
            'AND (("b" IS NOT NULL) AND ("b" <> \'NaN\'::float8)))'
        )

    def test_accepts_column_objects(self):
        assert sql_of(df().dropna([hg.col("amount")])) == (
            'SELECT * FROM "public"."orders" '
            'WHERE (("amount" IS NOT NULL) AND ("amount" <> \'NaN\'::float8))'
        )

    def test_rejects_empty_subset(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty subset"):
            df().dropna([])

    def test_none_subset_requires_pinned_schema(self):
        with pytest.raises(hg.InvalidArgumentError, match="pinned schema"):
            df().dropna()
        with pytest.raises(hg.InvalidArgumentError, match="pinned schema"):
            df().dropna(None)


class TestSort:
    def test_ascending_default(self):
        assert sql_of(df().sort("amount")) == (
            'SELECT * FROM "public"."orders" ORDER BY "amount" ASC'
        )

    def test_descending_flag(self):
        assert sql_of(df().sort("a", "b", ascending=False)) == (
            'SELECT * FROM "public"."orders" ORDER BY "a" DESC, "b" DESC'
        )

    def test_explicit_direction_wins_over_flag(self):
        # A column with its own .desc() keeps it despite ascending=True default.
        assert sql_of(df().sort(hg.col("a").desc(), "b")) == (
            'SELECT * FROM "public"."orders" ORDER BY "a" DESC, "b" ASC'
        )

    def test_order_by_is_alias(self):
        assert sql_of(df().order_by("amount", ascending=False)) == (
            'SELECT * FROM "public"."orders" ORDER BY "amount" DESC'
        )

    def test_requires_at_least_one(self):
        with pytest.raises(hg.InvalidArgumentError, match="at least one column"):
            df().sort()


class TestLimitOffset:
    def test_limit(self):
        assert sql_of(df().limit(10)) == 'SELECT * FROM "public"."orders" LIMIT 10'

    def test_offset(self):
        assert sql_of(df().offset(20)) == 'SELECT * FROM "public"."orders" OFFSET 20'

    def test_limit_rejects_negative(self):
        with pytest.raises(ValueError, match="non-negative integer"):
            df().limit(-1)

    def test_offset_rejects_non_int(self):
        with pytest.raises(ValueError, match="non-negative integer"):
            df().offset("5")


class TestDistinct:
    def test_distinct(self):
        assert sql_of(df().distinct()) == 'SELECT DISTINCT * FROM "public"."orders"'

    def test_drop_duplicates_no_subset_is_distinct(self):
        assert sql_of(df().drop_duplicates()) == (
            'SELECT DISTINCT * FROM "public"."orders"'
        )

    def test_drop_duplicates_with_subset_needs_a_pinned_schema(self):
        # Deduplicating on a subset projects the original columns back, so the
        # column list must be known. See test_schema_ops for the SQL shape.
        with pytest.raises(hg.InvalidArgumentError, match="needs a pinned schema"):
            df().drop_duplicates(["a", "b"])

    def test_drop_duplicates_rejects_empty_list(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty"):
            df().drop_duplicates([])


class TestChaining:
    def test_doc_headline_example(self):
        result = (
            df()
            .filter(hg.col("amount") > 100)
            .with_column("tax", hg.col("amount") * 0.06)
            .select("order_id", "region", "amount", "tax")
            .sort(hg.col("amount"), ascending=False)
        )
        assert sql_of(result) == (
            'SELECT "order_id", "region", "amount", "tax" FROM '
            '(SELECT *, ("amount" * 0.06) AS "tax" FROM "public"."orders" '
            'WHERE ("amount" > 100)) "t0" ORDER BY "amount" DESC'
        )

    def test_filter_chain_fuses(self):
        result = df().filter(hg.col("a") > 1).filter(hg.col("b") < 2)
        assert sql_of(result) == (
            'SELECT * FROM "public"."orders" WHERE ("a" > 1) AND ("b" < 2)'
        )


class TestImmutabilityAndProtocol:
    def test_transform_returns_new_dataframe(self):
        base = df()
        derived = base.limit(5)
        assert derived is not base
        assert sql_of(base) == 'SELECT * FROM "public"."orders"'

    def test_getitem_returns_column(self):
        assert isinstance(df()["amount"], hg.Column)
        assert df()["amount"]._to_sql().as_string() == '"amount"'

    def test_repr_shows_sql(self):
        assert repr(df().limit(1)) == (
            'DataFrame<SELECT * FROM "public"."orders" LIMIT 1>'
        )

    def test_no_dict_thanks_to_slots(self):
        assert not hasattr(df(), "__dict__")

    def test_session_propagates_through_transforms(self):
        sentinel = object()
        base = DataFrame(_plan.TableScan(TableName.valueOf("t")), session=sentinel)
        assert base.limit(1).filter(hg.col("a") > 1)._session is sentinel


class TestFacade:
    def test_dataframe_exported(self):
        assert hg.DataFrame is DataFrame
        assert "DataFrame" in hg.__all__
