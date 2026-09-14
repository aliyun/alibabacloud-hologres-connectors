"""Tests for schema-driven operators: drop / rename / fillna / dropna(None).

All tests use a pinned schema constructed from a FakeConnection so nothing
touches a database.
"""

import pytest

import hologres.dataframe as hg
from hologres.dataframe.dataframe import DataFrame
from tests._fake_conn import (
    OID_TEXT,
    FakeColumnInfo,
    make_session,
)

# ---------------------------------------------------------------------------
# Fixture table:   public.orders(id bigint, region text, amount double precision)
# ---------------------------------------------------------------------------

_ORDERS_ROWS = [
    ("id", "bigint", "int8"),
    ("region", "text", "text"),
    ("amount", "double precision", "float8"),
]
_ORDERS_COLS = [
    FakeColumnInfo("column_name", OID_TEXT),
    FakeColumnInfo("data_type", OID_TEXT),
    FakeColumnInfo("udt_name", OID_TEXT),
]


def orders_table_script():
    return (_ORDERS_ROWS, _ORDERS_COLS)


def orders_df():
    session, _ = make_session(script=[orders_table_script()])
    return session.table("public.orders")


def sql_of(df: DataFrame) -> str:
    return df._to_sql().as_string()


class TestDrop:
    def test_single(self):
        assert sql_of(orders_df().drop("region")) == (
            'SELECT "id", "amount" FROM "public"."orders"'
        )

    def test_multiple(self):
        assert sql_of(orders_df().drop("id", "region")) == (
            'SELECT "amount" FROM "public"."orders"'
        )

    def test_schema_narrows_to_remaining(self):
        d = orders_df().drop("region")
        assert d.columns == ["id", "amount"]
        assert d.schema["amount"].datatype == hg.DoubleType()

    def test_requires_at_least_one_name(self):
        with pytest.raises(hg.InvalidArgumentError, match="at least one column"):
            orders_df().drop()

    @pytest.mark.parametrize("bad", ["", None, 5])
    def test_rejects_non_string(self, bad):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty strings"):
            orders_df().drop(bad)

    def test_rejects_unknown_column(self):
        with pytest.raises(hg.InvalidArgumentError, match="not in schema"):
            orders_df().drop("no_such_col")

    def test_rejects_duplicate_arg(self):
        with pytest.raises(hg.InvalidArgumentError, match="duplicate"):
            orders_df().drop("id", "id")

    def test_rejects_dropping_all_columns(self):
        with pytest.raises(hg.InvalidArgumentError, match="every column"):
            orders_df().drop("id", "region", "amount")

    def test_requires_pinned_schema(self):
        # session.sql() has no pinned schema.
        session, _ = make_session()
        with pytest.raises(hg.InvalidArgumentError, match="pinned schema"):
            session.sql("SELECT * FROM t").drop("id")


class TestRename:
    def test_single_rename(self):
        assert sql_of(orders_df().rename({"region": "area"})) == (
            'SELECT "id", "region" AS "area", "amount" FROM "public"."orders"'
        )

    def test_multi_rename_preserves_order(self):
        out = sql_of(orders_df().rename({"id": "order_id", "amount": "amt"}))
        assert out == (
            'SELECT "id" AS "order_id", "region", "amount" AS "amt" '
            'FROM "public"."orders"'
        )

    def test_schema_reflects_new_names_and_types(self):
        d = orders_df().rename({"region": "area"})
        assert d.columns == ["id", "area", "amount"]
        # Types are preserved.
        assert d.schema["area"].datatype == hg.StringType()
        assert d.schema["amount"].datatype == hg.DoubleType()

    def test_rejects_empty_mapping(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty dict"):
            orders_df().rename({})

    @pytest.mark.parametrize("bad_key", ["", None, 5])
    def test_rejects_bad_key(self, bad_key):
        with pytest.raises(hg.InvalidArgumentError, match="keys must be non-empty"):
            orders_df().rename({bad_key: "new"})

    @pytest.mark.parametrize("bad_val", ["", None, 5])
    def test_rejects_bad_value(self, bad_val):
        with pytest.raises(hg.InvalidArgumentError, match="must be a non-empty string"):
            orders_df().rename({"region": bad_val})

    def test_rejects_unknown_source(self):
        with pytest.raises(hg.InvalidArgumentError, match="not in schema"):
            orders_df().rename({"no_such_col": "x"})

    def test_rejects_duplicate_target(self):
        # Renaming region -> id (an existing column) would collide.
        with pytest.raises(hg.InvalidArgumentError, match="duplicate column name"):
            orders_df().rename({"region": "id"})

    def test_requires_pinned_schema(self):
        session, _ = make_session()
        with pytest.raises(hg.InvalidArgumentError, match="pinned schema"):
            session.sql("SELECT * FROM t").rename({"a": "b"})


class TestFillna:
    def test_float_column_uses_case_with_nan_check(self):
        out = sql_of(orders_df().fillna({"amount": 0.0}))
        assert out == (
            'SELECT "id", "region", '
            'CASE WHEN "amount" IS NULL OR "amount" = \'NaN\'::float8 '
            'THEN 0.0 ELSE "amount" END AS "amount" FROM "public"."orders"'
        )

    def test_non_float_column_uses_coalesce(self):
        out = sql_of(orders_df().fillna({"region": "unknown"}))
        assert out == (
            'SELECT "id", COALESCE("region", \'unknown\') AS "region", "amount" '
            'FROM "public"."orders"'
        )

    def test_mixed_mapping(self):
        out = sql_of(orders_df().fillna({"region": "unknown", "amount": 0.0}))
        assert 'COALESCE("region"' in out
        assert 'CASE WHEN "amount"' in out
        # Order of columns is preserved.
        assert out.startswith('SELECT "id", COALESCE("region"')

    def test_schema_shape_preserved(self):
        d = orders_df().fillna({"amount": 0.0})
        assert d.columns == ["id", "region", "amount"]
        assert d.schema["amount"].datatype == hg.DoubleType()

    def test_rejects_empty_mapping(self):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty dict"):
            orders_df().fillna({})

    @pytest.mark.parametrize("bad_key", ["", None, 5])
    def test_rejects_bad_key(self, bad_key):
        with pytest.raises(hg.InvalidArgumentError, match="non-empty strings"):
            orders_df().fillna({bad_key: 0})

    def test_rejects_unknown_column(self):
        with pytest.raises(hg.InvalidArgumentError, match="not in schema"):
            orders_df().fillna({"no_such_col": 0})

    def test_rejects_column_value(self):
        with pytest.raises(hg.InvalidArgumentError, match="not a Column"):
            orders_df().fillna({"amount": hg.col("other")})

    def test_decimal_column_uses_numeric_nan_cast(self):
        from hologres.table_name import TableName

        from hologres.dataframe import _plan
        from hologres.dataframe.dataframe import DataFrame
        from hologres.dataframe.types import (
            DecimalType,
            StructField,
            StructType,
        )

        schema = StructType([StructField("price", DecimalType(10, 2))])
        df = DataFrame(_plan.TableScan(TableName.valueOf("t")), schema=schema)
        out = df.fillna({"price": 0})._to_sql().as_string()
        # numeric NaN comparison must use the same numeric type space, not
        # float8 (which would lose precision via implicit promotion).
        assert "'NaN'::numeric" in out
        assert "'NaN'::float8" not in out

    def test_fillna_marks_replaced_column_not_nullable(self):
        d = orders_df().fillna({"amount": 0.0})
        assert d.schema["amount"].nullable is False
        # Untouched columns keep their original nullability.
        assert d.schema["region"].nullable is True
        assert d.schema["id"].nullable is True

    def test_fillna_none_value_leaves_nullable(self):
        # Replacing with NULL cannot promise the column becomes NOT NULL.
        d = orders_df().fillna({"amount": None})
        assert d.schema["amount"].nullable is True

    def test_values_are_escaped(self):
        # SQL-injection-attempt string is safely quoted by psycopg.sql.Literal.
        out = sql_of(orders_df().fillna({"region": "east'; DROP TABLE users --"}))
        assert "'east''; DROP TABLE users --'" in out

    def test_null_value_replacement(self):
        # A NULL replacement is silly but should render syntactically valid SQL.
        out = sql_of(orders_df().fillna({"region": None}))
        assert "COALESCE" in out and "NULL" in out

    def test_requires_pinned_schema(self):
        session, _ = make_session()
        with pytest.raises(hg.InvalidArgumentError, match="pinned schema"):
            session.sql("SELECT * FROM t").fillna({"a": 0})


class TestDropnaNoSubset:
    """The no-subset form added in this commit."""

    def test_no_subset_covers_all_columns_type_aware(self):
        out = sql_of(orders_df().dropna())
        # id (bigint) and region (text) only need IS NOT NULL; amount adds NaN.
        assert '("id" IS NOT NULL)' in out
        assert '("region" IS NOT NULL)' in out
        assert ('(("amount" IS NOT NULL) AND ("amount" <> \'NaN\'::float8))') in out

    def test_subset_with_float_column_gets_full_check(self):
        out = sql_of(orders_df().dropna(["amount"]))
        assert "IS NOT NULL" in out
        assert "'NaN'::float8" in out

    def test_subset_with_non_float_column_is_null_only(self):
        # Type-aware upgrade of C4's dropna(subset): non-float column now
        # only gets the NULL check, avoiding a runtime type error.
        out = sql_of(orders_df().dropna(["region"]))
        assert "IS NOT NULL" in out
        assert "'NaN'::float8" not in out

    def test_subset_unknown_name_rejected(self):
        with pytest.raises(hg.InvalidArgumentError, match="not in schema"):
            orders_df().dropna(["nope"])

    def test_subset_column_expression_bypasses_schema_check(self):
        # A Column expression (not a bare ColumnRef) is not name-checked;
        # useful for computed predicates.
        out = sql_of(orders_df().dropna([hg.col("amount") + 1]))
        assert "IS NOT NULL" in out

    def test_subset_aliased_ref_uses_source_type(self):
        # dropna must resolve types by the *source* column name, not the alias
        # produced by .alias() on a bare ColumnRef. region is text, so no NaN
        # check should be emitted.
        out = sql_of(orders_df().dropna([hg.col("region").alias("x")]))
        assert "IS NOT NULL" in out
        assert "'NaN'::float8" not in out

    def test_decimal_column_dropna_uses_numeric_nan(self):
        # NUMERIC (DecimalType) NaN check must stay in numeric type space,
        # not implicitly promote to float8.
        from hologres.table_name import TableName

        from hologres.dataframe import _plan
        from hologres.dataframe.dataframe import DataFrame
        from hologres.dataframe.types import (
            DecimalType,
            StructField,
            StructType,
        )

        schema = StructType([StructField("price", DecimalType(10, 2))])
        df = DataFrame(_plan.TableScan(TableName.valueOf("t")), schema=schema)
        out = df.dropna(["price"])._to_sql().as_string()
        assert "'NaN'::numeric" in out
        assert "'NaN'::float8" not in out


class TestChainedSchemaOps:
    def test_drop_then_rename(self):
        d = orders_df().drop("id").rename({"region": "area"})
        assert d.columns == ["area", "amount"]
        assert sql_of(d) == (
            'SELECT "region" AS "area", "amount" FROM '
            '(SELECT "region", "amount" FROM "public"."orders") "t0"'
        )

    def test_fillna_then_drop(self):
        d = orders_df().fillna({"amount": 0.0}).drop("region")
        assert d.columns == ["id", "amount"]


class TestDropDuplicatesSubset:
    """Subset dedup goes through row_number(): Hologres rejects DISTINCT ON here."""

    def test_single_key(self):
        d = orders_df().drop_duplicates(["region"])
        assert sql_of(d) == (
            'SELECT "id", "region", "amount" FROM '
            '(SELECT "id", "region", "amount", '
            'row_number() OVER (PARTITION BY "region") AS "__hg_dedup_rank" '
            'FROM "public"."orders") "t0" WHERE ("__hg_dedup_rank" = 1)'
        )

    def test_columns_are_unchanged(self):
        assert orders_df().drop_duplicates(["region"]).columns == [
            "id",
            "region",
            "amount",
        ]

    def test_multiple_keys(self):
        out = sql_of(orders_df().drop_duplicates(["region", "amount"]))
        assert 'PARTITION BY "region", "amount"' in out

    def test_sort_direction_is_stripped(self):
        # A subset entry only names a column; asc/desc has no meaning for
        # partitioning and must not leak into the OVER clause.
        out = sql_of(orders_df().drop_duplicates([hg.col("region").desc()]))
        assert 'PARTITION BY "region")' in out
        assert "DESC" not in out

    def test_no_internal_column_leaks_into_the_result(self):
        d = orders_df().drop_duplicates(["region"])
        assert "__hg_dedup_rank" not in d.columns


class TestFacadeStillExports:
    def test_no_new_exports_needed(self):
        # drop / rename / fillna / dropna are DataFrame methods; the facade
        # doesn't need extra names for them.
        assert hasattr(hg.DataFrame, "drop")
        assert hasattr(hg.DataFrame, "rename")
        assert hasattr(hg.DataFrame, "fillna")
        assert hasattr(hg.DataFrame, "dropna")
