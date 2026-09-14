"""Tests for client-side sources (from_dict / from_records) and object_table.

Inline sources touch no database at all: the rows compile into a VALUES
relation, so every assertion here is on generated SQL. object_table uses a
FakeConnection to script the REFRESH plus the information_schema lookup.
"""

import datetime
import decimal

import pytest

import hologres.dataframe as hg
from hologres.dataframe import _plan
from hologres.dataframe.exceptions import (
    InvalidArgumentError,
    UnsupportedOperationError,
)
from tests._fake_conn import OID_BOOL, OID_TEXT, FakeColumnInfo, make_session


def sql_of(df) -> str:
    return df._to_sql().as_string()


class TestFromDict:
    def test_single_column(self):
        df = hg.from_dict({"id": [1, 2, 3]})
        assert sql_of(df) == (
            'SELECT * FROM (VALUES ((1)::bigint), (2), (3)) "t0" ("id")'
        )

    def test_multiple_columns(self):
        df = hg.from_dict({"id": [1, 2], "name": ["a", "b"]})
        assert sql_of(df) == (
            "SELECT * FROM (VALUES ((1)::bigint, ('a')::text), (2, 'b') ) "
            '"t0" ("id", "name")'.replace(") ) ", ")) ")
        )

    def test_only_the_first_row_is_cast(self):
        out = sql_of(hg.from_dict({"id": [1, 2, 3]}))
        assert out.count("::bigint") == 1

    def test_schema_inference(self):
        df = hg.from_dict(
            {
                "i": [1],
                "f": [1.5],
                "s": ["a"],
                "b": [True],
                "d": [datetime.date(2024, 1, 1)],
                "t": [datetime.datetime(2024, 1, 1, 12, 0)],
                "n": [decimal.Decimal("1.5")],
                "y": [b"x"],
            }
        )
        types = {f.name: type(f.datatype).__name__ for f in df.schema.fields}
        assert types == {
            "i": "LongType",
            "f": "DoubleType",
            "s": "StringType",
            "b": "BooleanType",
            "d": "DateType",
            "t": "TimestampType",
            "n": "DecimalType",
            "y": "BinaryType",
        }

    def test_int_and_float_widen_to_double(self):
        assert hg.from_dict({"x": [1, 2.5]}).schema["x"].datatype == hg.DoubleType()

    def test_all_ints_stay_long(self):
        assert hg.from_dict({"x": [1, 2]}).schema["x"].datatype == hg.LongType()

    def test_all_null_column_becomes_text(self):
        assert hg.from_dict({"x": [None, None]}).schema["x"].datatype == hg.StringType()

    def test_null_in_first_row_is_still_typed(self):
        out = sql_of(hg.from_dict({"x": [None, 2]}))
        assert "(NULL)::bigint" in out

    def test_literals_are_escaped(self):
        out = sql_of(hg.from_dict({"s": ["a'b", None], "b": [True, False]}))
        assert "'a''b'" in out
        assert "NULL" in out
        assert "true" in out and "false" in out

    def test_column_names_are_quoted(self):
        assert '"weird name"' in sql_of(hg.from_dict({"weird name": [1]}))

    def test_mixed_types_rejected(self):
        with pytest.raises(InvalidArgumentError, match="mixes value types"):
            hg.from_dict({"x": [1, "a"]})

    def test_unsupported_value_type_rejected(self):
        with pytest.raises(InvalidArgumentError, match="unsupported values"):
            hg.from_dict({"x": [object()]})

    def test_unequal_column_lengths_rejected(self):
        with pytest.raises(InvalidArgumentError, match="same length"):
            hg.from_dict({"a": [1, 2], "b": [1]})

    def test_empty_dict_rejected(self):
        with pytest.raises(InvalidArgumentError, match="non-empty dict"):
            hg.from_dict({})

    def test_non_dict_rejected(self):
        with pytest.raises(InvalidArgumentError, match="non-empty dict"):
            hg.from_dict([1, 2])

    def test_no_rows_rejected(self):
        with pytest.raises(InvalidArgumentError, match="no rows"):
            hg.from_dict({"a": []})

    def test_bad_column_name_rejected(self):
        with pytest.raises(InvalidArgumentError, match="non-empty strings"):
            hg.from_dict({"": [1]})

    def test_non_sequence_column_rejected(self):
        with pytest.raises(InvalidArgumentError, match="list or tuple"):
            hg.from_dict({"a": 1})


class TestSchemaOverride:
    def test_schema_replaces_names_and_types(self):
        schema = hg.StructType(
            [
                hg.StructField("a", hg.IntegerType()),
                hg.StructField("b", hg.StringType()),
            ]
        )
        df = hg.from_dict({"id": [1], "name": ["x"]}, schema=schema)
        assert df.columns == ["a", "b"]
        assert "(1)::integer" in sql_of(df)
        assert '"a", "b"' in sql_of(df)

    def test_field_count_must_match(self):
        schema = hg.StructType([hg.StructField("a", hg.IntegerType())])
        with pytest.raises(InvalidArgumentError, match="schema has 1 field"):
            hg.from_dict({"id": [1], "name": ["x"]}, schema=schema)

    def test_schema_must_be_a_struct_type(self):
        with pytest.raises(InvalidArgumentError, match="must be a StructType"):
            hg.from_dict({"id": [1]}, schema="id bigint")


class TestFromRecords:
    def test_dict_rows(self):
        df = hg.from_records([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        assert df.columns == ["id", "name"]
        assert "(2, 'b')" in sql_of(df)

    def test_dict_row_key_order_follows_first_row(self):
        df = hg.from_records([{"id": 1, "name": "a"}, {"name": "b", "id": 2}])
        assert df.columns == ["id", "name"]
        # The second row was reordered to match, not emitted as ('b', 2).
        assert "(2, 'b')" in sql_of(df)

    def test_tuple_rows(self):
        df = hg.from_records([(1, "a"), (2, "b")], columns=["id", "name"])
        assert df.columns == ["id", "name"]

    def test_list_rows(self):
        df = hg.from_records([[1, "a"]], columns=["id", "name"])
        assert df.columns == ["id", "name"]

    def test_tuple_rows_need_columns(self):
        with pytest.raises(InvalidArgumentError, match="columns= is required"):
            hg.from_records([(1, "a")])

    def test_dict_rows_reject_columns(self):
        with pytest.raises(InvalidArgumentError, match="tuple rows only"):
            hg.from_records([{"id": 1}], columns=["id"])

    def test_inconsistent_keys_rejected(self):
        with pytest.raises(InvalidArgumentError, match="same keys"):
            hg.from_records([{"id": 1}, {"other": 2}])

    def test_mixed_row_shapes_rejected(self):
        with pytest.raises(InvalidArgumentError, match="same shape"):
            hg.from_records([{"id": 1}, (2,)])

    def test_mixed_row_shapes_rejected_tuple_first(self):
        with pytest.raises(InvalidArgumentError, match="same shape"):
            hg.from_records([(1,), {"id": 2}], columns=["id"])

    def test_wrong_row_width_rejected(self):
        with pytest.raises(InvalidArgumentError, match="value"):
            hg.from_records([(1, "a"), (2,)], columns=["id", "name"])

    def test_empty_rows_rejected(self):
        with pytest.raises(InvalidArgumentError, match="non-empty list"):
            hg.from_records([])

    def test_bad_row_type_rejected(self):
        with pytest.raises(InvalidArgumentError, match="dicts or tuples"):
            hg.from_records([1, 2])


class TestInlineLimit:
    def test_within_limit(self):
        df = hg.from_dict({"x": list(range(5))}, inline_limit=5)
        assert df.columns == ["x"]

    def test_beyond_limit_without_a_session_is_refused(self):
        with pytest.raises(UnsupportedOperationError, match="needs session"):
            hg.from_dict({"x": [1, 2, 3]}, inline_limit=2)

    def test_beyond_limit_falls_back_to_materialization(self):
        from hologres.dataframe import _plan

        session, conn = make_session()
        df = hg.from_dict({"x": [1, 2, 3]}, session=session, inline_limit=2)
        # The rows no longer travel in the statement; they will be uploaded on
        # the first action instead.
        assert isinstance(df._plan, _plan.PendingSource)
        assert df.columns == ["x"]
        # Still lazy: nothing has been sent yet.
        assert conn.recorded == []

    def test_bad_limit_rejected(self):
        with pytest.raises(InvalidArgumentError, match="inline_limit must be an int"):
            hg.from_dict({"x": [1]}, inline_limit="10")


class TestComposition:
    def test_session_is_attached(self):
        session, _ = make_session()
        assert hg.from_dict({"x": [1]}, session=session)._session is session

    def test_without_session_actions_are_refused(self):
        df = hg.from_dict({"x": [1]})
        with pytest.raises(InvalidArgumentError, match="no session"):
            df.collect()

    def test_filter_fuses_into_one_statement(self):
        out = sql_of(hg.from_dict({"x": [1, 2]}).filter(hg.col("x") > 1))
        assert out.count("SELECT") == 1
        assert out.rstrip().endswith('WHERE ("x" > 1)')

    def test_select_and_sort(self):
        df = hg.from_dict({"id": [1], "name": ["a"]})
        out = sql_of(df.select("name").sort("name"))
        assert out.startswith('SELECT "name" FROM (VALUES')
        assert out.rstrip().endswith('ORDER BY "name" ASC')

    def test_group_by_aggregate(self):
        df = hg.from_dict({"k": ["a", "a"], "v": [1, 2]})
        out = sql_of(df.group_by("k").agg(hg.sum("v").alias("total")))
        assert 'GROUP BY "k"' in out
        assert 'sum("v") AS "total"' in out

    def test_join_against_a_table(self):
        session, _ = make_session(
            script=[
                (
                    [("tier", "text", "text"), ("amount", "bigint", "int8")],
                    [FakeColumnInfo(n, OID_TEXT) for n in ("c", "d", "u")],
                )
            ]
        )
        orders = session.table("public.orders")
        tiers = hg.from_dict({"tier": ["gold"], "discount": [0.2]}, session=session)
        out = sql_of(orders.join(tiers, on="tier"))
        assert "INNER JOIN (SELECT * FROM (VALUES" in out
        assert 'USING ("tier")' in out

    def test_unknown_column_rejected_by_pinned_schema(self):
        df = hg.from_dict({"x": [1]})
        with pytest.raises(InvalidArgumentError, match="not in schema"):
            df.select("nope")

    def test_plan_node_holds_the_data(self):
        df = hg.from_dict({"x": [1, 2]})
        assert isinstance(df._plan, _plan.InlineValues)
        assert df._plan.rows == [(1,), (2,)]


class TestObjectTable:
    def _session(self):
        rows = [
            ("object_uri", "text", "text"),
            ("etag", "text", "text"),
            ("file", "text", "text"),
            ("metadata", "json", "json"),
        ]
        info = [FakeColumnInfo(n, OID_TEXT) for n in ("c", "d", "u")]
        session, conn = make_session(
            script=[
                ([(True,)], [FakeColumnInfo("exists", OID_BOOL)]),
                ([], []),
                (rows, info),
            ]
        )
        session._server_version = (5, 1, 0)
        return session, conn

    def test_refresh_runs_before_the_schema_lookup(self):
        session, conn = self._session()
        df = session.object_table("public.pdfs")
        assert conn.recorded[1] == 'REFRESH OBJECT TABLE "public"."pdfs"'
        assert "information_schema.columns" in conn.recorded[2]
        assert df.columns == ["object_uri", "etag", "file", "metadata"]

    def test_refresh_can_be_skipped(self):
        rows = [("object_uri", "text", "text")]
        info = [FakeColumnInfo(n, OID_TEXT) for n in ("c", "d", "u")]
        session, conn = make_session(
            script=[
                ([(True,)], [FakeColumnInfo("exists", OID_BOOL)]),
                (rows, info),
            ]
        )
        session._server_version = (5, 1, 0)
        session.object_table("public.pdfs", refresh=False)
        assert not any("REFRESH" in q for q in conn.recorded)

    def test_returns_a_scan_of_the_table(self):
        session, _ = self._session()
        assert sql_of(session.object_table("pdfs")) == 'SELECT * FROM "public"."pdfs"'

    def test_invalid_name_rejected(self):
        session, _ = self._session()
        with pytest.raises(InvalidArgumentError):
            session.object_table("")
