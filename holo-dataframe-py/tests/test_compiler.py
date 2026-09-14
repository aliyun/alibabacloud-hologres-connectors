"""Tests for the logical-plan -> SQL compiler."""

import pytest
from hologres.table_name import TableName

from hologres.dataframe import _plan as p
from hologres.dataframe._compiler import compile_plan
from hologres.dataframe.functions import col


def scan():
    return p.TableScan(TableName.valueOf("public.orders"))


def compiled(plan) -> str:
    return compile_plan(plan).as_string()


class TestSingleOperator:
    def test_table_scan(self):
        assert compiled(scan()) == 'SELECT * FROM "public"."orders"'

    def test_project(self):
        plan = p.Project(scan(), [col("a"), col("b")])
        assert compiled(plan) == 'SELECT "a", "b" FROM "public"."orders"'

    def test_project_with_alias(self):
        plan = p.Project(scan(), [(col("amount") * 0.06).alias("tax")])
        assert compiled(plan) == (
            'SELECT ("amount" * 0.06) AS "tax" FROM "public"."orders"'
        )

    def test_filter(self):
        plan = p.Filter(scan(), col("amount") > 100)
        assert compiled(plan) == (
            'SELECT * FROM "public"."orders" WHERE ("amount" > 100)'
        )

    def test_sort(self):
        plan = p.Sort(scan(), [col("amount").desc()])
        assert compiled(plan) == (
            'SELECT * FROM "public"."orders" ORDER BY "amount" DESC'
        )

    def test_sort_multiple_keys(self):
        plan = p.Sort(scan(), [col("a"), col("b").desc()])
        assert compiled(plan) == (
            'SELECT * FROM "public"."orders" ORDER BY "a" ASC, "b" DESC'
        )

    def test_limit(self):
        assert compiled(p.Limit(scan(), 10)) == (
            'SELECT * FROM "public"."orders" LIMIT 10'
        )

    def test_offset(self):
        assert compiled(p.Offset(scan(), 20)) == (
            'SELECT * FROM "public"."orders" OFFSET 20'
        )

    def test_distinct(self):
        assert compiled(p.Distinct(scan())) == (
            'SELECT DISTINCT * FROM "public"."orders"'
        )

    def test_distinct_on(self):
        plan = p.Distinct(p.Sort(scan(), [col("a")]), [col("a")])
        assert compiled(plan) == (
            'SELECT DISTINCT ON ("a") * FROM "public"."orders" ORDER BY "a" ASC'
        )


class TestFusion:
    def test_filter_chain_merges_with_and(self):
        f1 = p.Filter(scan(), col("amount") > 100)
        f2 = p.Filter(f1, col("region") == "east")
        assert compiled(f2) == (
            'SELECT * FROM "public"."orders" '
            'WHERE ("amount" > 100) AND ("region" = \'east\')'
        )

    def test_scan_filter_sort_limit_single_select(self):
        f = p.Filter(scan(), col("amount") > 100)
        s = p.Sort(f, [col("amount").desc()])
        plan = p.Limit(s, 10)
        result = compiled(plan)
        assert "(" not in result.split(" FROM ")[1].split(" WHERE ")[0]  # no subquery
        assert result == (
            'SELECT * FROM "public"."orders" WHERE ("amount" > 100) '
            'ORDER BY "amount" DESC LIMIT 10'
        )

    def test_offset_then_limit_single_select(self):
        # OFFSET first, then LIMIT: both fit in one SELECT.
        plan = p.Limit(p.Offset(scan(), 20), 10)
        assert compiled(plan) == ('SELECT * FROM "public"."orders" LIMIT 10 OFFSET 20')

    def test_project_then_filter_below_is_single_select(self):
        # filter under a projection (closer to source) fuses.
        f = p.Filter(scan(), col("amount") > 100)
        plan = p.Project(f, [col("order_id")])
        assert compiled(plan) == (
            'SELECT "order_id" FROM "public"."orders" WHERE ("amount" > 100)'
        )


class TestNesting:
    def test_filter_above_project_nests(self):
        pr = p.Project(scan(), [col("a"), col("b")])
        plan = p.Filter(pr, col("a") > 1)
        assert compiled(plan) == (
            'SELECT * FROM (SELECT "a", "b" FROM "public"."orders") "t0" '
            'WHERE ("a" > 1)'
        )

    def test_limit_then_sort_nests(self):
        lt = p.Limit(scan(), 5)
        plan = p.Sort(lt, [col("a")])
        assert compiled(plan) == (
            'SELECT * FROM (SELECT * FROM "public"."orders" LIMIT 5) "t0" '
            'ORDER BY "a" ASC'
        )

    def test_project_over_project_nests(self):
        p1 = p.Project(scan(), [col("a")])
        plan = p.Project(p1, [col("a")])
        assert compiled(plan) == (
            'SELECT "a" FROM (SELECT "a" FROM "public"."orders") "t0"'
        )

    def test_limit_then_offset_nests(self):
        # limit-then-offset differs from offset-then-limit, so it must nest.
        plan = p.Offset(p.Limit(scan(), 10), 20)
        assert compiled(plan) == (
            'SELECT * FROM (SELECT * FROM "public"."orders" LIMIT 10) "t0" OFFSET 20'
        )

    def test_second_limit_nests(self):
        plan = p.Limit(p.Limit(scan(), 10), 5)
        assert compiled(plan) == (
            'SELECT * FROM (SELECT * FROM "public"."orders" LIMIT 10) "t0" LIMIT 5'
        )

    def test_distinct_then_limit_then_distinct_nests(self):
        d1 = p.Distinct(scan())
        lt = p.Limit(d1, 10)
        plan = p.Distinct(lt)
        assert compiled(plan) == (
            "SELECT DISTINCT * FROM "
            '(SELECT DISTINCT * FROM "public"."orders" LIMIT 10) "t0"'
        )

    def test_sort_after_distinct_nests(self):
        plan = p.Sort(p.Distinct(scan()), [col("b")])
        assert compiled(plan) == (
            'SELECT * FROM (SELECT DISTINCT * FROM "public"."orders") "t0" '
            'ORDER BY "b" ASC'
        )

    def test_sort_after_distinct_on_nests(self):
        # A later sort must not clobber the ORDER BY that DISTINCT ON relies on.
        distinct_on = p.Distinct(p.Sort(scan(), [col("a")]), [col("a")])
        plan = p.Sort(distinct_on, [col("b")])
        assert compiled(plan) == (
            'SELECT * FROM (SELECT DISTINCT ON ("a") * FROM "public"."orders" '
            'ORDER BY "a" ASC) "t0" ORDER BY "b" ASC'
        )

    def test_plain_distinct_after_sort_nests(self):
        plan = p.Distinct(p.Sort(scan(), [col("a")]))
        assert compiled(plan) == (
            "SELECT DISTINCT * FROM "
            '(SELECT * FROM "public"."orders" ORDER BY "a" ASC) "t0"'
        )

    def test_plain_distinct_after_project_sort_nests(self):
        # Must not emit `DISTINCT "b" ... ORDER BY "a"` (a is dropped) -> nest.
        plan = p.Distinct(p.Sort(p.Project(scan(), [col("b")]), [col("a")]))
        assert compiled(plan) == (
            "SELECT DISTINCT * FROM "
            '(SELECT "b" FROM "public"."orders" ORDER BY "a" ASC) "t0"'
        )

    def test_distinct_on_after_limit_nests(self):
        plan = p.Distinct(p.Limit(scan(), 5), [col("a")])
        assert compiled(plan) == (
            'SELECT DISTINCT ON ("a") * FROM '
            '(SELECT * FROM "public"."orders" LIMIT 5) "t0"'
        )

    def test_aliases_increment_for_multiple_nests(self):
        # Two independent nests should get t0 (inner) then t1 (outer).
        p1 = p.Project(scan(), [col("a")])
        p2 = p.Project(p1, [col("a")])
        plan = p.Project(p2, [col("a")])
        result = compiled(plan)
        assert '"t0"' in result and '"t1"' in result


class TestRawSql:
    def test_bare_raw_sql_is_verbatim(self):
        assert compiled(p.RawSql("SELECT * FROM orders WHERE x > 1")) == (
            "SELECT * FROM orders WHERE x > 1"
        )

    def test_raw_sql_as_subquery_source(self):
        plan = p.Filter(p.RawSql("SELECT * FROM orders"), col("a") > 1)
        assert compiled(plan) == (
            'SELECT * FROM (SELECT * FROM orders) "t0" WHERE ("a" > 1)'
        )

    def test_project_over_raw_sql(self):
        plan = p.Project(p.RawSql("SELECT * FROM orders"), [col("a")])
        assert compiled(plan) == ('SELECT "a" FROM (SELECT * FROM orders) "t0"')


class TestErrors:
    def test_unknown_node_raises(self):
        class Bogus(p.LogicalPlan):
            __slots__ = ()

        with pytest.raises(TypeError, match="Unknown plan node"):
            compile_plan(Bogus())


class TestInjectionSafety:
    def test_table_name_is_quoted(self):
        plan = p.TableScan(TableName.valueOf('public."weird name"'))
        assert '"weird name"' in compiled(plan)

    def test_string_literal_is_escaped(self):
        plan = p.Filter(scan(), col("region") == "e'ast")
        assert "'e''ast'" in compiled(plan)


class TestStageScan:
    """Reading staged Arrow files back through external_files."""

    def schema(self, *fields):
        from hologres.dataframe.types import StructType

        return StructType(list(fields))

    def test_renders_the_column_definition_list(self):
        from hologres.dataframe.types import LongType, StringType, StructField

        s = self.schema(
            StructField("id", LongType()), StructField("name", StringType())
        )
        assert compiled(p.StageScan("st1", s)) == (
            "SELECT * FROM external_files(path='internal_stage://st1') "
            'AS ("id" bigint, "name" text)'
        )

    def test_jsonb_is_declared_as_text_and_cast_back(self):
        from hologres.dataframe.types import JsonbType, LongType, StructField

        s = self.schema(StructField("id", LongType()), StructField("doc", JsonbType()))
        out = compiled(p.StageScan("st2", s))
        # Hologres surfaces json out of Arrow as text, so it is declared text
        # and cast in the projection.
        assert '"doc" text)' in out
        assert 'SELECT "id", ("doc")::jsonb AS "doc" FROM' in out

    def test_becomes_an_aliased_subquery_as_a_join_operand(self):
        from hologres.dataframe.types import LongType, StructField

        s = self.schema(StructField("id", LongType()))
        joined = p.Join(scan(), p.StageScan("st3", s), "cross")
        out = compiled(joined)
        assert "(SELECT * FROM external_files(path='internal_stage://st3')" in out
        assert ') "t1"' in out


class TestPendingSource:
    def test_resolves_once_and_caches(self):
        from hologres.dataframe.types import LongType, StructField, StructType

        s = StructType([StructField("id", LongType())])
        calls = []

        def loader():
            calls.append(1)
            return p.StageScan("st4", s)

        node = p.PendingSource(loader, s)
        first = compiled(node)
        second = compiled(node)
        assert first == second
        assert len(calls) == 1
