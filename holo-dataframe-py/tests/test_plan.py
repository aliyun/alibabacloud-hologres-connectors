"""Tests for logical-plan nodes."""

import pytest
from hologres.table_name import TableName

from hologres.dataframe import _plan as p
from hologres.dataframe.functions import col


def scan():
    return p.TableScan(TableName.valueOf("public.orders"))


class TestTableScan:
    def test_holds_table_name(self):
        node = scan()
        assert node.table_name.schema_name == "public"
        assert node.table_name.table_name == "orders"


class TestRawSql:
    def test_holds_text(self):
        assert p.RawSql("SELECT 1").sql_text == "SELECT 1"


class TestProject:
    def test_holds_child_and_columns(self):
        cols = [col("a"), col("b")]
        node = p.Project(scan(), cols)
        assert isinstance(node.child, p.TableScan)
        assert len(node.columns) == 2

    def test_columns_copied(self):
        cols = [col("a")]
        node = p.Project(scan(), cols)
        cols.append(col("b"))
        assert len(node.columns) == 1


class TestFilter:
    def test_holds_predicate(self):
        node = p.Filter(scan(), col("a") > 1)
        assert node.predicate is not None


class TestSort:
    def test_holds_orders(self):
        node = p.Sort(scan(), [col("a").desc()])
        assert len(node.orders) == 1

    def test_orders_copied(self):
        orders = [col("a")]
        node = p.Sort(scan(), orders)
        orders.append(col("b"))
        assert len(node.orders) == 1


class TestLimit:
    def test_holds_n(self):
        assert p.Limit(scan(), 10).n == 10

    def test_zero_allowed(self):
        assert p.Limit(scan(), 0).n == 0

    @pytest.mark.parametrize("bad", [-1, 1.5, "10", None, True])
    def test_rejects_bad_n(self, bad):
        with pytest.raises(ValueError, match="non-negative integer"):
            p.Limit(scan(), bad)


class TestOffset:
    def test_holds_n(self):
        assert p.Offset(scan(), 20).n == 20

    @pytest.mark.parametrize("bad", [-1, 2.5, "5", None, False])
    def test_rejects_bad_n(self, bad):
        with pytest.raises(ValueError, match="non-negative integer"):
            p.Offset(scan(), bad)


class TestDistinct:
    def test_default_on_is_none(self):
        assert p.Distinct(scan()).on is None

    def test_holds_on_columns(self):
        node = p.Distinct(scan(), [col("a"), col("b")])
        assert len(node.on) == 2

    def test_on_copied(self):
        on = [col("a")]
        node = p.Distinct(scan(), on)
        on.append(col("b"))
        assert len(node.on) == 1
