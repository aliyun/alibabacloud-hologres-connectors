"""Tests for df.join: USING/ON forms, relation-qualified columns, conflicts.

Everything runs off pinned schemas built from a FakeConnection, so no database
is involved. Fixture tables:

    public.orders(id bigint, user_id bigint, amount double precision)
    public.users(user_id bigint, name text)          -- shares user_id
    public.accounts(uid bigint, tier text)           -- shares nothing
    public.people(id bigint, name text)              -- shares id with orders
    public.staff(id bigint, name text)               -- shares id+name with people
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
    "orders": [
        ("id", "bigint", "int8"),
        ("user_id", "bigint", "int8"),
        ("amount", "double precision", "float8"),
    ],
    "users": [
        ("user_id", "bigint", "int8"),
        ("name", "text", "text"),
    ],
    "accounts": [
        ("uid", "bigint", "int8"),
        ("tier", "text", "text"),
    ],
    "people": [
        ("id", "bigint", "int8"),
        ("name", "text", "text"),
    ],
    "staff": [
        ("id", "bigint", "int8"),
        ("name", "text", "text"),
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


class TestColumnBinding:
    def test_renders_bare_outside_a_join(self):
        (orders,) = tables("orders")
        assert orders["amount"]._to_sql().as_string() == '"amount"'

    def test_carries_the_source_relation(self):
        (orders,) = tables("orders")
        assert orders["amount"]._expr.relation is orders._plan

    def test_unknown_column_rejected_against_pinned_schema(self):
        (orders,) = tables("orders")
        with pytest.raises(InvalidArgumentError, match="not in schema"):
            orders["nope"]

    def test_no_validation_without_a_pinned_schema(self):
        assert bare("orders")["anything"]._to_sql().as_string() == '"anything"'

    def test_col_stays_unbound(self):
        assert hg.col("amount")._expr.relation is None


class TestRewriteRelations:
    def setup_method(self):
        self.rel = object()
        self.mapping = {id(self.rel): "t0"}

    def rewrite(self, expression):
        return _expr.rewrite_relations(expression, self.mapping).to_sql().as_string()

    def ref(self, name="a"):
        return _expr.ColumnRef(name, relation=self.rel)

    def test_column_ref(self):
        assert self.rewrite(self.ref()) == '"t0"."a"'

    def test_unbound_ref_untouched(self):
        assert self.rewrite(_expr.ColumnRef("a")) == '"a"'

    def test_unknown_relation_untouched(self):
        assert self.rewrite(_expr.ColumnRef("a", relation=object())) == '"a"'

    def test_binary(self):
        node = _expr.BinaryExpr(self.ref(), "=", _expr.Literal(1))
        assert self.rewrite(node) == '("t0"."a" = 1)'

    def test_unary(self):
        node = _expr.UnaryExpr("IS NULL", self.ref(), postfix=True)
        assert self.rewrite(node) == '("t0"."a" IS NULL)'

    def test_predicate(self):
        from psycopg import sql

        node = _expr.PredicateExpr(
            sql.SQL("({} BETWEEN {} AND {})"),
            [self.ref(), _expr.Literal(1), _expr.Literal(9)],
        )
        assert self.rewrite(node) == '("t0"."a" BETWEEN 1 AND 9)'

    def test_cast(self):
        node = _expr.CastExpr(self.ref(), "integer")
        assert self.rewrite(node) == '("t0"."a")::integer'

    def test_sort(self):
        node = _expr.SortExpr(self.ref(), ascending=False, nulls="last")
        assert self.rewrite(node) == '"t0"."a" DESC NULLS LAST'

    def test_function_call(self):
        node = _expr.FunctionCall("sum", [self.ref()], required_capability="sum")
        assert self.rewrite(node) == 'sum("t0"."a")'
        rewritten = _expr.rewrite_relations(node, self.mapping)
        assert rewritten.required_capability == "sum"

    def test_literal_star_and_raw_are_returned_as_is(self):
        for node in (_expr.Literal(1), _expr.Star(), _expr.RawExpr("now()")):
            assert _expr.rewrite_relations(node, self.mapping) is node

    def test_ambiguous_relation_rejected(self):
        with pytest.raises(InvalidArgumentError, match="ambiguous"):
            _expr.rewrite_relations(self.ref(), {}, {id(self.rel)})

    def test_unknown_node_type(self):
        class Weird(_expr.Expression):
            def to_sql(self):
                return None

        with pytest.raises(TypeError, match="Unknown expression node"):
            _expr.rewrite_relations(Weird(), self.mapping)


class TestUsingForm:
    def test_single_column(self):
        orders, users = tables("orders", "users")
        assert sql_of(orders.join(users, on="user_id")) == (
            'SELECT * FROM "public"."orders" "t0" '
            'INNER JOIN "public"."users" "t1" USING ("user_id")'
        )

    def test_multiple_columns(self):
        people, staff = tables("people", "staff")
        assert 'USING ("id", "name")' in sql_of(people.join(staff, on=["id", "name"]))

    def test_tuple_accepted(self):
        people, staff = tables("people", "staff")
        assert 'USING ("id", "name")' in sql_of(people.join(staff, on=("id", "name")))

    def test_schema_puts_merged_columns_first(self):
        orders, users = tables("orders", "users")
        joined = orders.join(users, on="user_id")
        assert joined.columns == ["user_id", "id", "amount", "name"]
        assert joined.schema["user_id"].datatype == hg.LongType()
        assert joined.schema["name"].datatype == hg.StringType()

    def test_missing_on_left(self):
        accounts, users = tables("accounts", "users")
        with pytest.raises(InvalidArgumentError, match="must exist on both sides"):
            accounts.join(users, on="user_id")

    def test_missing_on_right(self):
        orders, accounts = tables("orders", "accounts")
        with pytest.raises(InvalidArgumentError, match="must exist on both sides"):
            orders.join(accounts, on="user_id")


class TestOnForm:
    def test_qualifies_both_sides(self):
        orders, accounts = tables("orders", "accounts")
        pred = orders["user_id"] == accounts["uid"]
        assert sql_of(orders.join(accounts, on=pred)) == (
            'SELECT * FROM "public"."orders" "t0" '
            'INNER JOIN "public"."accounts" "t1" '
            'ON ("t0"."user_id" = "t1"."uid")'
        )

    def test_schema_concatenates_both_sides(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        assert joined.columns == ["id", "user_id", "amount", "uid", "tier"]

    def test_compound_predicate(self):
        orders, accounts = tables("orders", "accounts")
        pred = (orders["user_id"] == accounts["uid"]) & (accounts["tier"] == "gold")
        out = sql_of(orders.join(accounts, on=pred))
        assert '("t0"."user_id" = "t1"."uid")' in out
        assert '("t1"."tier" = \'gold\')' in out

    @pytest.mark.parametrize(
        ("how", "keyword"),
        [
            ("inner", "INNER JOIN"),
            ("left", "LEFT JOIN"),
            ("right", "RIGHT JOIN"),
            ("full", "FULL JOIN"),
        ],
    )
    def test_join_keywords(self, how, keyword):
        orders, accounts = tables("orders", "accounts")
        pred = orders["user_id"] == accounts["uid"]
        assert keyword in sql_of(orders.join(accounts, on=pred, how=how))

    def test_cross_join(self):
        orders, accounts = tables("orders", "accounts")
        assert sql_of(orders.join(accounts, how="cross")) == (
            'SELECT * FROM "public"."orders" "t0" CROSS JOIN "public"."accounts" "t1"'
        )

    def test_raw_sql_side_becomes_a_subquery(self):
        (orders,) = tables("orders")
        raw = DataFrame(_plan.RawSql("SELECT 1 AS n"))
        out = sql_of(orders.join(raw, how="cross"))
        assert '(SELECT 1 AS n) "t1"' in out

    def test_filtered_side_becomes_a_subquery(self):
        orders, accounts = tables("orders", "accounts")
        left = orders.filter(orders["amount"] > 100)
        pred = orders["user_id"] == accounts["uid"]
        out = sql_of(left.join(accounts, on=pred))
        assert '(SELECT * FROM "public"."orders" WHERE ("amount" > 100)) "t0"' in out
        assert 'ON ("t0"."user_id" = "t1"."uid")' in out

    def test_chained_join_nests_the_left_side(self):
        orders, accounts, users = tables("orders", "accounts", "users")
        first = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        second = first.join(
            users, on=orders["user_id"] == users["user_id"], rsuffix="_u"
        )
        out = sql_of(second)
        assert '"public"."users" "t3"' in out
        # The left input became subquery t2, and a reference to the original
        # orders table still resolves -- to t2, the relation now in scope.
        assert 'ON ("t2"."user_id" = "t3"."user_id")' in out


class TestHowNormalization:
    @pytest.mark.parametrize(
        ("how", "keyword"),
        [
            ("INNER", "INNER JOIN"),
            ("Left", "LEFT JOIN"),
            ("left_outer", "LEFT JOIN"),
            ("leftouter", "LEFT JOIN"),
            ("right_outer", "RIGHT JOIN"),
            ("outer", "FULL JOIN"),
            ("full_outer", "FULL JOIN"),
            ("fullouter", "FULL JOIN"),
            ("full outer", "FULL JOIN"),
        ],
    )
    def test_aliases(self, how, keyword):
        orders, accounts = tables("orders", "accounts")
        pred = orders["user_id"] == accounts["uid"]
        assert keyword in sql_of(orders.join(accounts, on=pred, how=how))

    def test_unknown_how(self):
        orders, accounts = tables("orders", "accounts")
        with pytest.raises(InvalidArgumentError, match="unknown join type"):
            orders.join(accounts, on="uid", how="semi")

    def test_non_string_how(self):
        orders, accounts = tables("orders", "accounts")
        with pytest.raises(InvalidArgumentError, match="how must be a string"):
            orders.join(accounts, on="uid", how=1)


class TestConditionValidation:
    def test_other_must_be_a_dataframe(self):
        (orders,) = tables("orders")
        with pytest.raises(InvalidArgumentError, match="expects a DataFrame"):
            orders.join("users", on="user_id")

    def test_missing_condition(self):
        orders, accounts = tables("orders", "accounts")
        with pytest.raises(InvalidArgumentError, match="requires on="):
            orders.join(accounts)

    def test_cross_join_rejects_a_condition(self):
        orders, users = tables("orders", "users")
        with pytest.raises(InvalidArgumentError, match="no on= condition"):
            orders.join(users, on="user_id", how="cross")

    def test_empty_list(self):
        orders, users = tables("orders", "users")
        with pytest.raises(InvalidArgumentError, match="empty list"):
            orders.join(users, on=[])

    def test_non_string_in_list(self):
        orders, users = tables("orders", "users")
        with pytest.raises(InvalidArgumentError, match="column names"):
            orders.join(users, on=["user_id", 7])

    def test_bad_on_type(self):
        orders, users = tables("orders", "users")
        with pytest.raises(InvalidArgumentError, match="on must be"):
            orders.join(users, on=7)


class TestNodeValidation:
    def test_unknown_how(self):
        scan = bare("orders")._plan
        with pytest.raises(ValueError, match="unknown join type"):
            _plan.Join(scan, scan, "semi", using=["id"])

    def test_cross_with_condition(self):
        scan = bare("orders")._plan
        with pytest.raises(ValueError, match="takes no on/using"):
            _plan.Join(scan, scan, "cross", using=["id"])

    def test_both_on_and_using(self):
        scan = bare("orders")._plan
        with pytest.raises(ValueError, match="exactly one"):
            _plan.Join(scan, scan, "inner", on=hg.col("a"), using=["id"])

    def test_neither_on_nor_using(self):
        scan = bare("orders")._plan
        with pytest.raises(ValueError, match="exactly one"):
            _plan.Join(scan, scan, "inner")


class TestConflicts:
    def test_duplicate_names_rejected(self):
        orders, people = tables("orders", "people")
        with pytest.raises(InvalidArgumentError, match=r"repeat the column name"):
            orders.join(people, on=orders["id"] == people["id"])

    def test_suffixes_expand_to_an_explicit_projection(self):
        orders, people = tables("orders", "people")
        joined = orders.join(
            people,
            on=orders["id"] == people["id"],
            lsuffix="_o",
            rsuffix="_p",
        )
        assert sql_of(joined) == (
            'SELECT "t0"."id" AS "id_o", "t0"."user_id", "t0"."amount", '
            '"t1"."id" AS "id_p", "t1"."name" '
            'FROM "public"."orders" "t0" INNER JOIN "public"."people" "t1" '
            'ON ("t0"."id" = "t1"."id")'
        )
        assert joined.columns == ["id_o", "user_id", "amount", "id_p", "name"]
        assert joined.schema["id_o"].datatype == hg.LongType()

    def test_one_sided_suffix_keeps_the_other_name(self):
        orders, people = tables("orders", "people")
        joined = orders.join(people, on=orders["id"] == people["id"], rsuffix="_p")
        assert joined.columns == ["id", "user_id", "amount", "id_p", "name"]
        assert 'SELECT "t0"."id", ' in sql_of(joined)

    def test_using_merges_the_join_column_but_not_the_rest(self):
        people, staff = tables("people", "staff")
        joined = people.join(staff, on="id", lsuffix="_l", rsuffix="_r")
        assert sql_of(joined) == (
            'SELECT "id", "t0"."name" AS "name_l", "t1"."name" AS "name_r" '
            'FROM "public"."people" "t0" INNER JOIN "public"."staff" "t1" '
            'USING ("id")'
        )
        assert joined.columns == ["id", "name_l", "name_r"]

    def test_no_projection_when_names_are_unique(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        assert sql_of(joined).startswith("SELECT * FROM")

    @pytest.mark.parametrize("suffix", ["", 7])
    def test_suffix_must_be_a_non_empty_string(self, suffix):
        orders, people = tables("orders", "people")
        with pytest.raises(InvalidArgumentError, match="non-empty string"):
            orders.join(people, on="id", lsuffix=suffix)

    def test_suffix_needs_both_schemas(self):
        (orders,) = tables("orders")
        with pytest.raises(InvalidArgumentError, match="schema is unknown"):
            orders.join(bare("people"), how="cross", lsuffix="_o")

    def test_unknown_schema_skips_the_conflict_check(self):
        joined = bare("orders").join(bare("people"), how="cross")
        assert joined._schema is None
        assert sql_of(joined) == (
            'SELECT * FROM "public"."orders" "t0" CROSS JOIN "public"."people" "t1"'
        )


class TestSqlSideSchema:
    def sql_side(self, *, joined_schema=False):
        """A session.sql DataFrame whose describe reports (uid, tier)."""
        description = [FakeColumnInfo("uid", 20), FakeColumnInfo("tier", OID_TEXT)]
        if joined_schema:
            description = [
                FakeColumnInfo("id", 20),
                FakeColumnInfo("user_id", 20),
                FakeColumnInfo("amount", 701),
                *description,
            ]
        session, conn = make_session(
            script=[
                (_TABLES["orders"], _INFO_COLS),
                ([], description),
            ]
        )
        return session.table("public.orders"), session.sql("SELECT 1"), conn

    def test_describe_resolves_the_schema_on_join(self):
        orders, raw, conn = self.sql_side(joined_schema=True)
        joined = orders.join(raw, how="cross")
        assert not any("LIMIT 0" in q for q in conn.recorded)
        assert joined.columns == ["id", "user_id", "amount", "uid", "tier"]
        assert any("LIMIT 0" in q for q in conn.recorded)

    def test_cached_schema_is_reused(self):
        orders, raw, conn = self.sql_side()
        raw.schema  # noqa: B018 - warms the cache
        described = sum("LIMIT 0" in q for q in conn.recorded)
        joined = orders.join(raw, how="cross")
        assert joined.columns == ["id", "user_id", "amount", "uid", "tier"]
        assert sum("LIMIT 0" in q for q in conn.recorded) == described

    def test_construction_does_not_deploy_remote_functions(self):
        class Deployer:
            def __init__(self):
                self.definitions = []

            def deploy(self, definition):
                self.definitions.append(definition)
                return f"endpoint/{definition.fc_function_name}"

        session, conn = make_session()
        deployer = Deployer()
        session._fc_deployer = deployer

        @hg.udf(name="join_build_time_udf")
        def build_time_udf(value: int) -> str:
            return str(value)

        left = session.sql("SELECT 1 AS id").select(build_time_udf("id").alias("value"))
        right = session.sql("SELECT 2 AS id")

        left.join(right, how="cross")

        assert deployer.definitions == []
        assert conn.recorded == []


class TestSelfJoin:
    def test_same_dataframe_on_both_sides_is_ambiguous(self):
        (orders,) = tables("orders")
        joined = orders.join(
            orders,
            on=orders["user_id"] == orders["id"],
            lsuffix="_l",
            rsuffix="_r",
        )
        with pytest.raises(InvalidArgumentError, match="ambiguous"):
            sql_of(joined)

    def test_two_table_calls_disambiguate(self):
        left, right = tables("orders", "orders")
        joined = left.join(
            right,
            on=left["user_id"] == right["id"],
            lsuffix="_l",
            rsuffix="_r",
        )
        assert 'ON ("t0"."user_id" = "t1"."id")' in sql_of(joined)

    def test_using_form_needs_no_qualification(self):
        (people,) = tables("people")
        # Every column is merged by USING, so nothing needs a qualifier and the
        # same DataFrame on both sides is fine.
        assert 'USING ("id", "name")' in sql_of(people.join(people, on=["id", "name"]))


class TestFusionAndNesting:
    def test_filter_fuses_into_the_join(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        out = sql_of(joined.filter(orders["amount"] > 100))
        assert out.count("SELECT") == 1
        assert 'WHERE ("t0"."amount" > 100)' in out

    def test_unbound_col_stays_unqualified(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        assert 'WHERE ("amount" > 100)' in sql_of(joined.filter(hg.col("amount") > 100))

    def test_select_fuses_and_qualifies(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        out = sql_of(joined.select(orders["amount"], accounts["tier"]))
        assert out.count("SELECT") == 1
        assert out.startswith('SELECT "t0"."amount", "t1"."tier" FROM')

    def test_sort_fuses_and_qualifies(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        out = sql_of(joined.sort(orders["amount"], ascending=False))
        assert 'ORDER BY "t0"."amount" DESC' in out

    def test_group_by_fuses_and_qualifies(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        grouped = joined.group_by(accounts["tier"]).agg(
            hg.sum(orders["amount"]).alias("total")
        )
        out = sql_of(grouped)
        assert out.count("SELECT") == 1
        assert 'SELECT "t1"."tier", sum("t0"."amount") AS "total"' in out
        assert 'GROUP BY "t1"."tier"' in out

    def test_dedup_partition_key_qualifies(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        out = sql_of(joined.drop_duplicates(subset=[accounts["tier"]]))
        assert 'PARTITION BY "t1"."tier"' in out

    def test_qualification_is_rebound_to_nested_output(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        out = sql_of(joined.select(orders["amount"]).filter(orders["amount"] > 1))
        assert out.count("SELECT") == 2
        assert ') "t2" WHERE ("t2"."amount" > 1)' in out

    def test_suffix_projection_rebinds_left_relation(self):
        people, staff = tables("people", "staff")
        joined = people.join(staff, on=people["id"] == staff["id"], lsuffix="_l")
        out = sql_of(joined.filter(people["id"] > 1))
        assert ') "t2" WHERE ("t2"."id_l" > 1)' in out
        assert ') "t2" WHERE ("id" > 1)' not in out

    def test_left_join_suffix_rebinds_null_test_to_left_relation(self):
        people, staff = tables("people", "staff")
        joined = people.join(
            staff,
            on=people["id"] == staff["id"],
            how="left",
            lsuffix="_l",
        )
        out = sql_of(joined.filter(people["id"].is_not_null()))
        assert ') "t2" WHERE ("t2"."id_l" IS NOT NULL)' in out
        assert ') "t2" WHERE ("id" IS NOT NULL)' not in out

    def test_projected_away_relation_column_is_rejected(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, on=orders["user_id"] == accounts["uid"])
        filtered = joined.select(accounts["tier"]).filter(orders["amount"] > 1)
        with pytest.raises(InvalidArgumentError, match="no longer available"):
            sql_of(filtered)


class TestSessionOwnership:
    def test_different_sessions_rejected(self):
        (orders,) = tables("orders")
        (accounts,) = tables("accounts")
        with pytest.raises(InvalidArgumentError, match="different sessions"):
            orders.join(accounts, how="cross")

    def test_same_session_kept(self):
        orders, accounts = tables("orders", "accounts")
        joined = orders.join(accounts, how="cross")
        assert joined._session is orders._session

    def test_session_inherited_from_either_side(self):
        (accounts,) = tables("accounts")
        assert bare("orders").join(accounts, how="cross")._session is accounts._session
        assert accounts.join(bare("orders"), how="cross")._session is accounts._session

    def test_immutability(self):
        orders, accounts = tables("orders", "accounts")
        before = sql_of(orders)
        orders.join(accounts, how="cross")
        assert sql_of(orders) == before


class TestIterNodes:
    def test_walks_both_children_of_a_join(self):
        left, right = bare("orders")._plan, bare("people")._plan
        join = _plan.Join(left, right, "cross")
        assert list(_plan.iter_nodes(join)) == [join, left, right]

    def test_walks_a_single_input_chain(self):
        scan = bare("orders")._plan
        limit = _plan.Limit(scan, 5)
        assert list(_plan.iter_nodes(limit)) == [limit, scan]
