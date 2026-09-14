"""Tests for Row."""

import copy
import pickle

import pytest

from hologres.dataframe.row import Row


@pytest.fixture
def row():
    return Row(("id", "amount"), (7, 12.5))


class TestConstruction:
    def test_fields_and_values(self, row):
        assert row.fields == ("id", "amount")
        assert row.values == (7, 12.5)

    def test_lists_normalized_to_tuples(self):
        row = Row(["a", "b"], [1, 2])
        assert isinstance(row.fields, tuple)
        assert isinstance(row.values, tuple)

    def test_empty_row(self):
        row = Row((), ())
        assert len(row) == 0
        assert row.as_dict() == {}
        assert repr(row) == "Row()"

    @pytest.mark.parametrize(
        "fields, values", [(("a",), ()), ((), (1,)), (("a", "b"), (1,))]
    )
    def test_length_mismatch_rejected(self, fields, values):
        with pytest.raises(ValueError, match="field name"):
            Row(fields, values)


class TestAccess:
    def test_by_index(self, row):
        assert row[0] == 7
        assert row[1] == 12.5
        assert row[-1] == 12.5

    def test_by_slice(self, row):
        assert row[0:1] == (7,)
        assert row[:] == (7, 12.5)

    def test_by_name(self, row):
        assert row["id"] == 7
        assert row["amount"] == 12.5

    def test_by_attribute(self, row):
        assert row.id == 7
        assert row.amount == 12.5

    def test_index_out_of_range(self, row):
        with pytest.raises(IndexError):
            _ = row[9]

    def test_unknown_name(self, row):
        with pytest.raises(KeyError, match="No column named 'nope'"):
            _ = row["nope"]

    def test_unknown_attribute(self, row):
        with pytest.raises(AttributeError, match="no attribute 'nope'"):
            _ = row.nope

    def test_methods_not_shadowed_by_getattr(self, row):
        # __getattr__ must only fire for names that are not real attributes.
        assert callable(row.as_dict)
        assert row.fields == ("id", "amount")

    def test_non_identifier_column_via_getitem(self):
        row = Row(("total amount",), (3,))
        # Subscript is the reliable way to read it; `row.total amount` is a
        # Python syntax error, not something the class can control.
        assert row["total amount"] == 3
        # getattr still resolves it, since __getattr__ just does a name lookup.
        assert getattr(row, "total amount") == 3
        with pytest.raises(AttributeError):
            getattr(row, "no such column")


class TestConversion:
    def test_as_dict(self, row):
        assert row.as_dict() == {"id": 7, "amount": 12.5}

    def test_as_dict_is_a_copy(self, row):
        d = row.as_dict()
        d["id"] = 999
        assert row["id"] == 7

    def test_as_dict_with_duplicate_columns(self):
        # Joins can produce duplicate names; the last one wins, like dict().
        row = Row(("id", "id"), (1, 2))
        assert row.as_dict() == {"id": 2}
        # Positional access still reaches both values.
        assert row[0] == 1
        assert row[1] == 2


class TestProtocols:
    def test_len(self, row):
        assert len(row) == 2

    def test_iter(self, row):
        assert list(row) == [7, 12.5]

    def test_unpacking(self, row):
        id_, amount = row
        assert (id_, amount) == (7, 12.5)

    def test_contains(self, row):
        assert 7 in row
        assert 999 not in row

    def test_repr(self, row):
        assert repr(row) == "Row(id=7, amount=12.5)"

    def test_repr_quotes_strings(self):
        assert repr(Row(("name",), ("alice",))) == "Row(name='alice')"


class TestEqualityAndHashing:
    def test_equal_when_fields_and_values_match(self):
        assert Row(("a",), (1,)) == Row(("a",), (1,))

    def test_not_equal_on_different_values(self):
        assert Row(("a",), (1,)) != Row(("a",), (2,))

    def test_not_equal_on_different_field_names(self):
        assert Row(("a",), (1,)) != Row(("b",), (1,))

    def test_not_equal_to_other_types(self, row):
        assert row != (7, 12.5)
        assert row != "Row(id=7, amount=12.5)"

    def test_hashable_and_deduplicated(self):
        assert len({Row(("a",), (1,)), Row(("a",), (1,))}) == 1

    def test_usable_as_dict_key(self):
        assert {Row(("a",), (1,)): "x"}[Row(("a",), (1,))] == "x"

    def test_unhashable_values_propagate(self):
        with pytest.raises(TypeError):
            hash(Row(("a",), ([1, 2],)))


class TestImmutability:
    def test_cannot_set_new_attribute(self, row):
        with pytest.raises(AttributeError, match="immutable"):
            row.extra = 1

    def test_cannot_overwrite_field(self, row):
        with pytest.raises(AttributeError, match="immutable"):
            row.id = 99

    def test_cannot_assign_by_index(self, row):
        with pytest.raises(TypeError):
            row[0] = 99

    def test_no_dict_thanks_to_slots(self, row):
        assert not hasattr(row, "__dict__")


class TestSerialization:
    def test_pickle_round_trip(self, row):
        restored = pickle.loads(pickle.dumps(row))
        assert restored == row
        assert restored.fields == row.fields

    def test_copy(self, row):
        assert copy.copy(row) == row

    def test_deepcopy_independent(self):
        original = Row(("tags",), ([1, 2],))
        clone = copy.deepcopy(original)
        assert clone == original
        original["tags"].append(3)
        assert clone["tags"] == [1, 2]
