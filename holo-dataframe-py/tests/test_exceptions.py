"""Tests for the exception hierarchy."""

import pytest

from hologres.dataframe.exceptions import (
    CompilationError,
    ExecutionError,
    HoloDataFrameError,
    InvalidArgumentError,
    UnsupportedOperationError,
)

ALL_ERRORS = [
    InvalidArgumentError,
    UnsupportedOperationError,
    CompilationError,
    ExecutionError,
]


class TestHierarchy:
    @pytest.mark.parametrize("cls", ALL_ERRORS)
    def test_all_derive_from_base(self, cls):
        assert issubclass(cls, HoloDataFrameError)

    def test_base_is_exception(self):
        assert issubclass(HoloDataFrameError, Exception)

    def test_invalid_argument_is_also_value_error(self):
        # Lets existing `except ValueError` handlers keep working.
        assert issubclass(InvalidArgumentError, ValueError)
        assert isinstance(InvalidArgumentError("bad"), ValueError)

    def test_others_are_not_value_error(self):
        for cls in (UnsupportedOperationError, CompilationError, ExecutionError):
            assert not issubclass(cls, ValueError)

    @pytest.mark.parametrize("cls", ALL_ERRORS)
    def test_catchable_via_base(self, cls):
        with pytest.raises(HoloDataFrameError):
            raise cls("boom")

    @pytest.mark.parametrize("cls", ALL_ERRORS)
    def test_message_preserved(self, cls):
        assert str(cls("something went wrong")) == "something went wrong"


class TestExecutionError:
    def test_cause_defaults_to_none(self):
        err = ExecutionError("failed")
        assert err.cause is None
        assert err.__cause__ is None

    def test_cause_is_stored_and_chained(self):
        original = KeyError("missing")
        err = ExecutionError("failed", cause=original)
        assert err.cause is original
        assert err.__cause__ is original

    def test_from_holo_client_exception_wraps(self):
        original = RuntimeError("copy failed")
        err = ExecutionError.from_holo_client_exception(original)
        assert isinstance(err, ExecutionError)
        assert err.cause is original
        assert str(err) == "copy failed"

    def test_from_holo_client_exception_keeps_subclass(self):
        class MyExecutionError(ExecutionError):
            pass

        err = MyExecutionError.from_holo_client_exception(ValueError("x"))
        assert type(err) is MyExecutionError
