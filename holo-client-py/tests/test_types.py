"""Unit tests for types and enums."""

import pytest

from hologres import OnConflictAction, MutationType, ExceptionCode


class TestOnConflictAction:
    """Tests for OnConflictAction enum."""

    def test_insert_or_ignore(self):
        """Test INSERT_OR_IGNORE value."""
        assert OnConflictAction.INSERT_OR_IGNORE.value == "INSERT_OR_IGNORE"

    def test_insert_or_update(self):
        """Test INSERT_OR_UPDATE value."""
        assert OnConflictAction.INSERT_OR_UPDATE.value == "INSERT_OR_UPDATE"

    def test_insert_or_replace(self):
        """Test INSERT_OR_REPLACE value."""
        assert OnConflictAction.INSERT_OR_REPLACE.value == "INSERT_OR_REPLACE"


class TestMutationType:
    """Tests for MutationType enum."""

    def test_insert(self):
        """Test INSERT value."""
        assert MutationType.INSERT.value == "INSERT"

    def test_delete(self):
        """Test DELETE value."""
        assert MutationType.DELETE.value == "DELETE"


class TestExceptionCode:
    """Tests for ExceptionCode enum."""

    def test_is_dirty_data(self):
        """Test is_dirty_data property."""
        # Dirty data errors
        assert ExceptionCode.TABLE_NOT_FOUND.is_dirty_data is True
        assert ExceptionCode.CONSTRAINT_VIOLATION.is_dirty_data is True
        assert ExceptionCode.DATA_TYPE_ERROR.is_dirty_data is True
        assert ExceptionCode.DATA_VALUE_ERROR.is_dirty_data is True

        # Not dirty data errors
        assert ExceptionCode.CONNECTION_ERROR.is_dirty_data is False
        assert ExceptionCode.INTERNAL_ERROR.is_dirty_data is False
        assert ExceptionCode.UNKNOWN_ERROR.is_dirty_data is False

    def test_is_retryable(self):
        """Test is_retryable property."""
        # Retryable errors
        assert ExceptionCode.CONNECTION_ERROR.is_retryable is True
        assert ExceptionCode.READ_ONLY.is_retryable is True
        assert ExceptionCode.META_NOT_MATCH.is_retryable is True
        assert ExceptionCode.TIMEOUT.is_retryable is True
        assert ExceptionCode.BUSY.is_retryable is True
        assert ExceptionCode.TOO_MANY_CONNECTIONS.is_retryable is True

        # Not retryable errors
        assert ExceptionCode.INVALID_CONFIG.is_retryable is False
        assert ExceptionCode.INVALID_REQUEST.is_retryable is False
        assert ExceptionCode.AUTH_FAIL.is_retryable is False
        assert ExceptionCode.TABLE_NOT_FOUND.is_retryable is False
        assert ExceptionCode.INTERNAL_ERROR.is_retryable is False

    def test_exception_code_values(self):
        """Test specific exception code values."""
        assert ExceptionCode.INVALID_CONFIG == 1
        assert ExceptionCode.INVALID_REQUEST == 2
        assert ExceptionCode.CONNECTION_ERROR == 100
        assert ExceptionCode.AUTH_FAIL == 101
        assert ExceptionCode.ALREADY_CLOSE == 102
        assert ExceptionCode.TABLE_NOT_FOUND == 200
        assert ExceptionCode.INTERNAL_ERROR == 300
        assert ExceptionCode.UNKNOWN_ERROR == 500

    def test_exception_code_int_compatibility(self):
        """Test that ExceptionCode can be compared with ints."""
        assert ExceptionCode.INVALID_CONFIG == 1
        assert ExceptionCode.INVALID_CONFIG < 100
        assert ExceptionCode.CONNECTION_ERROR > 50
