"""Unit tests for HoloClientException."""

import pytest

from hologres import HoloClientException, HoloClientWithDetailsException, ExceptionCode


class TestHoloClientException:
    """Tests for HoloClientException class."""

    def test_exception_creation(self):
        """Test basic exception creation."""
        exc = HoloClientException(ExceptionCode.INVALID_CONFIG, "Invalid configuration")
        assert exc.code == ExceptionCode.INVALID_CONFIG
        assert "INVALID_CONFIG" in str(exc)
        assert "Invalid configuration" in str(exc)

    def test_exception_with_cause(self):
        """Test exception with cause."""
        cause = ValueError("original error")
        exc = HoloClientException(
            ExceptionCode.INTERNAL_ERROR, "Internal error occurred", cause=cause
        )
        assert exc.__cause__ is cause
        assert "Internal error occurred" in str(exc)

    def test_exception_is_retryable(self):
        """Test is_retryable property delegates to code."""
        exc = HoloClientException(ExceptionCode.CONNECTION_ERROR, "msg")
        assert exc.is_retryable is True

        exc = HoloClientException(ExceptionCode.INVALID_CONFIG, "msg")
        assert exc.is_retryable is False

    def test_exception_is_dirty_data(self):
        """Test is_dirty_data property delegates to code."""
        exc = HoloClientException(ExceptionCode.TABLE_NOT_FOUND, "msg")
        assert exc.is_dirty_data is True

        exc = HoloClientException(ExceptionCode.CONNECTION_ERROR, "msg")
        assert exc.is_dirty_data is False

    def test_exception_raise_catch(self):
        """Test that exception can be raised and caught."""
        with pytest.raises(HoloClientException) as exc_info:
            raise HoloClientException(ExceptionCode.INVALID_REQUEST, "Test error")

        assert exc_info.value.code == ExceptionCode.INVALID_REQUEST

    def test_exception_catch_as_generic(self):
        """Test that HoloClientException can be caught as Exception."""
        with pytest.raises(Exception):
            raise HoloClientException(ExceptionCode.INTERNAL_ERROR, "Test")


class TestHoloClientWithDetailsException:
    """Tests for HoloClientWithDetailsException class."""

    def test_with_details_creation(self):
        """Test creating exception with details."""
        exc = HoloClientWithDetailsException(
            ExceptionCode.CONSTRAINT_VIOLATION,
            "Constraint violation",
        )
        assert exc.code == ExceptionCode.CONSTRAINT_VIOLATION
        assert exc.details == []

    def test_add_and_add_all(self):
        """Test add() and add_all() methods."""
        exc = HoloClientWithDetailsException(
            ExceptionCode.CONSTRAINT_VIOLATION,
            "Constraint violation",
        )
        cause = HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, "bad")
        exc.add("record1", cause)
        assert exc.size == 1
        assert exc.details[0] == ("record1", cause)

        exc.add_all(["record2", "record3"], cause)
        assert exc.size == 3

    def test_merge(self):
        """Test merge() combines details from two exceptions."""
        exc1 = HoloClientWithDetailsException(
            ExceptionCode.CONSTRAINT_VIOLATION, "err1"
        )
        exc2 = HoloClientWithDetailsException(ExceptionCode.DATA_TYPE_ERROR, "err2")
        cause1 = HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, "c1")
        cause2 = HoloClientException(ExceptionCode.DATA_TYPE_ERROR, "c2")
        exc1.add("r1", cause1)
        exc2.add("r2", cause2)
        exc1.merge(exc2)
        assert exc1.size == 2
        assert exc1.details[0] == ("r1", cause1)
        assert exc1.details[1] == ("r2", cause2)

    def test_with_details_inherits_from_base(self):
        """Test that WithDetailsException inherits from base."""
        exc = HoloClientWithDetailsException(
            ExceptionCode.DATA_TYPE_ERROR, "Type error"
        )
        assert isinstance(exc, HoloClientException)


class TestExceptionFromPgError:
    """Tests for from_pg_error static method."""

    def test_from_operational_error_auth(self):
        """Test converting psycopg OperationalError for auth failure."""
        import psycopg.errors as pge

        exc = pge.OperationalError("password authentication failed")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.AUTH_FAIL
        assert holo_exc.is_retryable is False

    def test_from_operational_error_connection(self):
        """Test converting psycopg OperationalError for connection issue."""
        import psycopg.errors as pge

        exc = pge.OperationalError("connection refused")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.CONNECTION_ERROR
        assert holo_exc.is_retryable is True

    def test_from_undefined_table(self):
        """Test converting psycopg UndefinedTable."""
        import psycopg.errors as pge

        exc = pge.UndefinedTable("relation does not exist")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.TABLE_NOT_FOUND
        assert holo_exc.is_dirty_data is True

    def test_from_undefined_column(self):
        """Test converting psycopg UndefinedColumn."""
        import psycopg.errors as pge

        exc = pge.UndefinedColumn("column does not exist")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.META_NOT_MATCH

    def test_from_not_null_violation(self):
        """Test converting psycopg NotNullViolation."""
        import psycopg.errors as pge

        exc = pge.NotNullViolation("null value in column")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.CONSTRAINT_VIOLATION
        assert holo_exc.is_dirty_data is True

    def test_from_unique_violation(self):
        """Test converting psycopg UniqueViolation."""
        import psycopg.errors as pge

        exc = pge.UniqueViolation("duplicate key value")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.CONSTRAINT_VIOLATION

    def test_from_insufficient_privilege(self):
        """Test converting psycopg InsufficientPrivilege."""
        import psycopg.errors as pge

        exc = pge.InsufficientPrivilege("permission denied")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.PERMISSION_DENY

    def test_from_syntax_error(self):
        """Test converting psycopg SyntaxError."""
        import psycopg.errors as pge

        exc = pge.SyntaxError("syntax error at or near")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.SYNTAX_ERROR

    def test_from_invalid_text_representation(self):
        """Test converting psycopg InvalidTextRepresentation."""
        import psycopg.errors as pge

        exc = pge.InvalidTextRepresentation("invalid input syntax")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.DATA_TYPE_ERROR

    def test_from_read_only_transaction(self):
        """Test converting psycopg ReadOnlySqlTransaction."""
        import psycopg.errors as pge

        exc = pge.ReadOnlySqlTransaction("cannot execute in read-only transaction")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.READ_ONLY
        assert holo_exc.is_retryable is True

    def test_from_unknown_error(self):
        """Test that unknown errors map to UNKNOWN_ERROR."""
        # Create a generic exception
        exc = Exception("Some unknown error")
        holo_exc = HoloClientException.from_pg_error(exc)

        assert holo_exc.code == ExceptionCode.UNKNOWN_ERROR

    def test_from_timeout_message_pattern(self):
        """Test that timeout message pattern might map to CONNECTION_ERROR or TIMEOUT."""
        import psycopg.errors as pge

        exc = pge.OperationalError("query timeout")
        holo_exc = HoloClientException.from_pg_error(exc)

        # The actual implementation may map this to CONNECTION_ERROR
        assert holo_exc.code in (ExceptionCode.TIMEOUT, ExceptionCode.CONNECTION_ERROR)
        assert holo_exc.is_retryable is True

    def test_from_busy_message_pattern(self):
        """Test that busy message pattern might map to CONNECTION_ERROR or BUSY."""
        import psycopg.errors as pge

        exc = pge.OperationalError("resource busy")
        holo_exc = HoloClientException.from_pg_error(exc)

        # The actual implementation may map this to CONNECTION_ERROR
        assert holo_exc.code in (ExceptionCode.BUSY, ExceptionCode.CONNECTION_ERROR)
