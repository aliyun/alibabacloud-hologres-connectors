from __future__ import annotations

from .types import ExceptionCode


class HoloClientException(Exception):
    """Primary exception class for the Hologres client."""

    def __init__(
        self, code: ExceptionCode, message: str, cause: BaseException | None = None
    ):
        self.code = code
        if cause is not None:
            super().__init__(f"[{code.name}] {message}", cause)
            self.__cause__ = cause
        else:
            super().__init__(f"[{code.name}] {message}")

    @property
    def is_retryable(self) -> bool:
        return self.code.is_retryable

    @property
    def is_dirty_data(self) -> bool:
        return self.code.is_dirty_data

    @staticmethod
    def from_pg_error(exc: Exception) -> HoloClientException:
        """Convert a psycopg exception to a HoloClientException."""
        import psycopg.errors as pge

        msg = str(exc)

        if isinstance(exc, pge.OperationalError):
            if "password" in msg.lower() or "authentication" in msg.lower():
                return HoloClientException(ExceptionCode.AUTH_FAIL, msg, exc)
            return HoloClientException(ExceptionCode.CONNECTION_ERROR, msg, exc)

        if isinstance(exc, pge.InsufficientPrivilege):
            return HoloClientException(ExceptionCode.PERMISSION_DENY, msg, exc)

        if isinstance(exc, (pge.SyntaxError, pge.SyntaxErrorOrAccessRuleViolation)):
            return HoloClientException(ExceptionCode.SYNTAX_ERROR, msg, exc)

        if isinstance(exc, pge.UndefinedTable):
            return HoloClientException(ExceptionCode.TABLE_NOT_FOUND, msg, exc)

        if isinstance(exc, pge.UndefinedColumn):
            return HoloClientException(ExceptionCode.META_NOT_MATCH, msg, exc)

        if isinstance(
            exc,
            (
                pge.NotNullViolation,
                pge.UniqueViolation,
                pge.CheckViolation,
                pge.ForeignKeyViolation,
            ),
        ):
            return HoloClientException(ExceptionCode.CONSTRAINT_VIOLATION, msg, exc)

        if isinstance(exc, pge.InvalidTextRepresentation):
            return HoloClientException(ExceptionCode.DATA_TYPE_ERROR, msg, exc)

        if isinstance(
            exc,
            (
                pge.NumericValueOutOfRange,
                pge.StringDataRightTruncation,
                pge.DatetimeFieldOverflow,
                pge.DataError,
            ),
        ):
            return HoloClientException(ExceptionCode.DATA_VALUE_ERROR, msg, exc)

        if isinstance(exc, pge.InvalidPassword):
            return HoloClientException(ExceptionCode.AUTH_FAIL, msg, exc)

        if isinstance(exc, pge.ReadOnlySqlTransaction):
            return HoloClientException(ExceptionCode.READ_ONLY, msg, exc)

        if isinstance(exc, pge.TooManyConnections):
            return HoloClientException(ExceptionCode.TOO_MANY_CONNECTIONS, msg, exc)

        # Check message patterns for cases not covered by exception type
        msg_lower = msg.lower()
        if "connection" in msg_lower and (
            "closed" in msg_lower or "refused" in msg_lower
        ):
            return HoloClientException(ExceptionCode.CONNECTION_ERROR, msg, exc)
        if "busy" in msg_lower or "resource" in msg_lower:
            return HoloClientException(ExceptionCode.BUSY, msg, exc)
        if "timeout" in msg_lower:
            return HoloClientException(ExceptionCode.TIMEOUT, msg, exc)
        if "read only" in msg_lower or "readonly" in msg_lower:
            return HoloClientException(ExceptionCode.READ_ONLY, msg, exc)

        return HoloClientException(ExceptionCode.UNKNOWN_ERROR, msg, exc)


class HoloClientWithDetailsException(HoloClientException):
    """Exception with per-record error details.

    Each entry in ``details`` is a (Record, HoloClientException) pair
    representing a failed record and its cause.  Multiple failures are
    accumulated via ``merge()``.
    """

    def __init__(
        self, code: ExceptionCode, message: str, cause: BaseException | None = None
    ):
        super().__init__(code, message, cause)
        self.details: list[tuple] = []  # List[(Record, HoloClientException)]

    def add(self, record, exception: HoloClientException) -> None:
        """Attach a single failed record with its cause."""
        self.details.append((record, exception))

    def add_all(self, records, exception: HoloClientException) -> None:
        """Attach multiple records that all failed with the same cause."""
        for r in records:
            self.details.append((r, exception))

    def merge(self, other: HoloClientWithDetailsException) -> None:
        """Merge another details exception into this one."""
        self.details.extend(other.details)

    @property
    def size(self) -> int:
        return len(self.details)
