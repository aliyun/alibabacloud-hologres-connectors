from enum import Enum, IntEnum


class WriteFailStrategy(Enum):
    """Strategy when a batch write fails with dirty-data errors."""

    TRY_ONE_BY_ONE = "TRY_ONE_BY_ONE"
    NONE = "NONE"


class OnConflictAction(Enum):
    """Conflict resolution strategy for tables with primary keys."""

    INSERT_OR_IGNORE = "INSERT_OR_IGNORE"
    INSERT_OR_UPDATE = "INSERT_OR_UPDATE"
    INSERT_OR_REPLACE = "INSERT_OR_REPLACE"


class MutationType(Enum):
    """Mutation type for a write operation."""

    INSERT = "INSERT"
    DELETE = "DELETE"


class ExceptionCode(IntEnum):
    """Error classification codes for HoloClientException."""

    INVALID_CONFIG = 1
    INVALID_REQUEST = 2
    CONNECTION_ERROR = 100
    AUTH_FAIL = 101
    ALREADY_CLOSE = 102
    READ_ONLY = 103
    PERMISSION_DENY = 104
    SYNTAX_ERROR = 105
    TOO_MANY_CONNECTIONS = 106
    TOO_MANY_WAL_SENDERS = 107
    TABLE_NOT_FOUND = 200
    META_NOT_MATCH = 201
    CONSTRAINT_VIOLATION = 202
    DATA_TYPE_ERROR = 203
    DATA_VALUE_ERROR = 204
    TIMEOUT = 250
    BUSY = 251
    INTERNAL_ERROR = 300
    INTERRUPTED = 301
    NOT_SUPPORTED = 302
    UNKNOWN_ERROR = 500

    @property
    def is_dirty_data(self) -> bool:
        """Whether this error is caused by bad input data."""
        return self in (
            ExceptionCode.TABLE_NOT_FOUND,
            ExceptionCode.CONSTRAINT_VIOLATION,
            ExceptionCode.DATA_TYPE_ERROR,
            ExceptionCode.DATA_VALUE_ERROR,
        )

    @property
    def is_retryable(self) -> bool:
        """Whether the operation can be retried for this error."""
        return self in (
            ExceptionCode.CONNECTION_ERROR,
            ExceptionCode.READ_ONLY,
            ExceptionCode.META_NOT_MATCH,
            ExceptionCode.TIMEOUT,
            ExceptionCode.BUSY,
            ExceptionCode.TOO_MANY_CONNECTIONS,
        )
