"""Exception hierarchy for the Hologres DataFrame API.

All exceptions raised by ``hologres.dataframe`` derive from
:class:`HoloDataFrameError`, so a single ``except`` clause can catch anything
this library raises::

    import hologres.dataframe as hg

    try:
        df.write("public.summary", overwrite=True, on_conflict=("update", ["id"]))
    except hg.HoloDataFrameError as exc:
        print(f"DataFrame call failed: {exc}")

Choosing the right one when raising:

* :class:`InvalidArgumentError` -- the caller passed something wrong.
* :class:`UnsupportedOperationError` -- the call is well formed, but Hologres
  cannot do it.
* :class:`CompilationError` -- the logical plan could not be turned into SQL.
* :class:`ExecutionError` -- Hologres rejected or failed the statement.
"""

from __future__ import annotations


class HoloDataFrameError(Exception):
    """Base class for every error raised by ``hologres.dataframe``.

    Catch this to handle any DataFrame failure without caring about the
    specific cause.

    Example:
        >>> try:
        ...     raise InvalidArgumentError("how must be one of inner/left/right/full")
        ... except HoloDataFrameError as exc:
        ...     print(type(exc).__name__)
        InvalidArgumentError
    """


class InvalidArgumentError(HoloDataFrameError, ValueError):
    """Raised when an argument is missing, malformed, or mutually exclusive.

    Also inherits from :class:`ValueError`, so existing ``except ValueError``
    handlers keep working.

    Typical causes:
        * ``df.write(..., overwrite=True, on_conflict=(...))`` -- the two are
          mutually exclusive.
        * ``df.join(other, on=123)`` -- ``on`` must be a column name, a list of
          names, or a :class:`~hologres.dataframe.column.Column`.
        * ``hg.from_records(rows)`` without a ``schema`` when it cannot be
          inferred.

    Example:
        >>> isinstance(InvalidArgumentError("bad"), ValueError)
        True
    """


class UnsupportedOperationError(HoloDataFrameError):
    """Raised when Hologres cannot support an otherwise well-formed request.

    Used for hard engine limits, which are reported instead of being silently
    downgraded.

    Typical causes:
        * ``df.write_files(path, format="parquet")`` -- ``EXTERNAL_FILES``
          only writes CSV.
        * A UDF signature using ``DECIMAL``/``TIMESTAMP``, which Function
          Compute remote UDFs do not accept.

    Example:
        >>> raise UnsupportedOperationError(
        ...     "write_files only supports format='csv'"
        ... )  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        UnsupportedOperationError: write_files only supports format='csv'
    """


class CompilationError(HoloDataFrameError):
    """Raised when a logical plan cannot be compiled into SQL.

    This is a client-side failure: nothing was sent to Hologres yet.

    Typical causes:
        * Referencing a column that no upstream operator produces.
        * An expression tree containing an unknown node kind.

    Example:
        >>> raise CompilationError(
        ...     "unknown column 'amt' in projection"
        ... )  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        CompilationError: unknown column 'amt' in projection
    """


class ExecutionError(HoloDataFrameError):
    """Raised when Hologres fails to execute a compiled statement.

    Args:
        message: Human-readable description of the failure.
        cause: The underlying driver or client exception, if any. It is also
            wired up as ``__cause__`` so tracebacks keep the original context.

    Attributes:
        cause: The original exception, or ``None``.

    Example:
        >>> err = ExecutionError("relation does not exist", cause=KeyError("x"))
        >>> isinstance(err.cause, KeyError)
        True
    """

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause
        if cause is not None:
            self.__cause__ = cause
            self.__suppress_context__ = True

    @classmethod
    def from_holo_client_exception(cls, exc: BaseException) -> ExecutionError:
        """Wrap a ``hologres.HoloClientException`` as an :class:`ExecutionError`.

        Write paths delegate to ``hologres-client``, which raises its own
        exception type. Wrapping keeps a single error hierarchy for callers
        while preserving the original exception as ``cause``.

        Args:
            exc: The exception raised by ``hologres-client`` (or any driver).

        Returns:
            An :class:`ExecutionError` carrying ``exc`` as its cause.

        Example:
            >>> wrapped = ExecutionError.from_holo_client_exception(
            ...     RuntimeError("copy failed")
            ... )
            >>> str(wrapped)
            'copy failed'
        """
        return cls(str(exc), cause=exc)
