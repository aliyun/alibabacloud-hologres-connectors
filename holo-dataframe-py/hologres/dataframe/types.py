"""Hologres data types for the DataFrame API.

Names follow common Python DataFrame conventions (``LongType``, ``StringType``,
``DoubleType``, ...). Each type knows the PostgreSQL type it compiles to,
exposed as :attr:`DataType.sql_name`.

Instances are immutable and hashable, so they compare by value and can be used
as dict keys or set members.

Where types appear in the public API:

* ``@hg.udf(input_types=[hg.IntegerType()], return_type=hg.StringType())``
* ``@hg.udtf(output_schema=hg.StructType([hg.StructField("word", hg.StringType())]))``
* ``session.read_files(path, format="csv", schema=hg.StructType([...]))``
* ``df.schema`` -> :class:`StructType` describing the result columns

Hologres-specific types use their PostgreSQL names directly: :class:`JsonbType`
and :class:`GeographyType` / :class:`GeometryType`.

Example:
    >>> import hologres.dataframe as hg
    >>> hg.LongType().sql_name
    'bigint'
    >>> hg.StringType().sql_name
    'text'
    >>> hg.StringType(64).sql_name
    'varchar(64)'
    >>> hg.ArrayType(hg.IntegerType()).sql_name
    'integer[]'
"""

from __future__ import annotations

import datetime
import decimal
import re
import typing
from dataclasses import dataclass, field
from enum import Enum

from .exceptions import InvalidArgumentError, UnsupportedOperationError

__all__ = [
    "ArrayType",
    "BinaryType",
    "BooleanType",
    "ByteType",
    "DataType",
    "DateType",
    "DecimalType",
    "DoubleType",
    "FC_SUPPORTED_TYPES",
    "FloatType",
    "GeographyType",
    "GeometryType",
    "IntegerType",
    "JsonbType",
    "LongType",
    "ShortType",
    "StringType",
    "StructField",
    "StructType",
    "TimeType",
    "TimestampTimeZone",
    "TimestampType",
    "VectorType",
    "from_arrow_type",
    "from_python_type",
    "parse_type",
]


@dataclass(frozen=True)
class DataType:
    """Base class for all Hologres data types.

    Subclasses are frozen dataclasses: immutable, compared by value, hashable.

    Example:
        >>> LongType() == LongType()
        True
        >>> len({LongType(), LongType()})
        1
    """

    @property
    def sql_name(self) -> str:
        """The PostgreSQL type name this type compiles to.

        Returns:
            The exact text to embed in generated SQL, e.g. ``'bigint'``,
            ``'numeric(38,0)'``, ``'text[]'``.

        Raises:
            NotImplementedError: If a subclass fails to define it.
        """
        raise NotImplementedError(f"{type(self).__name__} must define sql_name")

    @property
    def type_name(self) -> str:
        """Short DataFrame-style name, i.e. the class name without ``Type``.

        Returns:
            A lowercase name such as ``'long'``, ``'string'``, ``'array'``.

        Example:
            >>> LongType().type_name
            'long'
        """
        return type(self).__name__.removesuffix("Type").lower()

    def simple_string(self) -> str:
        """Compact human-readable form, following Spark's ``simpleString()``.

        Composite types nest, e.g. ``array<integer>`` and
        ``struct<id:long,name:string>``. Useful for logging and error messages.

        Returns:
            The compact type description.

        Example:
            >>> ArrayType(IntegerType()).simple_string()
            'array<integer>'
        """
        return self.type_name

    def to_arrow(self):
        """Convert to the corresponding ``pyarrow`` type.

        Arrow is the wire format for bulk reads, so this is the bridge used by
        ``to_pandas()``.

        Returns:
            A ``pyarrow.DataType``.

        Raises:
            ImportError: If ``pyarrow`` is not installed.
            UnsupportedOperationError: If the type has no Arrow equivalent.

        Example:
            >>> LongType().to_arrow()  # doctest: +SKIP
            DataType(int64)
        """
        return _to_arrow(self)

    def __str__(self) -> str:
        return self.sql_name


@dataclass(frozen=True)
class _NumericType(DataType):
    """Base class for numeric types."""


@dataclass(frozen=True)
class _IntegralType(_NumericType):
    """Base class for whole-number types (``ByteType`` .. ``LongType``)."""


@dataclass(frozen=True)
class _FractionalType(_NumericType):
    """Base class for types with a fractional part (float, double, decimal)."""


@dataclass(frozen=True)
class BooleanType(DataType):
    """``boolean`` -- true/false. Maps to Python ``bool``."""

    @property
    def sql_name(self) -> str:
        return "boolean"


@dataclass(frozen=True)
class ByteType(_IntegralType):
    """1-byte integer, compiled to PostgreSQL's single-byte ``"char"`` type.

    Caveat: PostgreSQL has no 1-byte *integer*. ``"char"`` is the only 1-byte
    type available, and it is a *character* type, so arithmetic and aggregates
    need an explicit cast::

        SELECT sum(b::int4) FROM t;   -- works
        SELECT sum(b) FROM t;         -- error: sum("char") does not exist

    Prefer :class:`ShortType` unless you specifically need the 1-byte storage.

    Example:
        >>> ByteType().sql_name
        '"char"'
    """

    @property
    def sql_name(self) -> str:
        return '"char"'


@dataclass(frozen=True)
class ShortType(_IntegralType):
    """``smallint`` -- 2-byte signed integer (PG alias ``int2``)."""

    @property
    def sql_name(self) -> str:
        return "smallint"


@dataclass(frozen=True)
class IntegerType(_IntegralType):
    """``integer`` -- 4-byte signed integer (PG alias ``int4``)."""

    @property
    def sql_name(self) -> str:
        return "integer"


@dataclass(frozen=True)
class LongType(_IntegralType):
    """``bigint`` -- 8-byte signed integer (PG alias ``int8``).

    Python ``int`` is unbounded, so :func:`from_python_type` infers this rather
    than :class:`IntegerType`.
    """

    @property
    def sql_name(self) -> str:
        return "bigint"


@dataclass(frozen=True)
class FloatType(_FractionalType):
    """``real`` -- 4-byte float (PG alias ``float4``)."""

    @property
    def sql_name(self) -> str:
        return "real"


@dataclass(frozen=True)
class DoubleType(_FractionalType):
    """``double precision`` -- 8-byte float (``float8``). Maps to Python ``float``."""

    @property
    def sql_name(self) -> str:
        return "double precision"


@dataclass(frozen=True)
class DecimalType(_FractionalType):
    """``numeric`` -- exact decimal, for money and other lossless arithmetic.

    Args:
        precision: Total number of digits. Defaults to ``38``. Pass ``None``
            for an unconstrained ``numeric``.
        scale: Digits after the decimal point. Defaults to ``0``, so
            ``DecimalType()`` keeps **no** fractional digits.
            Money should be declared explicitly, e.g. ``DecimalType(38, 2)``.

    Raises:
        InvalidArgumentError: If only one of ``precision``/``scale`` is ``None``,
            if either is negative, or if ``scale`` exceeds ``precision``.

    Example:
        >>> DecimalType().sql_name
        'numeric(38,0)'
        >>> DecimalType(10, 2).sql_name
        'numeric(10,2)'
        >>> DecimalType(None, None).sql_name
        'numeric'
    """

    precision: int | None = 38
    scale: int | None = 0

    def __post_init__(self) -> None:
        if (self.precision is None) != (self.scale is None):
            raise InvalidArgumentError(
                "DecimalType precision and scale must both be set or both be "
                f"None, got precision={self.precision}, scale={self.scale}"
            )
        if self.precision is None:
            return
        if isinstance(self.precision, bool) or not isinstance(self.precision, int):
            raise InvalidArgumentError(
                f"DecimalType precision must be an int, got {self.precision!r}"
            )
        if isinstance(self.scale, bool) or not isinstance(self.scale, int):
            raise InvalidArgumentError(
                f"DecimalType scale must be an int, got {self.scale!r}"
            )
        if self.precision < 1:
            raise InvalidArgumentError(
                f"DecimalType precision must be >= 1, got {self.precision}"
            )
        if self.scale < 0:
            raise InvalidArgumentError(
                f"DecimalType scale must be >= 0, got {self.scale}"
            )
        if self.scale > self.precision:
            raise InvalidArgumentError(
                f"DecimalType scale {self.scale} exceeds precision {self.precision}"
            )

    @property
    def sql_name(self) -> str:
        if self.precision is None:
            return "numeric"
        return f"numeric({self.precision},{self.scale})"

    def simple_string(self) -> str:
        if self.precision is None:
            return "decimal"
        return f"decimal({self.precision},{self.scale})"


@dataclass(frozen=True)
class StringType(DataType):
    """Variable-length string, compiled to ``text`` or ``varchar(n)``.

    Uses one string type with an optional length rather than separate
    ``text``/``varchar`` classes.

    Args:
        length: Maximum number of characters. ``None`` (the default) compiles to
            ``text``, which is the preferred Hologres string type; a value
            compiles to ``varchar(n)``, which enforces the limit.

    Raises:
        InvalidArgumentError: If ``length`` is not positive.

    Example:
        >>> StringType().sql_name
        'text'
        >>> StringType(64).sql_name
        'varchar(64)'
    """

    length: int | None = None

    def __post_init__(self) -> None:
        if self.length is not None and (
            isinstance(self.length, bool)
            or not isinstance(self.length, int)
            or self.length < 1
        ):
            raise InvalidArgumentError(
                f"StringType length must be a positive int, got {self.length!r}"
            )

    @property
    def sql_name(self) -> str:
        return "text" if self.length is None else f"varchar({self.length})"

    def simple_string(self) -> str:
        return "string" if self.length is None else f"string({self.length})"


@dataclass(frozen=True)
class BinaryType(DataType):
    """``bytea`` -- variable-length binary data. Maps to Python ``bytes``."""

    @property
    def sql_name(self) -> str:
        return "bytea"


@dataclass(frozen=True)
class DateType(DataType):
    """``date`` -- calendar date without time. Maps to ``datetime.date``."""

    @property
    def sql_name(self) -> str:
        return "date"


@dataclass(frozen=True)
class TimeType(DataType):
    """``time`` -- time of day without date. Maps to ``datetime.time``."""

    @property
    def sql_name(self) -> str:
        return "time"


class TimestampTimeZone(Enum):
    """Time-zone flavor of a :class:`TimestampType`.

    Attributes:
        DEFAULT: A convenience alias for ``NTZ``. Hologres has no session-level
            default time zone, so a ``TimestampType`` built with ``DEFAULT`` is
            normalized to ``NTZ`` on construction; both compile to ``timestamp``.
        NTZ: No time zone -- ``timestamp``.
        TZ: With time zone -- ``timestamptz``.
        LTZ: Local time zone. Hologres does not support this flavor; constructing
            a ``TimestampType`` with it raises
            :class:`~hologres.dataframe.exceptions.UnsupportedOperationError`.
    """

    DEFAULT = "default"
    NTZ = "ntz"
    LTZ = "ltz"
    TZ = "tz"


@dataclass(frozen=True)
class TimestampType(DataType):
    """``timestamp`` / ``timestamptz`` -- a point in time.

    Args:
        timezone: Which flavor to use. See :class:`TimestampTimeZone`.
            ``DEFAULT`` and ``NTZ`` compile to ``timestamp``; ``TZ`` compiles to
            ``timestamptz``.

    Raises:
        UnsupportedOperationError: If ``timezone`` is
            :attr:`TimestampTimeZone.LTZ`, which PostgreSQL has no equivalent
            for.
        InvalidArgumentError: If ``timezone`` is not a
            :class:`TimestampTimeZone`.

    Example:
        >>> TimestampType().sql_name
        'timestamp'
        >>> TimestampType(TimestampTimeZone.TZ).sql_name
        'timestamptz'
    """

    timezone: TimestampTimeZone = TimestampTimeZone.DEFAULT

    def __post_init__(self) -> None:
        if not isinstance(self.timezone, TimestampTimeZone):
            raise InvalidArgumentError(
                f"TimestampType timezone must be a TimestampTimeZone, "
                f"got {type(self.timezone).__name__}"
            )
        if self.timezone is TimestampTimeZone.LTZ:
            raise UnsupportedOperationError(
                "Hologres has no local-time-zone timestamp; use "
                "TimestampTimeZone.TZ (timestamptz) or NTZ (timestamp)"
            )
        # DEFAULT and NTZ are indistinguishable in Hologres; normalize to NTZ so
        # equality and Arrow/parse round-trips stay consistent.
        if self.timezone is TimestampTimeZone.DEFAULT:
            object.__setattr__(self, "timezone", TimestampTimeZone.NTZ)

    @property
    def sql_name(self) -> str:
        if self.timezone is TimestampTimeZone.TZ:
            return "timestamptz"
        return "timestamp"

    def simple_string(self) -> str:
        return "timestamp_tz" if self.timezone is TimestampTimeZone.TZ else "timestamp"


@dataclass(frozen=True)
class JsonbType(DataType):
    """``jsonb`` -- JSON stored in binary form; indexable and order-normalized.

    Hologres stores this semi-structured value as ``jsonb``. It is preferred
    over ``json`` because it supports indexing and operator pushdown.
    """

    @property
    def sql_name(self) -> str:
        return "jsonb"


@dataclass(frozen=True)
class GeographyType(DataType):
    """``geography`` -- PostGIS spherical geospatial value.

    Requires the PostGIS extension in the target Hologres instance.
    """

    @property
    def sql_name(self) -> str:
        return "geography"


@dataclass(frozen=True)
class GeometryType(DataType):
    """``geometry`` -- PostGIS planar geospatial value.

    Requires the PostGIS extension in the target Hologres instance.
    """

    @property
    def sql_name(self) -> str:
        return "geometry"


@dataclass(frozen=True)
class ArrayType(DataType):
    """PostgreSQL array of a single element type.

    Args:
        element_type: Type of the array elements. Nesting is allowed.

    Raises:
        InvalidArgumentError: If ``element_type`` is not a :class:`DataType`.

    Example:
        >>> ArrayType(StringType()).sql_name
        'text[]'
        >>> ArrayType(ArrayType(IntegerType())).sql_name
        'integer[][]'
    """

    element_type: DataType

    def __post_init__(self) -> None:
        if not isinstance(self.element_type, DataType):
            raise InvalidArgumentError(
                f"ArrayType element_type must be a DataType, "
                f"got {type(self.element_type).__name__}"
            )

    @property
    def sql_name(self) -> str:
        return f"{self.element_type.sql_name}[]"

    def simple_string(self) -> str:
        return f"array<{self.element_type.simple_string()}>"


@dataclass(frozen=True)
class VectorType(DataType):
    """``vector`` -- pgvector embedding column, the target of vector search.

    Args:
        element_type: Python element type. Only ``float`` is supported, because
            pgvector stores 32-bit floats. ``element_type`` is kept explicit
            so schemas can describe the vector element type.
        dimension: Number of dimensions. Must be positive.

    Raises:
        InvalidArgumentError: If ``dimension`` is not positive.
        UnsupportedOperationError: If ``element_type`` is not ``float``.

    Example:
        >>> VectorType(float, 768).sql_name
        'vector(768)'
    """

    element_type: type = float
    dimension: int = 0

    def __post_init__(self) -> None:
        if self.element_type is not float:
            el_name = getattr(self.element_type, "__name__", self.element_type)
            raise UnsupportedOperationError(
                "pgvector only stores 32-bit floats; VectorType element_type "
                f"must be float, got {el_name!r}"
            )
        if (
            isinstance(self.dimension, bool)
            or not isinstance(self.dimension, int)
            or self.dimension < 1
        ):
            raise InvalidArgumentError(
                f"VectorType dimension must be >= 1, got {self.dimension!r}"
            )

    @property
    def sql_name(self) -> str:
        return f"vector({self.dimension})"

    def simple_string(self) -> str:
        return f"vector<float,{self.dimension}>"


@dataclass(frozen=True)
class StructField:
    """One named field of a :class:`StructType`.

    Args:
        name: Field name, used verbatim as the SQL column name.
        datatype: Type of the field.
        nullable: Whether the field accepts NULL. Informational only today:
            neither ``EXTERNAL_FILES`` schemas nor UDTF signatures emit
            ``NOT NULL``.

    Raises:
        InvalidArgumentError: If ``name`` is empty or not a string, or if
            ``datatype`` is not a :class:`DataType`.

    Example:
        >>> str(StructField("word", StringType()))
        'word text'
    """

    name: str
    datatype: DataType
    nullable: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise InvalidArgumentError("StructField name must be a non-empty string")
        if not isinstance(self.datatype, DataType):
            raise InvalidArgumentError(
                f"StructField datatype must be a DataType, "
                f"got {type(self.datatype).__name__}"
            )

    def __str__(self) -> str:
        return f"{self.name} {self.datatype.sql_name}"


@dataclass(frozen=True)
class StructType(DataType):
    """An ordered, named list of fields describing the shape of a row.

    This is a client-side schema descriptor, not a Hologres column type. It is
    what ``df.schema`` returns, what ``@hg.udtf(output_schema=...)`` declares,
    and what ``session.read_files(schema=...)`` accepts.

    Args:
        fields: The fields, in order. A list is accepted and stored as a tuple
            so instances stay hashable.

    Raises:
        InvalidArgumentError: If any element is not a :class:`StructField`, or
            if two fields share a name.

    Example:
        >>> schema = StructType([
        ...     StructField("id", LongType()),
        ...     StructField("word", StringType()),
        ... ])
        >>> schema.to_ddl()
        'id bigint, word text'
        >>> schema.names
        ('id', 'word')
        >>> schema["word"].datatype.sql_name
        'text'
        >>> len(schema)
        2
    """

    fields: tuple[StructField, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        fields = tuple(self.fields)
        for f in fields:
            if not isinstance(f, StructField):
                raise InvalidArgumentError(
                    f"StructType fields must be StructField instances, "
                    f"got {type(f).__name__}"
                )
        names = [f.name for f in fields]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise InvalidArgumentError(
                f"StructType has duplicate field names: {duplicates}"
            )
        object.__setattr__(self, "fields", fields)

    @property
    def names(self) -> tuple[str, ...]:
        """Field names, in declaration order."""
        return tuple(f.name for f in self.fields)

    @property
    def sql_name(self) -> str:
        """``record`` -- PostgreSQL's anonymous composite type.

        A struct has no standalone type name in SQL. To emit a field list for
        an ``AS (...)`` or ``RETURNS TABLE (...)`` clause, use :meth:`to_ddl`.
        """
        return "record"

    def to_ddl(self) -> str:
        """Render the fields as a SQL column list.

        This is the text that goes inside ``EXTERNAL_FILES(...) AS (...)`` and
        ``RETURNS TABLE (...)``.

        Returns:
            Comma-separated ``name type`` pairs, e.g. ``'id bigint, word text'``.
            An empty struct yields an empty string.

        Example:
            >>> StructType([StructField("n", IntegerType())]).to_ddl()
            'n integer'
        """
        return ", ".join(str(f) for f in self.fields)

    def simple_string(self) -> str:
        inner = ",".join(f"{f.name}:{f.datatype.simple_string()}" for f in self.fields)
        return f"struct<{inner}>"

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self):
        return iter(self.fields)

    def __getitem__(self, key: int | str) -> StructField:
        """Look up a field by position or by name.

        Args:
            key: Zero-based index, or a field name.

        Returns:
            The matching :class:`StructField`.

        Raises:
            IndexError: If an integer index is out of range.
            KeyError: If no field has the given name.
        """
        if isinstance(key, int):
            return self.fields[key]
        for f in self.fields:
            if f.name == key:
                return f
        raise KeyError(f"No field named {key!r} in {self.names}")


#: Types accepted by Function Compute remote UDFs (``LANGUAGE function_compute``).
#:
#: Function Compute exchanges rows as JSON, so only these scalar types and their
#: array forms are allowed. ``numeric``/``timestamp``/``jsonb`` arguments must be
#: cast to ``bigint`` or ``text`` before reaching a UDF. Note that only the
#: unconstrained ``StringType()`` (``text``) qualifies -- ``StringType(n)``
#: compiles to ``varchar(n)``, which Function Compute does not accept.
FC_SUPPORTED_TYPES: frozenset[DataType] = frozenset(
    [
        BooleanType(),
        IntegerType(),
        LongType(),
        FloatType(),
        DoubleType(),
        StringType(),
        ArrayType(BooleanType()),
        ArrayType(IntegerType()),
        ArrayType(LongType()),
        ArrayType(FloatType()),
        ArrayType(DoubleType()),
        ArrayType(StringType()),
    ]
)


_PYTHON_TYPE_MAP: dict[type, DataType] = {
    bool: BooleanType(),
    int: LongType(),
    float: DoubleType(),
    str: StringType(),
    bytes: BinaryType(),
    # Preserve fractional digits when inferring Decimal values from Python.
    # DecimalType's default scale is 0, which would silently truncate them.
    decimal.Decimal: DecimalType(38, 18),
    datetime.date: DateType(),
    datetime.time: TimeType(),
    datetime.datetime: TimestampType(),
}


def from_python_type(tp: typing.Any) -> DataType:
    """Infer a Hologres type from a Python type annotation.

    This is how ``@hg.udf`` derives a signature when ``input_types`` and
    ``return_type`` are omitted: ``def grade(n: int) -> str`` becomes
    ``grade(bigint) RETURNS text``.

    Mapping:
        ``bool`` -> ``boolean``, ``int`` -> ``bigint``, ``float`` ->
        ``double precision``, ``str`` -> ``text``, ``bytes`` -> ``bytea``,
        ``decimal.Decimal`` -> ``numeric(38,18)``, ``datetime.date`` -> ``date``,
        ``datetime.time`` -> ``time``, ``datetime.datetime`` -> ``timestamp``.

    ``list[X]``, ``set[X]``, ``frozenset[X]`` and ``tuple[X]`` become
    ``ArrayType(X)``. ``Optional[X]`` unwraps to ``X``, because nullability is
    not part of a Hologres column type.

    Args:
        tp: A Python type or typing annotation. A :class:`DataType` instance is
            returned unchanged, so callers may pass either form.

    Returns:
        The corresponding :class:`DataType`.

    Raises:
        InvalidArgumentError: If ``tp`` has no Hologres equivalent, or is a
            union of more than one non-``None`` member.

    Example:
        >>> from_python_type(int).sql_name
        'bigint'
        >>> from_python_type(typing.Optional[str]).sql_name
        'text'
        >>> from_python_type(typing.List[int]).sql_name
        'bigint[]'
    """
    if isinstance(tp, DataType):
        return tp

    origin = typing.get_origin(tp)
    if origin is not None:
        args = typing.get_args(tp)
        # typing.Union covers Optional[X]; the "UnionType" name covers the
        # PEP 604 form (X | None), whose get_origin() is types.UnionType.
        if origin is typing.Union or getattr(origin, "__name__", "") == "UnionType":
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) != 1:
                raise InvalidArgumentError(
                    f"Cannot map union type {tp!r} to a Hologres type; "
                    f"only Optional[X] is supported"
                )
            return from_python_type(non_none[0])
        if origin in (list, set, frozenset, tuple):
            # Tuple[X, ...] is the idiomatic homogeneous variadic tuple.
            if origin is tuple and len(args) == 2 and args[1] is Ellipsis:
                return ArrayType(from_python_type(args[0]))
            if len(args) != 1:
                raise InvalidArgumentError(
                    f"Cannot map {tp!r} to an array type; "
                    f"exactly one element type is required"
                )
            return ArrayType(from_python_type(args[0]))
        raise InvalidArgumentError(f"Cannot map generic type {tp!r} to a Hologres type")

    mapped = _PYTHON_TYPE_MAP.get(tp)
    if mapped is None:
        name = getattr(tp, "__name__", repr(tp))
        raise InvalidArgumentError(
            f"Cannot map Python type {name} to a Hologres type; "
            f"pass an explicit DataType instead"
        )
    return mapped


_SCALAR_TYPE_ALIASES: dict[str, DataType] = {
    "bool": BooleanType(),
    "boolean": BooleanType(),
    '"char"': ByteType(),
    "int2": ShortType(),
    "smallint": ShortType(),
    "int": IntegerType(),
    "int4": IntegerType(),
    "integer": IntegerType(),
    "int8": LongType(),
    "bigint": LongType(),
    "float4": FloatType(),
    "real": FloatType(),
    "float8": DoubleType(),
    "double": DoubleType(),
    "double precision": DoubleType(),
    "text": StringType(),
    "varchar": StringType(),
    "character varying": StringType(),
    "bpchar": StringType(),
    "date": DateType(),
    "time": TimeType(),
    "timestamp": TimestampType(),
    "timestamp without time zone": TimestampType(),
    "timestamptz": TimestampType(TimestampTimeZone.TZ),
    "timestamp with time zone": TimestampType(TimestampTimeZone.TZ),
    "jsonb": JsonbType(),
    "bytea": BinaryType(),
    "geography": GeographyType(),
    "geometry": GeometryType(),
}

_PARAMETRIC_TYPE_RE = re.compile(r"^([a-z ]+?)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)$")


def parse_type(name: str) -> DataType:
    """Parse a PostgreSQL type name into a :class:`DataType`.

    Turns server-reported or user-supplied type names into typed objects -- used
    to build ``df.schema`` from a cursor description, and to accept a
    ``read_files`` schema written as text.

    Parsing is case-insensitive and collapses internal whitespace.

    Supported forms:
        * Scalars and their PG aliases: ``bigint``/``int8``,
          ``integer``/``int4``/``int``, ``double precision``/``float8``,
          ``timestamptz``/``timestamp with time zone``, ``jsonb``, ``bytea``,
          ``geography``, ``geometry``, ``"char"``
        * Parametric: ``numeric(10,2)``, ``varchar(64)``, ``vector(768)``
        * Arrays, nested to any depth: ``text[]``, ``integer[][]``

    Note:
        Bare ``numeric`` parses to ``DecimalType(None, None)`` (unconstrained),
        which is what PostgreSQL reports; this differs from ``DecimalType()``,
        whose DataType default is ``numeric(38,0)``.

    Args:
        name: The type name, e.g. ``'numeric(10,2)'`` or ``'text[]'``.

    Returns:
        The parsed :class:`DataType`.

    Raises:
        InvalidArgumentError: If ``name`` is empty, not a string, or not a
            recognized type.

    Example:
        >>> parse_type("int8")
        LongType()
        >>> parse_type("VARCHAR(64)").sql_name
        'varchar(64)'
        >>> parse_type("text[]").sql_name
        'text[]'
        >>> parse_type("numeric").sql_name
        'numeric'
    """
    if not isinstance(name, str) or not name.strip():
        raise InvalidArgumentError("Type name must be a non-empty string")

    normalized = " ".join(name.strip().lower().split())

    if normalized.endswith("[]"):
        return ArrayType(parse_type(normalized[:-2]))

    scalar = _SCALAR_TYPE_ALIASES.get(normalized)
    if scalar is not None:
        return scalar
    if normalized in ("numeric", "decimal"):
        return DecimalType(None, None)
    if normalized == "vector":
        raise InvalidArgumentError(
            "vector requires an explicit dimension, e.g. 'vector(768)'"
        )

    match = _PARAMETRIC_TYPE_RE.match(normalized)
    if match is not None:
        base, first, second = match.group(1).strip(), match.group(2), match.group(3)
        if base in ("numeric", "decimal"):
            return DecimalType(int(first), int(second) if second else 0)
        if base in ("varchar", "character varying") and second is None:
            return StringType(int(first))
        if base in ("char", "character", "bpchar") and second is None:
            return StringType(int(first))
        if base == "vector" and second is None:
            return VectorType(float, int(first))

    raise InvalidArgumentError(f"Unrecognized Hologres type name: {name!r}")


def _import_pyarrow():
    """Import pyarrow, raising a clear error when it is missing."""
    try:
        import pyarrow as pa

        return pa
    except ImportError:
        raise ImportError(
            "pyarrow is required for Arrow conversion. "
            "Reinstall or upgrade hologres-dataframe to restore its required "
            "dependencies."
        ) from None


def _to_arrow(datatype: DataType):
    """Map a :class:`DataType` to a ``pyarrow`` type."""
    pa = _import_pyarrow()

    if isinstance(datatype, ArrayType):
        return pa.list_(_to_arrow(datatype.element_type))
    if isinstance(datatype, StructType):
        return pa.struct(
            [
                pa.field(f.name, _to_arrow(f.datatype), nullable=f.nullable)
                for f in datatype.fields
            ]
        )
    if isinstance(datatype, DecimalType):
        precision = 38 if datatype.precision is None else datatype.precision
        scale = 0 if datatype.scale is None else datatype.scale
        return pa.decimal128(precision, scale)
    if isinstance(datatype, TimestampType):
        tz = "UTC" if datatype.timezone is TimestampTimeZone.TZ else None
        return pa.timestamp("us", tz=tz)
    if isinstance(datatype, VectorType):
        return pa.list_(pa.float32(), datatype.dimension)

    simple = {
        BooleanType(): "bool_",
        ByteType(): "int8",
        ShortType(): "int16",
        IntegerType(): "int32",
        LongType(): "int64",
        FloatType(): "float32",
        DoubleType(): "float64",
        StringType(): "utf8",
        BinaryType(): "binary",
        DateType(): "date32",
    }
    factory = simple.get(datatype)
    if factory is not None:
        return getattr(pa, factory)()
    if isinstance(datatype, StringType):
        return pa.utf8()
    if isinstance(datatype, TimeType):
        return pa.time64("us")
    if isinstance(datatype, (JsonbType, GeographyType, GeometryType)):
        return pa.utf8()

    raise UnsupportedOperationError(
        f"No Arrow equivalent for {datatype.simple_string()}"
    )


def from_arrow_type(arrow_type) -> DataType:
    """Map a ``pyarrow`` type back to a :class:`DataType`.

    Used when a bulk read hands back Arrow batches and the caller wants the
    Hologres-side schema.

    Args:
        arrow_type: A ``pyarrow.DataType``.

    Returns:
        The corresponding :class:`DataType`.

    Raises:
        ImportError: If ``pyarrow`` is not installed.
        UnsupportedOperationError: If the Arrow type has no Hologres equivalent.

    Example:
        >>> import pyarrow as pa  # doctest: +SKIP
        >>> from_arrow_type(pa.int64())  # doctest: +SKIP
        LongType()
    """
    pa = _import_pyarrow()

    if pa.types.is_fixed_size_list(arrow_type):
        if pa.types.is_float32(arrow_type.value_type):
            return VectorType(float, arrow_type.list_size)
        return ArrayType(from_arrow_type(arrow_type.value_type))
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return ArrayType(from_arrow_type(arrow_type.value_type))
    if pa.types.is_struct(arrow_type):
        return StructType(
            [
                StructField(f.name, from_arrow_type(f.type), f.nullable)
                for f in arrow_type
            ]
        )
    if pa.types.is_decimal(arrow_type):
        return DecimalType(arrow_type.precision, arrow_type.scale)
    if pa.types.is_timestamp(arrow_type):
        tz = TimestampTimeZone.TZ if arrow_type.tz else TimestampTimeZone.NTZ
        return TimestampType(tz)
    if pa.types.is_boolean(arrow_type):
        return BooleanType()
    if pa.types.is_int8(arrow_type):
        return ByteType()
    if pa.types.is_int16(arrow_type):
        return ShortType()
    if pa.types.is_int32(arrow_type):
        return IntegerType()
    if pa.types.is_int64(arrow_type):
        return LongType()
    if pa.types.is_float32(arrow_type):
        return FloatType()
    if pa.types.is_float64(arrow_type):
        return DoubleType()
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return StringType()
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return BinaryType()
    if pa.types.is_date(arrow_type):
        return DateType()
    if pa.types.is_time(arrow_type):
        return TimeType()

    raise UnsupportedOperationError(
        f"No Hologres equivalent for Arrow type {arrow_type}"
    )
