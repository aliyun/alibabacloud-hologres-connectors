"""Hologres DataFrame API -- lazy, pandas-style querying that compiles to SQL.

Import as ``hg`` and build queries by chaining. Nothing touches Hologres until
an *action* (``collect``, ``to_pandas``, ``count``, ``show``, ...) runs; up to
that point you are only assembling a logical plan, which is then compiled into a
single PostgreSQL statement and pushed down to Hologres.

    import hologres.dataframe as hg

    session = hg.connect(
        host="xxx.hologres.aliyuncs.com",
        port=80,
        dbname="my_db",
        user="<access_id>",
        password="<access_key>",
    )

    result = (
        session.table("public.orders")
          .filter(hg.col("amount") > 100)
          .with_column("tax", hg.col("amount") * 0.06)
          .select("order_id", "region", "amount", "tax")
          .sort(hg.col("amount"), ascending=False)
    )
    result.show(10)   # only here does anything reach Hologres

Design notes:

* Operators use snake_case only (``group_by``, not ``groupBy``), matching the
  surrounding Python data tooling.
* Types use familiar DataFrame names (:class:`LongType`, :class:`StringType`)
  and each knows the PostgreSQL type it compiles to.
* Because computation stays inside the engine, only results cross back into
  Python.

This module is the single public entry point; everything re-exported here is
supported API, and anything else is internal.
"""

from . import ai
from ._sources import from_dict, from_pandas, from_records
from ._version import __version__
from .column import Column
from .dataframe import DataFrame
from .exceptions import (
    CompilationError,
    ExecutionError,
    HoloDataFrameError,
    InvalidArgumentError,
    UnsupportedOperationError,
)
from .functions import avg, col, count, expr, function, lit, max, min, sum
from .grouped import GroupedDataFrame
from .row import Row
from .session import Session, connect
from .types import (
    FC_SUPPORTED_TYPES,
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    GeometryType,
    IntegerType,
    JsonbType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VectorType,
    from_arrow_type,
    from_python_type,
    parse_type,
)
from .udf import udf, udtf

__all__ = [
    # Namespaces
    "ai",
    # Errors
    "CompilationError",
    "ExecutionError",
    "HoloDataFrameError",
    "InvalidArgumentError",
    "UnsupportedOperationError",
    # Column expressions
    "Column",
    "DataFrame",
    "GroupedDataFrame",
    "avg",
    "col",
    "count",
    "expr",
    "function",
    "lit",
    "max",
    "min",
    "sum",
    "udf",
    "udtf",
    # Result rows
    "Row",
    # Session
    "Session",
    "connect",
    # Client-side sources
    "from_dict",
    "from_pandas",
    "from_records",
    # Types
    "ArrayType",
    "BinaryType",
    "BooleanType",
    "ByteType",
    "DataType",
    "DateType",
    "DecimalType",
    "DoubleType",
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
    # Type helpers
    "FC_SUPPORTED_TYPES",
    "from_arrow_type",
    "from_python_type",
    "parse_type",
    # Metadata
    "__version__",
]
