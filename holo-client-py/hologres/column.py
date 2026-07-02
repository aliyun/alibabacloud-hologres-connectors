from __future__ import annotations

from typing import Any, Optional

# SQL type constants (mirroring java.sql.Types used in the Java client)
BOOLEAN = 16
BIT = -7
TINYINT = -6
SMALLINT = 5
INTEGER = 4
BIGINT = -5
REAL = 7
DOUBLE = 8
NUMERIC = 2
DECIMAL = 3
CHAR = 1
VARCHAR = 12
DATE = 91
TIME = 92
TIMESTAMP = 93
TIMESTAMP_WITH_TIMEZONE = 2014
BINARY = -2
OTHER = 1111
ARRAY = 2003

# Map from PostgreSQL type name to SQL type constant
_PG_TYPE_MAP: dict[str, int] = {
    "bool": BOOLEAN,
    "boolean": BOOLEAN,
    "bit": BIT,
    "int2": SMALLINT,
    "smallint": SMALLINT,
    "smallserial": SMALLINT,
    "int4": INTEGER,
    "integer": INTEGER,
    "serial": INTEGER,
    "int8": BIGINT,
    "bigint": BIGINT,
    "bigserial": BIGINT,
    "float4": REAL,
    "real": REAL,
    "float8": DOUBLE,
    "double precision": DOUBLE,
    "numeric": NUMERIC,
    "decimal": DECIMAL,
    "money": NUMERIC,
    "char": CHAR,
    "character": CHAR,
    "bpchar": CHAR,
    "varchar": VARCHAR,
    "character varying": VARCHAR,
    "text": VARCHAR,
    "name": VARCHAR,
    "date": DATE,
    "time": TIME,
    "timetz": TIME,
    "time with time zone": TIME,
    "timestamp": TIMESTAMP,
    "timestamptz": TIMESTAMP_WITH_TIMEZONE,
    "timestamp with time zone": TIMESTAMP_WITH_TIMEZONE,
    "timestamp without time zone": TIMESTAMP,
    "bytea": BINARY,
    "json": VARCHAR,
    "jsonb": VARCHAR,
    "uuid": VARCHAR,
    "inet": VARCHAR,
    "cidr": VARCHAR,
    "macaddr": VARCHAR,
    "xml": VARCHAR,
    "point": OTHER,
    "line": OTHER,
    "polygon": OTHER,
    "circle": OTHER,
    "box": OTHER,
    "path": OTHER,
    "interval": OTHER,
    "roaringbitmap": BINARY,
}

# Array element type mapping
_ARRAY_ELEMENT_TYPE_MAP: dict[str, int] = {
    "char": CHAR,
    "int2": SMALLINT,
    "int4": INTEGER,
    "int8": BIGINT,
    "float4": REAL,
    "float8": DOUBLE,
    "bool": BIT,
    "boolean": BIT,
    "text": VARCHAR,
    "varchar": VARCHAR,
}


class Column:
    """Represents a single column definition in a Hologres table."""

    __slots__ = (
        "name",
        "type_name",
        "type",
        "comment",
        "allow_null",
        "is_primary_key",
        "default_value",
        "is_array_type",
        "array_element_type",
        "precision",
        "scale",
        "is_generated_column",
    )

    def __init__(
        self,
        name: str,
        type_name: str = "",
        type: int = OTHER,
        comment: Optional[str] = None,
        allow_null: bool = True,
        is_primary_key: bool = False,
        default_value: Any = None,
        is_array_type: bool = False,
        array_element_type: int = 0,
        precision: int = 0,
        scale: int = 0,
        is_generated_column: bool = False,
    ):
        self.name = name
        self.type_name = type_name
        self.type = type
        self.comment = comment
        self.allow_null = allow_null
        self.is_primary_key = is_primary_key
        self.default_value = default_value
        self.is_array_type = is_array_type
        self.array_element_type = array_element_type
        self.precision = precision
        self.scale = scale
        self.is_generated_column = is_generated_column

    @classmethod
    def from_pg_type_name(cls, name: str, pg_type_name: str, **kwargs) -> Column:
        """Create a Column from a PostgreSQL type name string."""
        raw = pg_type_name.strip().lower()
        is_array = raw.endswith("[]") or raw.startswith("_")

        if is_array:
            element_name = raw.rstrip("[]").lstrip("_")
            sql_type = ARRAY
            element_type = _ARRAY_ELEMENT_TYPE_MAP.get(element_name, VARCHAR)
            return cls(
                name=name,
                type_name=pg_type_name,
                type=sql_type,
                is_array_type=True,
                array_element_type=element_type,
                **kwargs,
            )

        sql_type = _PG_TYPE_MAP.get(raw, OTHER)
        return cls(name=name, type_name=pg_type_name, type=sql_type, **kwargs)

    @property
    def is_serial(self) -> bool:
        return self.type_name.lower() in ("serial", "bigserial", "smallserial")

    def __repr__(self) -> str:
        pk = " PK" if self.is_primary_key else ""
        null = "" if self.allow_null else " NOT NULL"
        return f"Column({self.name!r}, {self.type_name!r}{pk}{null})"
