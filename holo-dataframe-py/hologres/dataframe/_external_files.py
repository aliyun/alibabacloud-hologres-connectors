"""Validation and SQL builders for Hologres ``EXTERNAL_FILES``."""

from __future__ import annotations

from typing import Any

from psycopg import sql

from .exceptions import InvalidArgumentError, UnsupportedOperationError
from .types import StructType

__all__ = [
    "build_external_files_relation",
    "build_write_files_sql",
    "normalize_external_options",
    "validate_oss_read",
    "validate_oss_write",
]

_READ_FORMATS = ("csv", "parquet", "orc")
_READ_OPTIONS = {
    "csv_skip_header",
    "csv_delimiter",
    "schema_deduce_file_num",
    "schema_deduce_file_order",
}
_SCHEMA_DEDUCE_ORDERS = ("latest_first", "earliest_first", "random")


def _require_oss_path(path) -> str:
    if not isinstance(path, str) or not path.startswith("oss://"):
        raise InvalidArgumentError("path must be a non-empty oss:// URI")
    if path == "oss://":
        raise InvalidArgumentError("path must include an OSS bucket")
    return path


def _optional_string(name: str, value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise InvalidArgumentError(f"{name} must be a non-empty string or None")
    return value


def _external_option_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def normalize_external_options(options: dict[str, Any]) -> dict[str, str]:
    """Validate read options and convert every value to server-required text."""
    unknown = set(options) - _READ_OPTIONS
    if unknown:
        raise InvalidArgumentError(
            f"unknown OSS read option(s) {tuple(sorted(unknown))}; supported: "
            f"{tuple(sorted(_READ_OPTIONS))}"
        )
    normalized = {key: _external_option_value(value) for key, value in options.items()}
    if "csv_skip_header" in options and not isinstance(
        options["csv_skip_header"], bool
    ):
        raise InvalidArgumentError("csv_skip_header must be a bool")
    if "csv_delimiter" in options:
        delimiter = options["csv_delimiter"]
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            raise InvalidArgumentError("csv_delimiter must be one character")
    if "schema_deduce_file_num" in options:
        count = options["schema_deduce_file_num"]
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise InvalidArgumentError("schema_deduce_file_num must be a positive int")
    if "schema_deduce_file_order" in options:
        order = options["schema_deduce_file_order"]
        if order not in _SCHEMA_DEDUCE_ORDERS:
            raise InvalidArgumentError(
                "schema_deduce_file_order must be latest_first, earliest_first, "
                "or random"
            )
    return normalized


def validate_oss_read(
    path,
    format,
    schema,
    *,
    oss_endpoint=None,
    role_arn=None,
    options=None,
):
    path = _require_oss_path(path)
    if format not in _READ_FORMATS:
        raise InvalidArgumentError(
            f"OSS read format must be one of {_READ_FORMATS}, got {format!r}"
        )
    if schema is not None and not isinstance(schema, StructType):
        raise InvalidArgumentError("schema must be a StructType or None")
    normalized = normalize_external_options(options or {})
    if format != "csv" and {
        "csv_skip_header",
        "csv_delimiter",
    } & set(normalized):
        raise InvalidArgumentError("CSV options require format='csv'")
    endpoint = _optional_string("oss_endpoint", oss_endpoint)
    role = _optional_string("role_arn", role_arn)
    if endpoint is not None:
        normalized["oss_endpoint"] = endpoint
    if role is not None:
        normalized["role_arn"] = role
    return path, format, schema, normalized


def validate_oss_write(
    path,
    format,
    *,
    oss_endpoint=None,
    role_arn=None,
    single_file=False,
    target_file_size_mb=10,
):
    path = _require_oss_path(path)
    if format != "csv":
        raise UnsupportedOperationError("write_files only supports format='csv'")
    endpoint = _optional_string("oss_endpoint", oss_endpoint)
    role = _optional_string("role_arn", role_arn)
    if not isinstance(single_file, bool):
        raise InvalidArgumentError("single_file must be a bool")
    if (
        isinstance(target_file_size_mb, bool)
        or not isinstance(target_file_size_mb, int)
        or target_file_size_mb <= 0
    ):
        raise InvalidArgumentError("target_file_size_mb must be a positive int")
    options = {}
    if endpoint is not None:
        options["oss_endpoint"] = endpoint
    if role is not None:
        options["role_arn"] = role
    if single_file:
        options["single_file"] = "true"
    else:
        options["target_file_size_mb"] = str(target_file_size_mb)
    return path, format, options


def build_external_files_relation(
    path: str,
    format: str,
    options: dict[str, str],
    schema: StructType | None = None,
) -> sql.Composable:
    arguments = {"path": path, "format": format, **options}
    rendered = sql.SQL(", ").join(
        sql.SQL("{}={}").format(sql.SQL(name), sql.Literal(value))
        for name, value in arguments.items()
    )
    relation = sql.SQL("external_files({})").format(rendered)
    if schema is None:
        return relation
    declarations = sql.SQL(", ").join(
        sql.SQL("{} {}").format(
            sql.Identifier(field.name), sql.SQL(field.datatype.sql_name)
        )
        for field in schema.fields
    )
    return sql.SQL("{} AS ({})").format(relation, declarations)


def build_write_files_sql(
    source_query: sql.Composable,
    path: str,
    format: str,
    options: dict[str, str],
) -> sql.Composable:
    target = build_external_files_relation(path, format, options)
    return sql.SQL("INSERT INTO {} ").format(target) + source_query
