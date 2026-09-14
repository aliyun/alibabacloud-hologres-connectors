"""Validation, SQL generation, and execution for Hologres Dynamic Tables."""

from __future__ import annotations

import math
import re
from typing import Any

from hologres.table_name import TableName
from psycopg import sql

from .exceptions import InvalidArgumentError

__all__ = ["as_dynamic_table"]

_REFRESH_MODES = ("auto", "full", "incremental")
_EXISTENCE_MODES = ("ignore", "errorifexists", "replace")
_OPTION_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_OPTION_ALIASES = {"cdc_format": "base_table_cdc_format"}
_RESERVED_OPTIONS = {"freshness", "auto_refresh_mode", "auto_refresh_enable"}


def as_dynamic_table(
    dataframe,
    name: str,
    *,
    freshness: str,
    refresh_mode: str = "auto",
    auto_refresh: bool = True,
    mode: str = "ignore",
    options: dict[str, Any] | None = None,
) -> None:
    """Create a Dynamic Table from ``dataframe`` and execute the DDL now."""
    session = dataframe._session
    if session is None:
        raise InvalidArgumentError(
            "this DataFrame has no session; obtain it from "
            "session.table() or session.sql() first"
        )

    table_name = _parse_table_name(name)
    normalized_options = _validate_options(
        freshness,
        refresh_mode,
        auto_refresh,
        mode,
        options,
    )
    require_capability = getattr(session, "_require_capability", None)
    if require_capability is not None:
        require_capability("dynamic_table", operation="as_dynamic_table")

    # Compile before a replace transaction can drop the old object. This also
    # materializes a PendingSource first, so a failed upload cannot remove it.
    source_query = dataframe._prepare_sql()
    create = _build_create_sql(
        table_name,
        source_query,
        normalized_options,
        if_not_exists=mode == "ignore",
    )
    if mode == "replace":
        drop = sql.SQL("DROP TABLE IF EXISTS {}").format(table_name.full_name)
        session._execute_write_transaction([drop, create])
    else:
        session.execute(create, fetch=False)
    return None


def _parse_table_name(name: str) -> TableName:
    if not isinstance(name, str) or not name:
        raise InvalidArgumentError("name must be a non-empty table name")
    try:
        return TableName.valueOf(name)
    except ValueError as exc:
        raise InvalidArgumentError(f"invalid table name {name!r}: {exc}") from exc


def _validate_options(freshness, refresh_mode, auto_refresh, mode, options):
    if not isinstance(freshness, str) or not freshness.strip():
        raise InvalidArgumentError("freshness must be a non-empty string")
    if refresh_mode not in _REFRESH_MODES:
        raise InvalidArgumentError(
            f"refresh_mode must be one of {_REFRESH_MODES}, got {refresh_mode!r}"
        )
    if not isinstance(auto_refresh, bool):
        raise InvalidArgumentError("auto_refresh must be a bool")
    if mode not in _EXISTENCE_MODES:
        raise InvalidArgumentError(
            f"mode must be one of {_EXISTENCE_MODES}, got {mode!r}"
        )
    if options is None:
        options = {}
    if not isinstance(options, dict):
        raise InvalidArgumentError("options must be a dict or None")

    normalized = {
        "freshness": freshness.strip(),
        "auto_refresh_mode": refresh_mode,
        "auto_refresh_enable": "true" if auto_refresh else "false",
    }
    for original_name, value in options.items():
        if not isinstance(original_name, str) or not _OPTION_NAME_RE.fullmatch(
            original_name
        ):
            raise InvalidArgumentError(
                "dynamic table option names must be lowercase SQL identifiers"
            )
        option_name = _OPTION_ALIASES.get(original_name, original_name)
        if option_name in _RESERVED_OPTIONS:
            raise InvalidArgumentError(
                f"option {original_name!r} is controlled by a named argument"
            )
        if option_name in normalized:
            raise InvalidArgumentError(
                f"dynamic table option {option_name!r} was specified more than once"
            )
        normalized[option_name] = _normalize_option_value(original_name, value)
    return normalized


def _normalize_option_value(name: str, value: Any):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        if not value:
            raise InvalidArgumentError(f"dynamic table option {name!r} cannot be empty")
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise InvalidArgumentError(
        f"dynamic table option {name!r} must be a string, number, or bool"
    )


def _build_create_sql(
    table_name: TableName,
    source_query: sql.Composable,
    options: dict[str, Any],
    *,
    if_not_exists: bool,
) -> sql.Composable:
    rendered_options = sql.SQL(", ").join(
        sql.SQL("{} = {}").format(sql.SQL(name), sql.Literal(value))
        for name, value in options.items()
    )
    prefix = (
        sql.SQL("CREATE DYNAMIC TABLE IF NOT EXISTS ")
        if if_not_exists
        else sql.SQL("CREATE DYNAMIC TABLE ")
    )
    return sql.Composed(
        [
            prefix,
            table_name.full_name,
            sql.SQL(" WITH ("),
            rendered_options,
            sql.SQL(") AS "),
            source_query,
        ]
    )
