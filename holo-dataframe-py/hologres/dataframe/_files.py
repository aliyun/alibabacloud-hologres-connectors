"""Read local files into a DataFrame.

``session.read_files`` covers local csv / json / parquet. Hologres itself can
only read paths *it* can see (``oss://``, ``internal_stage://``), so a file on
the client's disk has to be parsed here and uploaded -- which is exactly what
the client-side source machinery in :mod:`hologres.dataframe._sources` already
does for pandas.

The local read happens immediately (that is what makes ``df.schema`` free); the
**upload** stays lazy and runs once, on the first action.
"""

from __future__ import annotations

import glob
import os

from .exceptions import InvalidArgumentError

__all__ = ["read_local_files"]

# Extension -> format, including the compressed and line-delimited spellings
# pyarrow handles natively.
_EXTENSIONS = {
    ".csv": "csv",
    ".tsv": "csv",
    ".txt": "csv",
    ".json": "json",
    ".jsonl": "json",
    ".ndjson": "json",
    ".parquet": "parquet",
    ".pq": "parquet",
}

_COMPRESSION_SUFFIXES = (".gz", ".bz2", ".zst", ".lz4", ".br")

_FORMATS = ("csv", "json", "parquet")

# Reader options we forward, grouped by the pyarrow options object they belong
# to. Anything outside this map is rejected rather than silently dropped.
_CSV_OPTION_GROUPS = {
    "read": ("skip_rows", "skip_rows_after_names", "column_names", "encoding"),
    "parse": ("delimiter", "quote_char", "double_quote", "escape_char"),
    "convert": ("null_values", "true_values", "false_values", "strings_can_be_null"),
}
_JSON_OPTIONS = ("explicit_schema", "unexpected_field_behavior")
_PARQUET_OPTIONS = ("columns",)


def read_local_files(path, fmt=None, **options):
    """Read one or more local files into a single ``pyarrow.Table``.

    Args:
        path: A file path, a glob pattern, or a list of either.
        fmt: ``"csv"``/``"json"``/``"parquet"``, or ``None`` to infer from the
            extension.
        **options: Reader options, forwarded to pyarrow. See the per-format
            allow-lists in this module.

    Returns:
        ``(table, format)`` -- the concatenated data and the format used.

    Raises:
        InvalidArgumentError: If no file matches, the format cannot be inferred
            or is unknown, an option is not recognized, the files disagree on
            their schema, or the data has no rows.
        ImportError: If pyarrow is not installed.
    """
    paths = _expand_paths(path)
    resolved = _resolve_format(fmt, paths)
    reader = _READERS[resolved]
    tables = [reader(p, options) for p in paths]
    table = _concat(tables, paths)
    if table.num_rows == 0:
        raise InvalidArgumentError(
            f"{_describe(paths)} contained no rows; there is nothing to read"
        )
    return table, resolved


def _expand_paths(path) -> list[str]:
    """Turn a path / glob / list into a concrete, sorted list of files."""
    if isinstance(path, str):
        patterns = [path]
    elif isinstance(path, (list, tuple)):
        if not path:
            raise InvalidArgumentError("path is an empty list")
        patterns = list(path)
    else:
        raise InvalidArgumentError(
            f"path must be a string or a list of strings, got {type(path).__name__}"
        )

    for pattern in patterns:
        if not isinstance(pattern, str) or not pattern:
            raise InvalidArgumentError(
                f"each path must be a non-empty string, got {pattern!r}"
            )
    resolved: list[str] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern)) if _is_glob(pattern) else [pattern]
        if not matches:
            raise InvalidArgumentError(f"no file matches {pattern!r}")
        for match in matches:
            if not os.path.isfile(match):
                raise InvalidArgumentError(f"{match!r} is not a readable file")
            resolved.append(match)

    # A glob and an explicit path can name the same file; keep the first.
    seen = set()
    unique = []
    for p in resolved:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique


def _is_glob(pattern: str) -> bool:
    return any(ch in pattern for ch in "*?[")


def _resolve_format(fmt, paths: list[str]) -> str:
    """Validate an explicit format, or infer one from the file extensions."""
    if fmt is not None:
        if fmt not in _FORMATS:
            raise InvalidArgumentError(
                f"unknown format {fmt!r}; expected one of {_FORMATS}"
            )
        return fmt

    inferred = {_format_from_extension(p) for p in paths}
    if None in inferred:
        unknown = next(p for p in paths if _format_from_extension(p) is None)
        raise InvalidArgumentError(
            f"cannot tell the format of {unknown!r} from its extension; "
            f"pass format= explicitly (one of {_FORMATS})"
        )
    if len(inferred) > 1:
        raise InvalidArgumentError(
            f"the files mix formats {tuple(sorted(inferred))}; read them "
            "separately or pass format= to force one"
        )
    return inferred.pop()


def _format_from_extension(path: str) -> str | None:
    """Map a filename to a format, ignoring a trailing compression suffix."""
    stem, ext = os.path.splitext(path)
    if ext.lower() in _COMPRESSION_SUFFIXES:
        ext = os.path.splitext(stem)[1]
    return _EXTENSIONS.get(ext.lower())


def _split_options(options: dict, groups: dict) -> dict:
    """Route options into their pyarrow option groups, rejecting unknown keys."""
    known = {name for group in groups.values() for name in group}
    unknown = set(options) - known
    if unknown:
        raise InvalidArgumentError(
            f"unknown read option(s) {tuple(sorted(unknown))}; supported: "
            f"{tuple(sorted(known))}"
        )
    return {
        group: {k: v for k, v in options.items() if k in names}
        for group, names in groups.items()
    }


def _check_options(options: dict, allowed: tuple) -> dict:
    """Validate a flat option set against an allow-list."""
    unknown = set(options) - set(allowed)
    if unknown:
        raise InvalidArgumentError(
            f"unknown read option(s) {tuple(sorted(unknown))}; supported: "
            f"{tuple(sorted(allowed))}"
        )
    return dict(options)


def _read_csv(path: str, options: dict):
    from pyarrow import csv

    grouped = _split_options(options, _CSV_OPTION_GROUPS)
    return csv.read_csv(
        path,
        read_options=csv.ReadOptions(**grouped["read"]),
        parse_options=csv.ParseOptions(**grouped["parse"]),
        convert_options=csv.ConvertOptions(**grouped["convert"]),
    )


def _read_json(path: str, options: dict):
    from pyarrow import json as pajson

    checked = _check_options(options, _JSON_OPTIONS)
    return pajson.read_json(path, parse_options=pajson.ParseOptions(**checked))


def _read_parquet(path: str, options: dict):
    from pyarrow import parquet

    checked = _check_options(options, _PARQUET_OPTIONS)
    return parquet.read_table(path, **checked)


_READERS = {"csv": _read_csv, "json": _read_json, "parquet": _read_parquet}


def _concat(tables: list, paths: list[str]):
    """Concatenate per-file tables, reporting the first schema disagreement."""
    import pyarrow as pa

    if len(tables) == 1:
        return tables[0]
    first = tables[0]
    for path, table in zip(paths[1:], tables[1:]):
        if table.schema.names != first.schema.names:
            raise InvalidArgumentError(
                f"{path!r} has columns {tuple(table.schema.names)} but "
                f"{paths[0]!r} has {tuple(first.schema.names)}; all files must "
                "share one schema"
            )
    try:
        return pa.concat_tables(tables, promote_options="permissive")
    except Exception as exc:
        raise InvalidArgumentError(
            f"the files under {_describe(paths)} have incompatible column types: {exc}"
        ) from exc


def _describe(paths: list[str]) -> str:
    """A short, readable description of the files read."""
    if len(paths) == 1:
        return repr(paths[0])
    return f"{paths[0]!r} and {len(paths) - 1} more file(s)"
