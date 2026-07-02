from __future__ import annotations

import re
import threading
from typing import Optional
from psycopg import sql


_IDENTIFIER_PATTERN = re.compile(r'^[^"\s\d\-;][^"\s\-;]*$')

_cache: dict[str, TableName] = {}
_cache_lock = threading.Lock()


class TableName:
    """Immutable, schema-qualified table name with identifier quoting.

    Caches instances for identity-based deduplication.
    """

    __slots__ = ("schema_name", "table_name", "full_name")

    DEFAULT_SCHEMA = "public"
    QUOTE = '"'

    def __init__(self, schema_name: str, table_name: str):
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_name = sql.Identifier(schema_name, table_name)

    @classmethod
    def valueOf(cls, name: str) -> TableName:
        """Parse a dotted identifier into a TableName.

        Supports:
          - ``public.my_table``
          - ``"Public"."My Table"``
          - ``my_table`` (defaults to public schema)
        """
        with _cache_lock:
            cached = _cache.get(name)
            if cached is not None:
                return cached

        parts = cls._parse_multi_identifier(name)
        if len(parts) == 1:
            schema = cls.DEFAULT_SCHEMA
            table = parts[0]
        elif len(parts) == 2:
            schema = parts[0]
            table = parts[1]
        else:
            raise ValueError(f"Invalid table identifier: {name!r}")

        obj = cls(schema, table)
        with _cache_lock:
            existing = _cache.get(name)
            if existing is not None:
                return existing
            _cache[name] = obj
        return obj

    @classmethod
    def _parse_multi_identifier(cls, identifier: str) -> list[str]:
        """Split a dotted identifier into its component parts."""
        parts: list[str] = []
        i = 0
        n = len(identifier)
        while i < n:
            if identifier[i] == '"':
                # Quoted identifier
                i += 1
                buf: list[str] = []
                while i < n:
                    if identifier[i] == '"':
                        if i + 1 < n and identifier[i + 1] == '"':
                            buf.append('"')
                            i += 2
                        else:
                            i += 1
                            break
                    else:
                        buf.append(identifier[i])
                        i += 1
                parts.append("".join(buf))
                # Skip dot separator
                if i < n and identifier[i] == ".":
                    i += 1
            else:
                # Unquoted identifier — read until dot
                j = i
                while j < n and identifier[j] != ".":
                    j += 1
                raw = identifier[i:j]
                if not raw or not _IDENTIFIER_PATTERN.match(raw):
                    raise ValueError(f"Invalid identifier: {raw!r} in {identifier!r}")
                parts.append(raw.lower())
                i = j + 1 if j < n else j
        return parts

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TableName):
            return NotImplemented
        return (
            self.schema_name == other.schema_name
            and self.table_name == other.table_name
        )

    def __hash__(self) -> int:
        return hash((self.schema_name, self.table_name))

    def __repr__(self) -> str:
        return self.full_name.as_string()

    def __str__(self) -> str:
        return self.full_name.as_string()
