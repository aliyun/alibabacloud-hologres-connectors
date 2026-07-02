"""Config file parser for Java-style .conf files.

Supports the same format as the Java holo-e2e-performance-tool:
  - key=value lines
  - # and -- prefixed comment lines
  - prefix-based filtering (e.g. "put.", "get.", "holoClient.")
  - case-insensitive field matching (threadSize -> thread_size)
"""

from __future__ import annotations

import dataclasses
import logging
import urllib.parse
from typing import Any, Dict, Type, TypeVar

from hologres.config import HoloConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _normalize_key(key: str) -> str:
    """Normalize a key for case-insensitive matching.

    Strips underscores and lowercases, so 'thread_size' matches 'threadSize'.
    """
    return key.replace("_", "").lower()


def _parse_properties(file_path: str) -> Dict[str, str]:
    """Parse a Java-style properties file into a dict."""
    props: Dict[str, str] = {}
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("--"):
                continue
            eq = line.find("=")
            if eq < 0:
                continue
            key = line[:eq].strip()
            value = line[eq + 1 :].strip()
            props[key] = value
    return props


def load_conf(file_path: str, prefix: str, conf: T) -> T:
    """Load config values from file into a dataclass instance.

    Keys are matched case-insensitively after stripping the prefix.
    Only keys starting with the given prefix are considered.
    """
    props = _parse_properties(file_path)

    # Aliases: Java config key -> Python field name (for Python keywords etc.)
    _ALIASES = {
        "async": "asyncmode",  # Java "async" -> Python "async_mode"
    }

    # Build field lookup: normalized_name -> (field_name, field_type)
    field_map: Dict[str, tuple[str, Type]] = {}
    for field in dataclasses.fields(conf):
        norm = _normalize_key(field.name)
        field_map[norm] = (field.name, field.type)

    for key, value in props.items():
        if not key.startswith(prefix):
            continue
        field_key = key[len(prefix) :]
        norm = _normalize_key(field_key)
        norm = _ALIASES.get(norm, norm)
        if norm not in field_map:
            logger.debug(
                "Config key %s (normalized: %s) not found in %s",
                key,
                norm,
                type(conf).__name__,
            )
            continue

        field_name, field_type = field_map[norm]
        try:
            coerced = _coerce(value, field_type)
        except (ValueError, TypeError) as e:
            logger.warning(
                "Could not coerce %s=%s to %s: %s", key, value, field_type, e
            )
            continue

        setattr(conf, field_name, coerced)
        logger.info("Config %s=%s", key, value)

    return conf


def _coerce(value: str, field_type: Any) -> Any:
    """Coerce a string value to the target type."""
    # Handle Optional[X] and string type annotations
    type_str = str(field_type).lower()
    if field_type is bool or type_str == "bool":
        return value.lower() == "true"
    if field_type is int or type_str == "int":
        return int(value)
    if field_type is float or type_str == "float":
        return float(value)
    return value


def load_conf_as_dict(file_path: str, prefix: str) -> Dict[str, str]:
    """Load config values with a given prefix into a dict.

    Keys are returned normalized (lowercase, no underscores).
    """
    props = _parse_properties(file_path)
    result: Dict[str, str] = {}
    for key, value in props.items():
        if key.startswith(prefix):
            field_key = key[len(prefix) :]
            result[_normalize_key(field_key)] = value
    return result


def build_holo_config(conf_name: str) -> HoloConfig:
    """Build a HoloConfig from holoClient.* properties in a conf file."""
    props = load_conf_as_dict(conf_name, "holoClient.")

    host = None
    port = 80
    database = None

    # Parse jdbcUrl if present
    jdbc_url = props.get("jdbcurl", "")
    if jdbc_url:
        # jdbc:hologres://host:port/database or jdbc:postgresql://host:port/database
        url = jdbc_url
        for scheme in ("jdbc:hologres://", "jdbc:postgresql://"):
            if url.startswith(scheme):
                url = "http://" + url[len(scheme) :]
                break
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 80
        database = parsed.path.lstrip("/")

    # Direct overrides
    if "host" in props:
        host = props["host"]
    if "port" in props:
        port = int(props["port"])
    if "database" in props:
        database = props["database"]

    username = props.get("username", "")
    password = props.get("password", "")

    config = HoloConfig(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
    )

    # Map additional fields
    if "writebatchsize" in props:
        config.write_batch_size = int(props["writebatchsize"])
    if "writemaxintervalms" in props:
        config.write_max_interval_ms = int(props["writemaxintervalms"])
    if "retrycount" in props:
        config.retry_count = int(props["retrycount"])
    if "readparallelism" in props:
        config.read_parallelism = int(props["readparallelism"])
    elif "readthreadsize" in props:
        config.read_parallelism = int(props["readthreadsize"])
    if "writeparallelism" in props:
        config.write_parallelism = int(props["writeparallelism"])
    elif "writeprocesssize" in props:
        config.write_parallelism = int(props["writeprocesssize"])
    return config
