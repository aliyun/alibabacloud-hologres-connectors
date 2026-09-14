"""Function Compute packaging, deployment, and Hologres registration."""

from __future__ import annotations

import ast
import base64
import inspect
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import types
import zipfile
from pathlib import Path
from typing import Any, Iterator

from psycopg import sql

from . import _expr, _plan
from .column import Column
from .exceptions import ExecutionError, InvalidArgumentError
from .udf import ImportSpec, RemoteFunctionDefinition

__all__ = [
    "DefaultFCDeployer",
    "build_registration_sql",
    "iter_remote_definitions",
    "validate_fc_config",
]

_CONFIG_KEYS = {
    "endpoint",
    "region",
    "account_id",
    "role_arn",
    "api_endpoint",
    "memory_size",
    "timeout",
}


def validate_fc_config(config) -> dict[str, Any] | None:
    """Validate and normalize the Session-level Function Compute config."""
    if config is None:
        return None
    if not isinstance(config, dict):
        raise InvalidArgumentError("fc_config must be a dict or None")
    unknown = sorted(set(config) - _CONFIG_KEYS)
    if unknown:
        raise InvalidArgumentError(f"unknown fc_config options: {', '.join(unknown)}")
    endpoint = _required_string(config, "endpoint").rstrip("/")
    if "://" in endpoint or "/" in endpoint:
        raise InvalidArgumentError(
            "fc_config endpoint must be a hostname without a scheme or path"
        )
    if not endpoint.endswith("-internal.fc.aliyuncs.com"):
        raise InvalidArgumentError(
            "fc_config endpoint must be an internal FC hostname ending with "
            "'-internal.fc.aliyuncs.com'"
        )
    normalized = {
        "endpoint": endpoint,
        "region": _required_string(config, "region"),
        "memory_size": _positive_int(config.get("memory_size", 512), "memory_size"),
        "timeout": _positive_int(config.get("timeout", 60), "timeout"),
    }
    for key in ("account_id", "role_arn", "api_endpoint"):
        value = config.get(key)
        if value is not None:
            if not isinstance(value, str) or not value:
                raise InvalidArgumentError(
                    f"fc_config {key} must be a non-empty string"
                )
            normalized[key] = value
    return normalized


class DefaultFCDeployer:
    """Deploy ZIP packages with the Alibaba Cloud FC 3.0 Python SDK."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self._client = None

    def deploy(self, definition: RemoteFunctionDefinition) -> str:
        """Create or update the FC function and return its invocation path."""
        client, models = self._sdk()
        code = models.InputCodeLocation(
            zip_file=base64.b64encode(build_function_zip(definition)).decode()
        )
        common = {
            "code": code,
            "handler": "handler.handler",
            "runtime": definition.runtime,
            "role": self.config.get("role_arn"),
            "memory_size": self.config["memory_size"],
            "timeout": self.config["timeout"],
        }
        exists = self._function_exists(client, models, definition.fc_function_name)
        if exists:
            if definition.replace:
                request = models.UpdateFunctionRequest(
                    body=models.UpdateFunctionInput(**common)
                )
                client.update_function(definition.fc_function_name, request)
        else:
            request = models.CreateFunctionRequest(
                body=models.CreateFunctionInput(
                    function_name=definition.fc_function_name,
                    **common,
                )
            )
            client.create_function(request)
        return f"{self.config['endpoint']}/{definition.fc_function_name}"

    def _sdk(self):
        if self._client is not None:
            return self._client
        try:
            from alibabacloud_credentials.client import Client as CredentialClient
            from alibabacloud_fc20230330 import models
            from alibabacloud_fc20230330.client import Client
            from alibabacloud_tea_openapi import models as openapi_models
        except ImportError:
            raise ImportError(
                "Function Compute deployment dependencies are missing; reinstall "
                "hologres-dataframe"
            ) from None
        api_endpoint = self.config.get(
            "api_endpoint", f"fcv3.{self.config['region']}.aliyuncs.com"
        )
        sdk_config = openapi_models.Config(
            credential=CredentialClient(),
            region_id=self.config["region"],
            endpoint=api_endpoint,
        )
        self._client = (Client(sdk_config), models)
        return self._client

    @staticmethod
    def _function_exists(client, models, name: str) -> bool:
        try:
            client.get_function(name, models.GetFunctionRequest())
            return True
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise


def build_function_zip(definition: RemoteFunctionDefinition) -> bytes:
    """Build a self-contained FC ZIP package for one definition."""
    try:
        with tempfile.TemporaryDirectory(prefix="hg_dataframe_fc_") as temp:
            root = Path(temp)
            if definition.packages:
                _install_packages(root, definition.packages, definition.runtime)
            for spec in definition.imports:
                _copy_import(root, spec)
            (root / "user_code.py").write_text(
                _render_user_code(definition), encoding="utf-8"
            )
            (root / "handler.py").write_text(
                _render_handler(definition), encoding="utf-8"
            )
            archive = root / "function.zip"
            with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as out:
                for path in sorted(root.rglob("*")):
                    if path.is_file() and path != archive:
                        out.write(path, path.relative_to(root).as_posix())
            return archive.read_bytes()
    except (InvalidArgumentError, ImportError):
        raise
    except Exception as exc:
        raise ExecutionError(
            f"failed to package FC function: {exc}", cause=exc
        ) from exc


def _render_user_code(definition: RemoteFunctionDefinition) -> str:
    obj = definition.python_object
    try:
        source = textwrap.dedent(inspect.getsource(obj))
    except (OSError, TypeError) as exc:
        raise InvalidArgumentError(
            f"cannot read source for {definition.sql_name!r}; define the UDF in "
            "a Python module instead of an interactive expression"
        ) from exc
    tree = ast.parse(source)
    candidates = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    if len(candidates) != 1:
        raise InvalidArgumentError(
            f"cannot isolate source definition for {definition.sql_name!r}"
        )
    node = candidates[0]
    node.decorator_list = []
    if isinstance(node, ast.AsyncFunctionDef):
        raise InvalidArgumentError("async Python functions are not supported by @udf")
    prelude = _render_global_bindings(definition)
    body = ast.unparse(tree)
    return f"{prelude}\n\n{body}\n\nENTRY = {node.name}\n"


def _render_global_bindings(definition: RemoteFunctionDefinition) -> str:
    functions = []
    if inspect.isclass(definition.python_object):
        for value in definition.python_object.__dict__.values():
            if inspect.isfunction(value):
                functions.append(value)
    else:
        functions.append(definition.python_object)
    bindings: dict[str, Any] = {}
    for function in functions:
        try:
            closure = inspect.getclosurevars(function)
        except TypeError:
            continue
        bindings.update(closure.globals)
        bindings.update(closure.nonlocals)
    lines = []
    for name, value in sorted(bindings.items()):
        if isinstance(value, types.ModuleType):
            lines.append(f"import {value.__name__} as {name}")
        elif _is_literal_binding(value):
            lines.append(f"{name} = {value!r}")
        elif callable(value) and getattr(value, "__module__", None):
            module = value.__module__
            object_name = getattr(value, "__name__", None)
            owner_module = definition.python_object.__module__
            if object_name and module not in {
                "builtins",
                "__main__",
                owner_module,
            }:
                lines.append(f"from {module} import {object_name} as {name}")
            else:
                raise InvalidArgumentError(
                    f"cannot package global callable {name!r}; move it into the "
                    "UDF or provide it through imports"
                )
        else:
            raise InvalidArgumentError(
                f"cannot package global {name!r}; move it into the UDF or "
                "provide it through imports"
            )
    return "\n".join(lines)


def _is_literal_binding(value) -> bool:
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return True
    if isinstance(value, (tuple, list, set, frozenset)):
        return all(_is_literal_binding(item) for item in value)
    if isinstance(value, dict):
        return all(
            _is_literal_binding(key) and _is_literal_binding(item)
            for key, item in value.items()
        )
    return False


def _render_handler(definition: RemoteFunctionDefinition) -> str:
    if definition.is_table_function:
        body = """
    instance = ENTRY()
    results = []
    for row_num, row in enumerate(rows):
        for output in instance.process(*row):
            if not isinstance(output, (tuple, list)) or len(output) != 1:
                raise ValueError("UDTF process() must yield one-element tuples")
            results.append([row_num, output[0]])
"""
    else:
        body = """
    results = [[ENTRY(*row)] for row in rows]
"""
    return (
        "import gzip\n"
        "import json\n\n"
        "from user_code import ENTRY\n\n\n"
        "def handler(event, context):\n"
        "    compressed = isinstance(event, bytes)\n"
        "    compressed = compressed and event.startswith(b'\\x1f\\x8b')\n"
        "    if isinstance(event, bytes):\n"
        "        if compressed:\n"
        "            event = gzip.decompress(event)\n"
        "        event = event.decode('utf-8')\n"
        "    payload = json.loads(event) if isinstance(event, str) else event\n"
        "    rows = payload.get('data', [])\n"
        f"{textwrap.indent(textwrap.dedent(body).strip(), '    ')}\n"
        "    response = {'results': results}\n"
        "    if compressed:\n"
        "        return gzip.compress(json.dumps(response).encode('utf-8'))\n"
        "    return response\n"
    )


def _install_packages(root: Path, packages: tuple[str, ...], runtime: str) -> None:
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--no-compile",
        "--target",
        str(root),
    ]
    match = re.fullmatch(r"python(\d)\.(\d+)", runtime)
    if match is not None:
        abi = f"cp{match.group(1)}{match.group(2)}"
        command.extend(
            [
                "--platform",
                "manylinux2014_x86_64",
                "--implementation",
                "cp",
                "--python-version",
                f"{match.group(1)}{match.group(2)}",
                "--abi",
                abi,
                "--only-binary=:all:",
            ]
        )
    command.extend(packages)
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ExecutionError(f"failed to install FC packages: {detail}")


def _copy_import(root: Path, spec: ImportSpec) -> None:
    source = Path(spec.source).expanduser()
    if not source.exists():
        raise InvalidArgumentError(f"FC import source does not exist: {spec.source!r}")
    if "/" in spec.target or (source.suffix and spec.target.endswith(source.suffix)):
        target = Path(spec.target)
    else:
        target = Path(spec.target.replace(".", "/"))
    if target.is_absolute() or ".." in target.parts:
        raise InvalidArgumentError(f"invalid FC import target: {spec.target!r}")
    if source.is_file() and target.suffix == "":
        target = target.with_suffix(source.suffix)
    destination = root / target
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        shutil.copy2(source, destination)


def build_registration_sql(
    definition: RemoteFunctionDefinition, endpoint_path: str
) -> sql.Composable:
    """Build ``CREATE [OR REPLACE] FUNCTION ... LANGUAGE function_compute``."""
    prefix = "CREATE OR REPLACE FUNCTION" if definition.replace else "CREATE FUNCTION"
    function_name = sql.Identifier(*definition.sql_name_parts)
    args = sql.SQL(", ").join(
        sql.SQL("{} {}").format(
            sql.Identifier(f"arg{index}"), sql.SQL(datatype.sql_name)
        )
        for index, datatype in enumerate(definition.input_types)
    )
    if definition.is_table_function:
        returns = sql.SQL("RETURNS TABLE ({} {})").format(
            sql.Identifier(definition.output_field_name),
            sql.SQL(definition.return_type.sql_name),
        )
    else:
        returns = sql.SQL("RETURNS {}").format(sql.SQL(definition.return_type.sql_name))
    parts = [
        sql.SQL(f"{prefix} "),
        function_name,
        sql.SQL(" ("),
        args,
        sql.SQL(") "),
        returns,
        sql.SQL(" LANGUAGE function_compute "),
        sql.SQL("AS {} ").format(sql.Literal(endpoint_path)),
    ]
    if definition.strict:
        parts.append(sql.SQL("STRICT "))
    parts.append(
        sql.SQL("SET function_compute.max_batch_size TO {} ").format(
            sql.Literal(definition.max_batch_size)
        )
    )
    if definition.compression is not None:
        parts.append(
            sql.SQL("SET function_compute.compression TO {} ").format(
                sql.Literal(definition.compression)
            )
        )
    parts.append(
        sql.SQL("SET function_compute.qualifier TO {}").format(
            sql.Literal(definition.version)
        )
    )
    return sql.Composed(parts)


def build_drop_function_sql(
    definition: RemoteFunctionDefinition,
) -> sql.Composable:
    """Build a signature-qualified ``DROP FUNCTION`` statement."""
    args = sql.SQL(", ").join(
        sql.SQL(datatype.sql_name) for datatype in definition.input_types
    )
    return sql.SQL("DROP FUNCTION {} ({})").format(
        sql.Identifier(*definition.sql_name_parts), args
    )


def requires_function_recreate(
    previous: RemoteFunctionDefinition,
    current: RemoteFunctionDefinition,
) -> bool:
    """Return whether PostgreSQL rejects CREATE OR REPLACE for this change."""
    return (
        previous.return_type != current.return_type
        or previous.output_field_name != current.output_field_name
        or previous.is_table_function != current.is_table_function
    )


def iter_remote_definitions(
    plan: _plan.LogicalPlan,
) -> Iterator[RemoteFunctionDefinition]:
    """Yield each distinct remote definition referenced by ``plan``."""
    seen: dict[tuple[str, tuple[str, ...]], RemoteFunctionDefinition] = {}
    for node in _plan.iter_nodes(plan):
        for value in _plan_values(node):
            for definition in _definitions_in_value(value):
                key = (
                    definition.sql_name,
                    tuple(datatype.sql_name for datatype in definition.input_types),
                )
                previous = seen.get(key)
                if previous is None:
                    seen[key] = definition
                    yield definition
                elif previous.fingerprint != definition.fingerprint:
                    raise InvalidArgumentError(
                        f"logical plan references conflicting definitions for remote "
                        f"function {definition.sql_name!r} with the same argument types"
                    )


def _plan_values(node) -> Iterator[Any]:
    for cls in type(node).__mro__:
        slots = getattr(cls, "__slots__", ())
        if isinstance(slots, str):
            slots = (slots,)
        for name in slots:
            if hasattr(node, name):
                value = getattr(node, name)
                if not isinstance(value, _plan.LogicalPlan):
                    yield value


def _definitions_in_value(value) -> Iterator[RemoteFunctionDefinition]:
    if isinstance(value, Column):
        yield from _definitions_in_expression(value._expr)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _definitions_in_value(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _definitions_in_value(item)


def _definitions_in_expression(
    expression: _expr.Expression,
) -> Iterator[RemoteFunctionDefinition]:
    if isinstance(expression, _expr.RemoteFunctionCall):
        yield expression.definition
        for argument in expression.args:
            yield from _definitions_in_expression(argument)
        return
    children = []
    if isinstance(expression, _expr.BinaryExpr):
        children = [expression.left, expression.right]
    elif isinstance(expression, _expr.UnaryExpr):
        children = [expression.operand]
    elif isinstance(expression, _expr.PredicateExpr):
        children = list(expression.operands)
    elif isinstance(expression, (_expr.CastExpr, _expr.SortExpr)):
        children = [expression.child]
    elif isinstance(expression, _expr.FunctionCall):
        children = list(expression.args)
    elif isinstance(expression, _expr.WindowExpr):
        children = list(expression.args) + list(expression.partition_by)
    for child in children:
        yield from _definitions_in_expression(child)


def _required_string(config: dict[str, Any], key: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value:
        raise InvalidArgumentError(f"fc_config {key} must be a non-empty string")
    return value


def _positive_int(value, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise InvalidArgumentError(f"fc_config {name} must be a positive int")
    return value


def _is_not_found(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    code = getattr(exc, "code", None)
    data = getattr(exc, "data", None)
    if isinstance(data, dict):
        status = status or data.get("statusCode") or data.get("status_code")
        code = code or data.get("code")
    return status == 404 or str(code).lower() in {
        "functionnotfound",
        "resourcenotfound.function",
    }
