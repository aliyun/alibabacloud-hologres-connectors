"""Function Compute remote UDF and UDTF definitions.

Decorating a Python function or class is intentionally lazy: it validates and
stores the definition but performs no network I/O. The Session that executes a
DataFrame discovers :class:`RemoteFunctionCall` expressions in the plan and
deploys/registers their definitions immediately before the query action.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import re
import types
import typing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import _expr
from .column import Column
from .exceptions import InvalidArgumentError
from .types import (
    FC_SUPPORTED_TYPES,
    DataType,
    StructType,
    from_python_type,
)

__all__ = ["udf", "udtf"]

_FUNCTION_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)?")
_MISSING = inspect.Signature.empty


@dataclass(frozen=True)
class ImportSpec:
    """One file or directory copied into the FC code package."""

    source: str
    target: str


@dataclass(frozen=True)
class RemoteFunctionDefinition:
    """Validated deployment and SQL-registration metadata."""

    sql_name: str
    python_object: Any
    input_types: tuple[DataType, ...]
    return_type: DataType
    output_field_name: str | None
    packages: tuple[str, ...]
    imports: tuple[ImportSpec, ...]
    runtime: str
    replace: bool
    strict: bool
    max_batch_size: int
    compression: str | None
    version: str
    is_table_function: bool

    @property
    def sql_name_parts(self) -> tuple[str, ...]:
        return tuple(self.sql_name.split("."))

    @property
    def fc_function_name(self) -> str:
        return "_".join(self.sql_name_parts)

    @property
    def output_name(self) -> str:
        if self.is_table_function and self.output_field_name is not None:
            return self.output_field_name
        return self.sql_name_parts[-1]

    @property
    def fingerprint(self) -> str:
        try:
            source = inspect.getsource(self.python_object)
        except (OSError, TypeError):
            source = repr(self.python_object)
        global_bindings = []
        functions = []
        if inspect.isclass(self.python_object):
            functions.extend(
                value
                for value in self.python_object.__dict__.values()
                if inspect.isfunction(value)
            )
        else:
            functions.append(self.python_object)
        for function in functions:
            try:
                closure = inspect.getclosurevars(function)
            except TypeError:
                continue
            for name, value in sorted({**closure.globals, **closure.nonlocals}.items()):
                if isinstance(value, types.ModuleType):
                    marker = (name, "module", value.__name__)
                elif _is_literal_binding(value):
                    marker = (name, "literal", repr(value))
                elif callable(value):
                    marker = (
                        name,
                        "callable",
                        getattr(value, "__module__", None),
                        getattr(value, "__name__", None),
                    )
                else:
                    marker = (name, "object", repr(value))
                global_bindings.append(marker)
        import_files = []
        for spec in self.imports:
            path = Path(spec.source).expanduser()
            if path.is_file():
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
            elif path.is_dir():
                parts = []
                for child in sorted(p for p in path.rglob("*") if p.is_file()):
                    parts.append(
                        (
                            str(child.relative_to(path)),
                            hashlib.sha256(child.read_bytes()).hexdigest(),
                        )
                    )
                digest = repr(parts)
            else:
                digest = "missing"
            import_files.append((spec.source, spec.target, digest))
        payload = repr(
            (
                self.sql_name,
                source,
                tuple(t.sql_name for t in self.input_types),
                self.return_type.sql_name,
                self.output_field_name,
                self.packages,
                self.imports,
                self.runtime,
                self.strict,
                self.max_batch_size,
                self.compression,
                self.version,
                self.is_table_function,
                global_bindings,
                import_files,
            )
        ).encode()
        return hashlib.sha256(payload).hexdigest()


class _RemoteFunction:
    """Callable object returned by ``@udf`` and ``@udtf``."""

    def __init__(self, definition: RemoteFunctionDefinition) -> None:
        self.definition = definition
        functools.update_wrapper(self, definition.python_object, updated=())

    def __call__(self, *args: Any) -> Column:
        expected = len(self.definition.input_types)
        if len(args) != expected:
            raise InvalidArgumentError(
                f"{self.definition.sql_name}() expects {expected} arguments, "
                f"got {len(args)}"
            )
        expressions = [_argument_expression(arg) for arg in args]
        result = Column(_expr.RemoteFunctionCall(self.definition, expressions))
        if self.definition.is_table_function:
            return result.alias(self.definition.output_field_name)
        return result

    def __repr__(self) -> str:
        kind = "UDTF" if self.definition.is_table_function else "UDF"
        return f"{kind}<{self.definition.sql_name}>"


def udf(
    func=None,
    *,
    name: str | None = None,
    input_types=None,
    return_type=None,
    packages=None,
    imports=None,
    runtime: str = "python3.10",
    replace: bool = True,
    strict: bool = True,
    max_batch_size: int = 1024,
    compression: str | None = "gzip",
    version: str = "LATEST",
):
    """Define a lazy Function Compute scalar UDF.

    Types may be supplied explicitly or inferred from Python annotations. The
    decorated object is callable and returns a lazy :class:`Column`.

    Example:
        >>> import hologres.dataframe as hg
        >>> @hg.udf
        ... def grade(n: int) -> str:
        ...     return "high" if n >= 2 else "low"
        >>> grade(hg.col("score"))._to_sql().as_string()
        '"grade"("score")'
    """

    def decorate(python_func):
        if not inspect.isfunction(python_func):
            raise InvalidArgumentError("@udf must decorate a Python function")
        definition = _build_definition(
            python_func,
            name=name,
            input_types=input_types,
            return_type=return_type,
            output_schema=None,
            packages=packages,
            imports=imports,
            runtime=runtime,
            replace=replace,
            strict=strict,
            max_batch_size=max_batch_size,
            compression=compression,
            version=version,
            is_table_function=False,
        )
        return _RemoteFunction(definition)

    if func is None:
        return decorate
    return decorate(func)


def udtf(
    cls=None,
    *,
    output_schema,
    name: str | None = None,
    input_types=None,
    packages=None,
    imports=None,
    runtime: str = "python3.10",
    replace: bool = True,
    strict: bool = True,
    max_batch_size: int = 1024,
    compression: str | None = "gzip",
    version: str = "LATEST",
):
    """Define a lazy, single-output-column Function Compute UDTF.

    Hologres Remote UDX cannot return PostgreSQL ``record``, so
    ``output_schema`` must contain exactly one field.

    Example:
        >>> import hologres.dataframe as hg
        >>> @hg.udtf(output_schema=hg.StructType([
        ...     hg.StructField("word", hg.StringType())
        ... ]))
        ... class SplitWords:
        ...     def process(self, text: str):
        ...         for word in text.split():
        ...             yield (word,)
        >>> SplitWords("text")._to_sql().as_string()
        '"SplitWords"("text")'
    """

    def decorate(python_class):
        if not inspect.isclass(python_class):
            raise InvalidArgumentError("@udtf must decorate a Python class")
        process = getattr(python_class, "process", None)
        if not inspect.isfunction(process):
            raise InvalidArgumentError("@udtf class must define process(self, ...)")
        definition = _build_definition(
            python_class,
            name=name,
            input_types=input_types,
            return_type=None,
            output_schema=output_schema,
            packages=packages,
            imports=imports,
            runtime=runtime,
            replace=replace,
            strict=strict,
            max_batch_size=max_batch_size,
            compression=compression,
            version=version,
            is_table_function=True,
        )
        return _RemoteFunction(definition)

    if cls is None:
        return decorate
    return decorate(cls)


def _build_definition(
    python_object,
    *,
    name,
    input_types,
    return_type,
    output_schema,
    packages,
    imports,
    runtime,
    replace,
    strict,
    max_batch_size,
    compression,
    version,
    is_table_function,
) -> RemoteFunctionDefinition:
    sql_name = _validate_name(python_object.__name__ if name is None else name)
    callable_for_signature = (
        python_object.process if is_table_function else python_object
    )
    signature = inspect.signature(callable_for_signature)
    try:
        type_hints = typing.get_type_hints(callable_for_signature)
    except (NameError, TypeError):
        type_hints = {}
    parameters = list(signature.parameters.values())
    if is_table_function:
        if not parameters or parameters[0].name not in {"self", "cls"}:
            raise InvalidArgumentError("UDTF process() must start with self")
        parameters = parameters[1:]
    _validate_parameters(parameters)
    normalized_inputs = _resolve_input_types(parameters, input_types, type_hints)

    output_field_name = None
    if is_table_function:
        if not isinstance(output_schema, StructType):
            raise InvalidArgumentError("udtf output_schema must be a StructType")
        if len(output_schema.fields) != 1:
            raise InvalidArgumentError(
                "Hologres remote UDTF output_schema must contain exactly one field"
            )
        output_field = output_schema.fields[0]
        normalized_return = _validate_fc_type(
            output_field.datatype, "output_schema field"
        )
        output_field_name = output_field.name
    else:
        normalized_return = _resolve_return_type(signature, return_type, type_hints)

    return RemoteFunctionDefinition(
        sql_name=sql_name,
        python_object=python_object,
        input_types=normalized_inputs,
        return_type=normalized_return,
        output_field_name=output_field_name,
        packages=_normalize_packages(packages),
        imports=_normalize_imports(imports),
        runtime=_normalize_runtime(runtime),
        replace=_bool_value("replace", replace),
        strict=_bool_value("strict", strict),
        max_batch_size=_positive_int("max_batch_size", max_batch_size),
        compression=_normalize_compression(compression),
        version=_non_empty_string("version", version),
        is_table_function=is_table_function,
    )


def _validate_name(name) -> str:
    if not isinstance(name, str) or not _FUNCTION_NAME_RE.fullmatch(name):
        raise InvalidArgumentError(
            "UDF name must be a SQL identifier, optionally schema-qualified"
        )
    return name


def _validate_parameters(parameters) -> None:
    for parameter in parameters:
        if parameter.kind not in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }:
            raise InvalidArgumentError(
                "UDF parameters must be positional; *args, **kwargs, and "
                "keyword-only parameters are not supported"
            )


def _resolve_input_types(parameters, explicit, type_hints) -> tuple[DataType, ...]:
    if explicit is None:
        resolved = []
        for parameter in parameters:
            annotation = type_hints.get(parameter.name, parameter.annotation)
            if annotation is _MISSING:
                raise InvalidArgumentError(
                    f"parameter {parameter.name!r} needs a type annotation or "
                    "an explicit input_types entry"
                )
            resolved.append(
                _validate_fc_type(
                    from_python_type(annotation),
                    f"parameter {parameter.name!r}",
                )
            )
        return tuple(resolved)
    if not isinstance(explicit, (list, tuple)):
        raise InvalidArgumentError("input_types must be a list or tuple")
    if len(explicit) != len(parameters):
        raise InvalidArgumentError(
            f"input_types has {len(explicit)} entries but the function has "
            f"{len(parameters)} parameters"
        )
    return tuple(
        _validate_fc_type(_as_datatype(value), f"input_types[{index}]")
        for index, value in enumerate(explicit)
    )


def _resolve_return_type(signature, explicit, type_hints) -> DataType:
    if explicit is None:
        annotation = type_hints.get("return", signature.return_annotation)
        if annotation is _MISSING:
            raise InvalidArgumentError(
                "UDF needs a return annotation or an explicit return_type"
            )
        explicit = annotation
    return _validate_fc_type(_as_datatype(explicit), "return_type")


def _as_datatype(value) -> DataType:
    return value if isinstance(value, DataType) else from_python_type(value)


def _validate_fc_type(datatype: DataType, label: str) -> DataType:
    if datatype not in FC_SUPPORTED_TYPES:
        raise InvalidArgumentError(
            f"{label} type {datatype.sql_name!r} is not supported by "
            "Function Compute remote UDFs"
        )
    return datatype


def _normalize_packages(packages) -> tuple[str, ...]:
    if packages is None:
        return ()
    if not isinstance(packages, (list, tuple)):
        raise InvalidArgumentError("packages must be a list or tuple")
    normalized = tuple(_non_empty_string("package", value) for value in packages)
    if len(set(normalized)) != len(normalized):
        raise InvalidArgumentError("packages must not contain duplicates")
    return normalized


def _normalize_imports(imports) -> tuple[ImportSpec, ...]:
    if imports is None:
        return ()
    if not isinstance(imports, (list, tuple)):
        raise InvalidArgumentError("imports must be a list or tuple")
    normalized = []
    for index, value in enumerate(imports):
        if isinstance(value, (str, Path)):
            source = _import_string(f"imports[{index}]", value)
            target = Path(source).name
        elif isinstance(value, (list, tuple)) and len(value) == 2:
            source = _import_string(f"imports[{index}].source", value[0])
            target = _import_string(f"imports[{index}].target", value[1])
        else:
            raise InvalidArgumentError(
                "each imports entry must be a path or a (source, target) pair"
            )
        normalized.append(ImportSpec(source, target))
    return tuple(normalized)


def _import_string(name: str, value) -> str:
    if not isinstance(value, (str, Path)) or not str(value):
        raise InvalidArgumentError(f"{name} must be a non-empty path string")
    return str(value)


def _normalize_compression(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value.upper() != "GZIP":
        raise InvalidArgumentError("compression must be None or 'gzip'")
    return "GZIP"


def _normalize_runtime(value) -> str:
    value = _non_empty_string("runtime", value)
    if value != "python3" and not re.fullmatch(r"python3\.\d+", value):
        raise InvalidArgumentError(
            "runtime must be a Python Function Compute runtime, for example "
            "'python3.10'"
        )
    return value


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


def _non_empty_string(name: str, value) -> str:
    if not isinstance(value, str) or not value:
        raise InvalidArgumentError(f"{name} must be a non-empty string")
    return value


def _bool_value(name: str, value) -> bool:
    if not isinstance(value, bool):
        raise InvalidArgumentError(f"{name} must be a bool")
    return value


def _positive_int(name: str, value) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise InvalidArgumentError(f"{name} must be a positive int")
    return value


def _argument_expression(value) -> _expr.Expression:
    if isinstance(value, Column):
        return value._expr
    if isinstance(value, str):
        return _expr.ColumnRef(value)
    return _expr.Literal(value)
