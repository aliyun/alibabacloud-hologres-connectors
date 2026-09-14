"""Internal feature requirements for version-sensitive Hologres behavior."""

from __future__ import annotations

from typing import Any, Iterator

from . import _expr, _plan
from .column import Column
from .types import ArrayType, StructType, VectorType

EXTERNAL_FILES = "external_files"
DYNAMIC_TABLE = "dynamic_table"
OBJECT_TABLE = "object_table"
FUNCTION_COMPUTE = "function_compute"
VECTOR_TYPE = "vector_type"
BINLOG_GAP_GUC = "binlog_gap_guc"

MIN_VERSIONS = {
    EXTERNAL_FILES: (4, 1, 0),
    DYNAMIC_TABLE: (4, 0, 13),
    OBJECT_TABLE: (4, 0, 0),
}


def iter_plan_requirements(plan: _plan.LogicalPlan) -> Iterator[tuple[str, str]]:
    """Yield distinct ``(kind, name)`` requirements referenced by ``plan``."""
    seen = set()
    for node in _plan.iter_nodes(plan):
        if isinstance(node, _plan.PendingSource):
            pending_requirements = (
                node.requirements()
                if callable(node.requirements)
                else node.requirements
            )
            for requirement in pending_requirements:
                if requirement not in seen:
                    seen.add(requirement)
                    yield requirement
            if node.resolved is not None:
                for requirement in iter_plan_requirements(node.resolved):
                    if requirement not in seen:
                        seen.add(requirement)
                        yield requirement
        if isinstance(node, (_plan.ExternalFilesScan, _plan.StageScan)):
            requirement = ("capability", EXTERNAL_FILES)
            if requirement not in seen:
                seen.add(requirement)
                yield requirement
        for datatype in _node_datatypes(node):
            if _contains_vector(datatype):
                requirement = ("capability", VECTOR_TYPE)
                if requirement not in seen:
                    seen.add(requirement)
                    yield requirement
        for value in _plan_values(node):
            for requirement in _requirements_in_value(value):
                if requirement not in seen:
                    seen.add(requirement)
                    yield requirement


def _node_datatypes(node) -> Iterator[Any]:
    if isinstance(node, _plan.InlineValues):
        yield from node.types
    elif isinstance(
        node, (_plan.ExternalFilesScan, _plan.PendingSource, _plan.StageScan)
    ):
        if node.schema is not None:
            yield node.schema


def _contains_vector(datatype) -> bool:
    if isinstance(datatype, VectorType):
        return True
    if isinstance(datatype, ArrayType):
        return _contains_vector(datatype.element_type)
    if isinstance(datatype, StructType):
        return any(_contains_vector(field.datatype) for field in datatype.fields)
    return False


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


def _requirements_in_value(value) -> Iterator[tuple[str, str]]:
    if isinstance(value, Column):
        yield from _requirements_in_expression(value._expr)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _requirements_in_value(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _requirements_in_value(item)


def _requirements_in_expression(
    expression: _expr.Expression,
) -> Iterator[tuple[str, str]]:
    if isinstance(expression, _expr.CastExpr) and expression.type_name.startswith(
        "vector"
    ):
        yield "capability", VECTOR_TYPE
    if isinstance(expression, _expr.FunctionCall):
        if expression.required_capability is not None:
            yield "sql_function", expression.required_capability

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
        yield from _requirements_in_expression(child)
