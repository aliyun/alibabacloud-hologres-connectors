"""Private execution settings carried by a lazy DataFrame."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg import sql

from .exceptions import InvalidArgumentError

_MAX_CORES = 102400


@dataclass(frozen=True)
class ServerlessOptions:
    """Validated options for one DataFrame Serverless execution."""

    priority: int | None = None
    required_cores: int | None = None
    max_cores: int | None = None

    @classmethod
    def create(
        cls,
        *,
        priority: int | None,
        required_cores: int | None,
        max_cores: int | None,
    ) -> ServerlessOptions:
        priority = _optional_int("priority", priority, minimum=1, maximum=5)
        required_cores = _optional_int(
            "required_cores", required_cores, minimum=0, maximum=_MAX_CORES
        )
        max_cores = _optional_int("max_cores", max_cores, minimum=0, maximum=_MAX_CORES)
        if (
            required_cores is not None
            and required_cores > 0
            and max_cores is not None
            and max_cores > 0
            and required_cores > max_cores
        ):
            raise InvalidArgumentError(
                "required_cores cannot be greater than max_cores"
            )
        return cls(priority, required_cores, max_cores)

    def set_local_statements(self) -> tuple[sql.Composable, ...]:
        statements: list[sql.Composable] = [
            sql.SQL("SET LOCAL hg_computing_resource = 'serverless'")
        ]
        for name, value in (
            (
                "hg_experimental_serverless_computing_query_priority",
                self.priority,
            ),
            (
                "hg_experimental_serverless_computing_required_cores",
                self.required_cores,
            ),
            (
                "hg_experimental_serverless_computing_max_cores",
                self.max_cores,
            ),
        ):
            if value is not None:
                statements.append(
                    sql.SQL("SET LOCAL {} = {}").format(
                        sql.Identifier(name), sql.Literal(value)
                    )
                )
        return tuple(statements)


def _optional_int(name: str, value, *, minimum: int, maximum: int) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidArgumentError(f"{name} must be an int or None")
    if not minimum <= value <= maximum:
        raise InvalidArgumentError(
            f"{name} must be between {minimum} and {maximum}, got {value}"
        )
    return value
