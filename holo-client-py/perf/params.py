"""Random parameter providers for key generation.

Mirrors Java ParamsProvider / IntRandomParamProvider / LongRandomParamProvider.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import Any, List


class ParamProvider(ABC):
    """Base class for random parameter providers."""

    @abstractmethod
    def next(self) -> Any: ...


class IntRandomParamProvider(ParamProvider):
    """Random int in [min, max). Mirrors Java IntRandomParamProvider."""

    def __init__(self, pattern: str):
        parts = pattern.split("-")
        self.min_val = int(parts[0])
        self.max_val = int(parts[1])

    def next(self) -> int:
        return random.randint(self.min_val, self.max_val - 1)


class LongRandomParamProvider(ParamProvider):
    """Random long in [min, max). Mirrors Java LongRandomParamProvider."""

    def __init__(self, pattern: str):
        parts = pattern.split("-")
        self.min_val = int(parts[0])
        self.max_val = int(parts[1])

    def next(self) -> int:
        return random.randint(self.min_val, self.max_val - 1)


class ParamsProvider:
    """Composite provider for multi-column primary keys.

    Pattern format: "I<min>-<max>,L<min>-<max>,..."
    I = int, L = long. Each element maps to one PK column.
    """

    def __init__(self, pattern: str):
        self.providers: List[ParamProvider] = []
        for part in pattern.split(","):
            part = part.strip()
            if not part:
                continue
            type_char = part[0]
            range_str = part[1:]
            if type_char == "I":
                self.providers.append(IntRandomParamProvider(range_str))
            elif type_char == "L":
                self.providers.append(LongRandomParamProvider(range_str))
            else:
                raise ValueError(f"Unknown param type: {type_char!r}")

    def get(self, index: int) -> Any:
        return self.providers[index].next()

    def size(self) -> int:
        return len(self.providers)
