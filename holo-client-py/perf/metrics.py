"""Lightweight metrics: Meter (RPS) and Histogram (latency percentiles).

Pure Python, no external dependencies. Thread-safe.
Mirrors Codahale Metrics used by the Java tool.
"""

from __future__ import annotations

import math
import random
import threading
import time
from dataclasses import dataclass
from typing import List


class _EWMA:
    """Exponentially Weighted Moving Average, matching Codahale EWMA."""

    def __init__(self, alpha: float):
        self._alpha = alpha
        self._rate = 0.0
        self._initialized = False
        self._uncounted = 0

    def update(self, n: int) -> None:
        self._uncounted += n

    def tick(self) -> None:
        count = self._uncounted
        self._uncounted = 0
        instant_rate = count / 5.0  # per-second rate over 5s tick
        if self._initialized:
            self._rate += self._alpha * (instant_rate - self._rate)
        else:
            self._rate = instant_rate
            self._initialized = True

    @property
    def rate(self) -> float:
        return self._rate


class Meter:
    """Tracks event count and rates (1m, 5m, 15m EWMA)."""

    _TICK_INTERVAL = 5.0  # seconds

    def __init__(self):
        self._count = 0
        self._lock = threading.Lock()
        self._start_time = time.monotonic()
        self._last_tick = self._start_time
        self._m1 = _EWMA(1 - math.exp(-5.0 / 60))
        self._m5 = _EWMA(1 - math.exp(-5.0 / 300))
        self._m15 = _EWMA(1 - math.exp(-5.0 / 900))

    def mark(self, n: int = 1) -> None:
        with self._lock:
            self._count += n
            self._m1.update(n)
            self._m5.update(n)
            self._m15.update(n)
            self._tick_if_needed()

    def _tick_if_needed(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_tick
        if elapsed >= self._TICK_INTERVAL:
            ticks = int(elapsed / self._TICK_INTERVAL)
            for _ in range(ticks):
                self._m1.tick()
                self._m5.tick()
                self._m15.tick()
            self._last_tick = now

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    @property
    def mean_rate(self) -> float:
        elapsed = time.monotonic() - self._start_time
        if elapsed <= 0:
            return 0.0
        return self._count / elapsed

    @property
    def m1_rate(self) -> float:
        with self._lock:
            self._tick_if_needed()
            return self._m1.rate

    @property
    def m5_rate(self) -> float:
        with self._lock:
            self._tick_if_needed()
            return self._m5.rate

    @property
    def m15_rate(self) -> float:
        with self._lock:
            self._tick_if_needed()
            return self._m15.rate


@dataclass
class Snapshot:
    """Histogram snapshot with percentiles."""

    count: int
    min: float
    max: float
    mean: float
    p50: float
    p75: float
    p95: float
    p98: float
    p99: float
    p999: float


class Histogram:
    """Reservoir-sampled histogram for latency tracking.

    Uses Vitter's Algorithm R with reservoir size 1028.
    """

    _RESERVOIR_SIZE = 1028

    def __init__(self):
        self._values: List[float] = []
        self._count = 0
        self._lock = threading.Lock()

    def update(self, value: float) -> None:
        with self._lock:
            self._count += 1
            if len(self._values) < self._RESERVOIR_SIZE:
                self._values.append(value)
            else:
                j = random.randint(0, self._count - 1)
                if j < self._RESERVOIR_SIZE:
                    self._values[j] = value

    def snapshot(self) -> Snapshot:
        with self._lock:
            if not self._values:
                return Snapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            sorted_vals = sorted(self._values)
            n = len(sorted_vals)
            total = sum(sorted_vals)
            return Snapshot(
                count=self._count,
                min=sorted_vals[0],
                max=sorted_vals[-1],
                mean=total / n,
                p50=sorted_vals[int(n * 0.50)],
                p75=sorted_vals[int(n * 0.75)],
                p95=sorted_vals[int(n * 0.95)],
                p98=sorted_vals[int(n * 0.98)],
                p99=sorted_vals[min(int(n * 0.99), n - 1)],
                p999=sorted_vals[min(int(n * 0.999), n - 1)],
            )
