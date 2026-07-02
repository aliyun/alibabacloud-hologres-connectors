"""CSV result reporter matching the Java Reporter format."""

from __future__ import annotations

import logging
import os
import re
import resource
import time
from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from .metrics import Histogram, Meter

logger = logging.getLogger(__name__)


def _get_holo_version(conninfo: str) -> str:
    """Query Hologres version, returning 'major.minor.fix' string."""
    try:
        with psycopg.connect(conninfo, autocommit=True) as conn:
            row = conn.execute("select hg_version()").fetchone()
            if row:
                # "Hologres 5.0.0 (...)" -> "5.0.0"
                m = re.search(r"Hologres\s+(\d+\.\d+\.\d+)", row[0])
                if m:
                    return m.group(1)
    except Exception as e:
        logger.warning("Failed to get Hologres version: %s", e)
    return "unknown"


def _get_memory_usage_kb() -> int:
    """Return current process RSS in KB (like Java's jstat total)."""
    # ru_maxrss is in KB on Linux
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


class Reporter:
    """Collects and writes performance results to CSV."""

    def __init__(self, conf_name: str):
        self.conf_dir = os.path.dirname(os.path.abspath(conf_name))
        self.start_time_ms = 0
        self.version = "unknown"

    def start(self, conninfo: str = None) -> None:
        self.start_time_ms = int(time.time() * 1000)
        if conninfo:
            self.version = _get_holo_version(conninfo)
            logger.info("Hologres version: %s", self.version)

    def report(
        self,
        meter: Meter,
        histogram: Histogram,
        batch_histogram: Histogram = None,
        dump_memory_stat: bool = False,
    ) -> None:
        """Write results to result.csv and log summary."""
        end_time_ms = int(time.time() * 1000)
        elapsed_s = (end_time_ms - self.start_time_ms) / 1000.0
        snap = histogram.snapshot()
        memory_kb = _get_memory_usage_kb() if dump_memory_stat else 0

        # Log summary
        logger.info("=" * 60)
        logger.info("Performance Test Results")
        logger.info("=" * 60)
        logger.info("Duration:    %.1f s", elapsed_s)
        logger.info("Total ops:   %d", meter.count)
        logger.info("Mean QPS:    %.1f", meter.mean_rate)
        logger.info("1m QPS:      %.1f", meter.m1_rate)
        logger.info("5m QPS:      %.1f", meter.m5_rate)
        logger.info("15m QPS:     %.1f", meter.m15_rate)
        logger.info("--- Per-Get Latency (get_perf_latency) ---")
        logger.info("  mean: %.2f ms", snap.mean)
        logger.info("  p50:  %.2f ms", snap.p50)
        logger.info("  p95:  %.2f ms", snap.p95)
        logger.info("  p99:  %.2f ms", snap.p99)
        logger.info("  p999: %.2f ms", snap.p999)
        if batch_histogram is not None:
            bsnap = batch_histogram.snapshot()
            if bsnap.count > 0:
                logger.info("--- Per-Batch Latency (dimlookup_latency) ---")
                logger.info("  count: %d", bsnap.count)
                logger.info("  mean:  %.2f ms", bsnap.mean)
                logger.info("  p50:   %.2f ms", bsnap.p50)
                logger.info("  p95:   %.2f ms", bsnap.p95)
                logger.info("  p99:   %.2f ms", bsnap.p99)
        logger.info("Memory:      %d KB", memory_kb)
        logger.info("Version:     %s", self.version)
        logger.info("=" * 60)

        # Write CSV
        csv_path = os.path.join(self.conf_dir, "result.csv")
        with open(csv_path, "w") as f:
            f.write(
                "start,end,count,qps1,qps5,qps15,"
                "latencyMean,latencyP99,latencyP999,memoryUsage,version\n"
            )
            f.write(
                f"{self.start_time_ms},{end_time_ms},{meter.count},"
                f"{meter.m1_rate:.2f},{meter.m5_rate:.2f},{meter.m15_rate:.2f},"
                f"{snap.mean:.2f},{snap.p99:.2f},{snap.p999:.2f},"
                f"{memory_kb},{self.version}\n"
            )
        logger.info("Results written to %s", csv_path)
