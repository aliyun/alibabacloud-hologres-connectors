"""SCAN mode prefix scan test.

Executes SELECT * FROM table WHERE id = ? using HoloClient.scan() API.
Each thread uses its own HoloClient instance.
Mirrors Java ScanTest.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import List

import psycopg

from hologres import HoloClient, Scan, SortKeys

from .conf_loader import build_holo_config, load_conf
from .config import ScanTestConf
from .metrics import Histogram, Meter
from .params import ParamsProvider
from .reporter import Reporter
from .sql_util import vacuum_table

logger = logging.getLogger(__name__)


class ScanTest:
    """Prefix scan benchmark using HoloClient.scan() API."""

    def __init__(self):
        self.conf = ScanTestConf()
        self.holo_config = None
        self.target_time = 0.0
        self.meter = Meter()
        self.histogram = Histogram()

    def run(self, conf_name: str) -> None:
        load_conf(conf_name, "scan.", self.conf)
        self.holo_config = build_holo_config(conf_name)

        provider = ParamsProvider(self.conf.key_range_params)

        # Setup
        if self.conf.vacuum_table_before_run:
            with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
                vacuum_table(conn, self.conf.table_name)

        logger.info(
            "SCAN test: %d threads, table=%s",
            self.conf.thread_size,
            self.conf.table_name,
        )

        reporter = Reporter(conf_name)
        reporter.start(self.holo_config.conninfo)
        self.target_time = time.time() + self.conf.test_time / 1000.0

        # Spawn threads
        threads: List[threading.Thread] = []
        for i in range(self.conf.thread_size):
            t = threading.Thread(
                target=self._scan_job,
                args=(i, provider),
                name=f"Thread-{i}",
                daemon=True,
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        logger.info("finished, %d scans executed", self.meter.count)
        reporter.report(
            self.meter, self.histogram, dump_memory_stat=self.conf.dump_memory_stat
        )

        # Cleanup
        if self.conf.delete_table_after_done:
            with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
                from .sql_util import drop_table

                drop_table(conn, self.conf.table_name)

    def _scan_job(self, thread_id: int, provider: ParamsProvider) -> None:
        """Each thread creates its own HoloClient and runs scan queries."""
        try:
            client = HoloClient(self.holo_config)
            schema = client.get_table_schema(self.conf.table_name)
            meter = self.meter
            histogram = self.histogram
            i = 0

            while True:
                i += 1
                if i % 100 == 0 and time.time() > self.target_time:
                    break

                key_value = provider.get(0)

                scan = (
                    Scan.builder(schema)
                    .add_equal_filter("id", key_value)
                    .set_sort_keys(SortKeys.NONE)
                    .build()
                )

                start_ns = time.monotonic_ns()
                client.scan(scan)
                elapsed_ms = (time.monotonic_ns() - start_ns) / 1_000_000

                meter.mark()
                histogram.update(elapsed_ms)

            logger.info("Thread-%d: %d scans", thread_id, i)
            client.close()
        except Exception:
            logger.exception("Thread-%d failed", thread_id)
