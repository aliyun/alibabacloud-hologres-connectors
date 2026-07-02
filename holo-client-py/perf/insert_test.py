"""INSERT mode write test using HoloClient.put() API.

Uses a single HoloClient. Parallelism is controlled by write_parallelism
in the HoloConfig (multi-process workers with shared memory).
"""

from __future__ import annotations

import logging
import random
import time
from typing import Callable

from hologres import HoloClient, Put

from .put_test import PutTest
from .util import get_write_columns

logger = logging.getLogger(__name__)


class InsertTest(PutTest):
    """Write test using HoloClient.put() (batched inserts).

    Parallelism is handled internally by HoloClient via write_parallelism.
    """

    def __init__(self):
        super().__init__()
        self._client: HoloClient | None = None
        self._update_mode = False

    def run(self, conf_name: str, update_mode: bool = False) -> None:
        self._update_mode = update_mode
        super().run(conf_name)

    def init(self) -> None:
        if self._update_mode:
            self.conf.create_table_before_run = False
        self._client = HoloClient(self.holo_config)

    def build_job(self, thread_id: int) -> Callable:
        client = self._client

        def job():
            try:
                schema = client.get_table_schema(self.conf.table_name)
                write_columns = get_write_columns(self.conf, schema)
                rng = random.Random()
                i = 0

                while True:
                    pk = self.tic.increment_and_get()
                    i += 1

                    if i % 1000 == 0 and self.should_stop(i, pk):
                        break

                    put = Put(schema)
                    self.fill_record(put, pk, schema, write_columns, rng)

                    start_ns = time.monotonic_ns()
                    client.put(put)
                    elapsed_ms = (time.monotonic_ns() - start_ns) / 1_000_000
                    self.meter.mark()
                    self.histogram.update(elapsed_ms)

                logger.info("Thread-%d: wrote %d records", thread_id, i)
            except Exception:
                logger.exception("Thread-%d failed", thread_id)

        return job

    def cleanup(self) -> None:
        if self._client is not None:
            try:
                self._client.flush()
                self._client.close()
            except Exception:
                logger.exception("Error closing client")
            self._client = None
