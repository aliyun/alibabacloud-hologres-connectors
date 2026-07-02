"""GET mode point query test.

Mirrors Java GetTest. Supports three modes (set via get.mode in conf):
  - sync:               each thread calls client.get() which blocks on future.result()
  - async-with-future:  each thread submits gets via client.async_get() (non-blocking future)
  - async-with-coroutine: uses AsyncHoloClient.get() with asyncio coroutines
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import List

import psycopg

from hologres import AsyncHoloClient, HoloClient, Get

from .conf_loader import build_holo_config, load_conf
from .config import GetTestConf
from .metrics import Histogram, Meter
from .params import ParamsProvider
from .reporter import Reporter
from .sql_util import vacuum_table

logger = logging.getLogger(__name__)


class GetTest:
    """Point query benchmark using HoloClient.get() / AsyncHoloClient.get()."""

    def __init__(self):
        self.conf = GetTestConf()
        self.holo_config = None
        self.target_time = 0.0
        self.meter = Meter()
        self.histogram = Histogram()  # get_perf_latency (per individual get)
        self.batch_histogram = Histogram()  # dimlookup_latency (per batch execution)
        # Shared client for sync/async-future modes
        self._shared_client: HoloClient = None
        self._shared_schema = None
        self._shared_pk_names: List[str] = []

    def run(self, conf_name: str) -> None:
        load_conf(conf_name, "get.", self.conf)
        self.holo_config = build_holo_config(conf_name)

        provider = ParamsProvider(self.conf.key_range_params)

        # Setup
        if self.conf.vacuum_table_before_run:
            with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
                vacuum_table(conn, self.conf.table_name)

        # Determine mode
        mode = self.conf.mode.lower()
        if mode == "async-with-coroutine":
            mode_str = "async-with-coroutine (batch_size=%d)" % self.conf.batch_size
        elif mode == "async-with-future":
            mode_str = "async-with-future (batch_size=%d)" % self.conf.batch_size
        else:
            mode_str = "sync (batch_size=%d)" % self.conf.batch_size
        logger.info("GET test: %d threads, %s mode", self.conf.thread_size, mode_str)

        reporter = Reporter(conf_name)
        reporter.start(self.holo_config.conninfo)
        self.target_time = time.time() + self.conf.test_time / 1000.0

        # Dispatch by mode
        if mode == "async-with-coroutine":
            self._run_await_mode(provider)
        else:
            # sync or async-with-future both use HoloClient
            self.holo_config.read_batch_size = self.conf.batch_size
            if self.conf.queue_size > 0:
                self.holo_config.read_batch_queue_size = self.conf.queue_size
            else:
                self.holo_config.read_batch_queue_size = (
                    self.conf.batch_size * self.conf.thread_size * 2
                )
            self._shared_client = HoloClient(self.holo_config)
            self._shared_client._batch_callback = lambda ms: (
                self.batch_histogram.update(ms)
            )
            self._shared_schema = self._shared_client.get_table_schema(
                self.conf.table_name
            )
            self._shared_pk_names = list(self._shared_schema.primary_keys)

            # Spawn threads
            threads: List[threading.Thread] = []
            for i in range(self.conf.thread_size):
                if mode == "async-with-future":
                    target = self._async_future_job
                else:
                    target = self._sync_job
                t = threading.Thread(
                    target=target,
                    args=(i, provider),
                    name=f"Thread-{i}",
                    daemon=True,
                )
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

        logger.info("finished, %d queries executed", self.meter.count)

        # Report
        reporter.report(
            self.meter,
            self.histogram,
            self.batch_histogram,
            dump_memory_stat=self.conf.dump_memory_stat,
        )

        # Cleanup
        if self._shared_client is not None:
            self._shared_client.close()
            self._shared_client = None

        if self.conf.delete_table_after_done:
            with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
                from .sql_util import drop_table

                drop_table(conn, self.conf.table_name)

    def _sync_job(self, thread_id: int, provider: ParamsProvider) -> None:
        """Sync mode: client.get() blocks on future.result() internally."""
        try:
            schema = self._shared_schema
            pk_names = self._shared_pk_names
            self._validate_pk_count(provider, pk_names)

            pk_indices = schema.pk_index
            meter = self.meter
            histogram = self.histogram
            client = self._shared_client
            i = 0

            while True:
                i += 1
                if i % 1000 == 0 and time.time() > self.target_time:
                    break

                get = Get(schema)
                for j, pk_idx in enumerate(pk_indices):
                    get.set_primary_key_fast(pk_idx, provider.get(j))

                start_ns = time.monotonic_ns()
                client.get(get)  # blocks until result
                elapsed_ms = (time.monotonic_ns() - start_ns) / 1_000_000

                meter.mark()
                histogram.update(elapsed_ms)

            logger.info("Thread-%d: %d queries", thread_id, i)
        except Exception:
            logger.exception("Thread-%d failed", thread_id)

    def _async_future_job(self, thread_id: int, provider: ParamsProvider) -> None:
        """Async-future mode: client.async_get() returns Future without blocking.

        Submit one get at a time without blocking.
        The reader batches them internally. Per-get latency is recorded
        via a callback when the future completes.
        """
        try:
            schema = self._shared_schema
            pk_names = self._shared_pk_names
            self._validate_pk_count(provider, pk_names)

            pk_indices = schema.pk_index
            meter = self.meter
            histogram = self.histogram
            i = 0
            last_future = None

            while True:
                i += 1
                if i % 1000 == 0 and time.time() > self.target_time:
                    break

                get = Get(schema)
                for j, pk_idx in enumerate(pk_indices):
                    get.set_primary_key_fast(pk_idx, provider.get(j))

                future = self._shared_client.async_get(get)
                submit_ns = get.submit_ns

                def _on_done(f, sns=submit_ns):
                    elapsed_ms = (time.monotonic_ns() - sns) / 1_000_000
                    meter.mark()
                    histogram.update(elapsed_ms)

                future.add_done_callback(_on_done)
                last_future = future

            # Wait for the last future to ensure all are done
            if last_future is not None:
                last_future.result()

            logger.info("Thread-%d: %d queries", thread_id, i)
        except Exception:
            logger.exception("Thread-%d failed", thread_id)

    def _run_await_mode(self, provider: ParamsProvider) -> None:
        """Await mode: uses AsyncHoloClient with asyncio coroutines."""
        self.holo_config.read_batch_size = self.conf.batch_size
        if self.conf.queue_size > 0:
            self.holo_config.read_batch_queue_size = self.conf.queue_size
        else:
            self.holo_config.read_batch_queue_size = (
                self.conf.batch_size * self.conf.thread_size * 2
            )

        asyncio.run(self._await_main(provider))

    async def _await_main(self, provider: ParamsProvider) -> None:
        """Main async entry: spawn N coroutines using AsyncHoloClient."""
        async with AsyncHoloClient(self.holo_config) as client:
            schema = await client.get_table_schema(self.conf.table_name)
            pk_names = list(schema.primary_keys)
            self._validate_pk_count(provider, pk_names)

            tasks = []
            for i in range(self.conf.thread_size):
                tasks.append(
                    asyncio.create_task(self._await_worker(client, schema, provider, i))
                )

            await asyncio.gather(*tasks)

    async def _await_worker(
        self, client: AsyncHoloClient, schema, provider: ParamsProvider, worker_id: int
    ) -> None:
        """Single async worker coroutine — batch submit via create_task."""
        try:
            pk_indices = schema.pk_index
            meter = self.meter
            histogram = self.histogram
            batch_size = self.conf.batch_size
            i = 0

            while True:
                if time.time() > self.target_time:
                    break

                # Submit a batch of gets as concurrent tasks
                tasks = []
                submit_times = []
                for _ in range(batch_size):
                    i += 1
                    get = Get(schema)
                    for j, pk_idx in enumerate(pk_indices):
                        get.set_primary_key_fast(pk_idx, provider.get(j))
                    submit_times.append(time.monotonic_ns())
                    tasks.append(asyncio.create_task(client.get(get)))

                # Wait for all in the batch
                await asyncio.gather(*tasks)

                now_ns = time.monotonic_ns()
                for sns in submit_times:
                    elapsed_ms = (now_ns - sns) / 1_000_000
                    meter.mark()
                    histogram.update(elapsed_ms)

            logger.info("Worker-%d: %d queries", worker_id, i)
        except Exception:
            logger.exception("Worker-%d failed", worker_id)

    @staticmethod
    def _validate_pk_count(provider: ParamsProvider, pk_names: List[str]) -> None:
        if provider.size() != len(pk_names):
            raise ValueError(
                f"keyRangeParams has {provider.size()} columns but "
                f"table has {len(pk_names)} PK columns"
            )
