"""ASYNC_INSERT mode write test using AsyncHoloClient.put() API.

Uses a single AsyncHoloClient. Parallelism is controlled by write_parallelism
in the HoloConfig (multiple async writer tasks).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time

from hologres import AsyncHoloClient, Put

from .conf_loader import build_holo_config, load_conf
from .config import PutTestConf
from .metrics import Histogram, Meter
from .reporter import Reporter
from .sql_util import create_table, drop_table, vacuum_table
from .util import AtomicLong, get_write_columns

import psycopg

logger = logging.getLogger(__name__)


class AsyncInsertTest:
    """Write test using AsyncHoloClient.put() (batched inserts).

    Parallelism is handled internally by AsyncHoloClient via write_parallelism.
    """

    def __init__(self):
        self.conf = PutTestConf()
        self.holo_config = None
        self.meter = Meter()
        self.histogram = Histogram()
        self.tic = AtomicLong(0)

    def run(self, conf_name: str, update_mode: bool = False) -> None:
        load_conf(conf_name, "put.", self.conf)
        if update_mode:
            self.conf.create_table_before_run = False
        self.holo_config = build_holo_config(conf_name)

        # Setup table
        with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
            if self.conf.create_table_before_run:
                create_table(conn, self.conf)
            if self.conf.vacuum_table_before_run:
                vacuum_table(conn, self.conf.table_name)

        reporter = Reporter(conf_name)
        reporter.start(self.holo_config.conninfo)

        # Run async loop
        asyncio.run(self._run_async())

        logger.info("finished, %d rows written", self.meter.count)
        reporter.report(
            self.meter, self.histogram, dump_memory_stat=self.conf.dump_memory_stat
        )

        # Cleanup table
        if self.conf.delete_table_after_done:
            with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
                drop_table(conn, self.conf.table_name)

    async def _run_async(self) -> None:
        async with AsyncHoloClient(self.holo_config) as client:
            schema = await client.get_table_schema(self.conf.table_name)
            write_columns = get_write_columns(self.conf, schema)

            target_time = time.time() + self.conf.test_time / 1000.0
            rng = random.Random()
            i = 0

            while True:
                pk = self.tic.increment_and_get()
                i += 1

                if i % 1000 == 0 and time.time() > target_time:
                    break

                put = Put(schema)
                self._fill_record(put, pk, schema, write_columns, rng)

                start_ns = time.monotonic_ns()
                await client.put(put)
                elapsed_ms = (time.monotonic_ns() - start_ns) / 1_000_000
                self.meter.mark()
                self.histogram.update(elapsed_ms)

            await client.flush()
            logger.info("wrote %d records", i)

    def _fill_record(self, put, pk, schema, write_columns, rng):
        """Populate a Put with test data."""
        from datetime import datetime, timezone
        from hologres.column import (
            BIGINT,
            BINARY,
            BOOLEAN,
            DATE,
            DOUBLE,
            INTEGER,
            NUMERIC,
            REAL,
            SMALLINT,
            TIMESTAMP,
            TIMESTAMP_WITH_TIMEZONE,
            VARCHAR,
        )
        from .util import align_with_column_size

        for col_name in write_columns:
            col_idx = schema.get_column_index(col_name)
            if col_idx is None:
                continue
            col = schema.get_column(col_idx)
            value = pk
            col_type = col.type

            if col_type in (INTEGER, BIGINT, SMALLINT):
                put.set_object(col_name, value)
            elif col_type in (DOUBLE, REAL):
                put.set_object(col_name, float(value))
            elif col_type == NUMERIC:
                from decimal import Decimal

                put.set_object(col_name, Decimal(value))
            elif col_type == VARCHAR:
                put.set_object(
                    col_name, align_with_column_size(value, self.conf.column_size)
                )
            elif col_type == BINARY:
                put.set_object(col_name, rng.randbytes(self.conf.column_size))
            elif col_type in (TIMESTAMP, TIMESTAMP_WITH_TIMEZONE):
                if self.conf.fill_timestamp_with_now:
                    put.set_object(col_name, datetime.now(timezone.utc))
                else:
                    put.set_object(
                        col_name, datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                    )
            elif col_type == DATE:
                put.set_object(
                    col_name,
                    datetime.fromtimestamp(value / 1000, tz=timezone.utc).date(),
                )
            elif col_type == BOOLEAN:
                put.set_object(col_name, value % 2 == 0)
            else:
                put.set_object(
                    col_name, align_with_column_size(value, self.conf.column_size)
                )
