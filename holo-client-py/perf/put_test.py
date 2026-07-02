"""Abstract base class for write performance tests.

Mirrors Java PutTest: handles table creation, thread management,
fill_record logic, and lifecycle.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Callable, List

import psycopg

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
from hologres.config import HoloConfig
from hologres.put import Put
from hologres.table_schema import TableSchema

from .conf_loader import build_holo_config, load_conf
from .config import PutTestConf
from .metrics import Histogram, Meter
from .reporter import Reporter
from .sql_util import create_table, drop_table, vacuum_table
from .util import AtomicLong, align_with_column_size, get_write_columns

logger = logging.getLogger(__name__)


class PutTest(ABC):
    """Abstract base for write tests (INSERT / FIXED_COPY)."""

    def __init__(self):
        self.conf = PutTestConf()
        self.holo_config: HoloConfig = None
        self.conf_name = ""
        self.target_time = 0.0
        self.tic = AtomicLong(0)
        self.meter = Meter()
        self.bps_meter = Meter()
        self.histogram = Histogram()

    def run(self, conf_name: str) -> None:
        self.conf_name = conf_name
        load_conf(conf_name, "put.", self.conf)
        self.holo_config = build_holo_config(conf_name)
        self.init()

        # Setup
        with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
            if self.conf.create_table_before_run:
                create_table(conn, self.conf)
            if self.conf.vacuum_table_before_run:
                vacuum_table(conn, self.conf.table_name)

        reporter = Reporter(conf_name)
        reporter.start(self.holo_config.conninfo)
        self.target_time = time.time() + self.conf.test_time / 1000.0

        # Spawn threads
        threads: List[threading.Thread] = []
        for i in range(self.conf.thread_size):
            job = self.build_job(i)
            t = threading.Thread(target=job, name=f"Thread-{i}", daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # Subclass cleanup (e.g. flush/close shared client)
        self.cleanup()

        logger.info("finished, %d rows written", self.meter.count)

        # Report
        reporter.report(
            self.meter, self.histogram, dump_memory_stat=self.conf.dump_memory_stat
        )

        # Cleanup
        if self.conf.delete_table_after_done:
            with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
                drop_table(conn, self.conf.table_name)

    def init(self) -> None:
        """Override for subclass-specific initialization."""
        pass

    def cleanup(self) -> None:
        """Override for subclass-specific cleanup after threads finish."""
        pass

    @abstractmethod
    def build_job(self, thread_id: int) -> Callable:
        """Return a callable for the thread to run."""
        ...

    def should_stop(self, row_count: int, pk: int) -> bool:
        """Check termination condition. Called every 1000 rows."""
        if self.conf.test_by_time:
            return time.time() > self.target_time
        else:
            return pk > self.conf.row_number

    def fill_record(
        self,
        put: Put,
        pk: int,
        schema: TableSchema,
        write_columns: List[str],
        rng: random.Random,
    ) -> None:
        """Populate a Put with test data matching Java fillRecord.

        Args:
            put: The Put to fill.
            pk: The primary key value.
            schema: Table schema.
            write_columns: Column names to write.
            rng: Per-thread Random instance.
        """
        for col_name in write_columns:
            col_idx = schema.get_column_index(col_name)
            if col_idx is None:
                continue
            col = schema.get_column(col_idx)

            # Determine the value for this column
            value = pk

            # For prefix PK mode
            if self.conf.prefix_pk and col_name == "id":
                value = (pk - 1) // self.conf.record_count_per_prefix
            elif self.conf.prefix_pk and col_name == "id1":
                value = pk

            # Partition column
            if col_name == "ds":
                if value >= self.conf.partition_count:
                    # Skew: values >= partitionCount map to 0
                    if self.conf.partition_ratio > 0:
                        value = rng.randint(0, self.conf.partition_count - 1)
                    else:
                        value = 0
                put.set_object(col_name, value)
                continue

            # Set by type
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
