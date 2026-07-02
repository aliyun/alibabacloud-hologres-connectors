"""Fixed COPY (STREAM mode) write test using CopyWriter.

Streams records via PostgreSQL COPY protocol with stream_mode=true.
Single connection, one COPY stream.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Callable

import psycopg

from hologres._schema_loader import load_table_schema_sync
from hologres.copy import CopyFormat, CopyMode, CopyWriter
from hologres.put import Put
from hologres.table_name import TableName

from .put_test import PutTest
from .util import get_write_columns

logger = logging.getLogger(__name__)


class FixedCopyTest(PutTest):
    """Write test using COPY protocol with STREAM mode (fixed copy)."""

    def run(self, conf_name: str, update_mode: bool = False) -> None:
        from .conf_loader import build_holo_config, load_conf
        from .reporter import Reporter
        from .sql_util import create_table, drop_table, vacuum_table

        self.conf_name = conf_name
        load_conf(conf_name, "put.", self.conf)
        if update_mode:
            self.conf.create_table_before_run = False
        self.holo_config = build_holo_config(conf_name)
        self.init()

        # Setup table
        with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
            if self.conf.create_table_before_run:
                create_table(conn, self.conf)
            if self.conf.vacuum_table_before_run:
                vacuum_table(conn, self.conf.table_name)

        reporter = Reporter(conf_name)
        reporter.start(self.holo_config.conninfo)

        self.target_time = time.time() + self.conf.test_time / 1000.0
        import threading

        job = self.build_job(0)
        t = threading.Thread(target=job, name="Thread-0", daemon=True)
        t.start()
        t.join()
        logger.info("finished, %d rows written", self.meter.count)
        reporter.report(
            self.meter, self.histogram, dump_memory_stat=self.conf.dump_memory_stat
        )

        # Cleanup table
        if self.conf.delete_table_after_done:
            with psycopg.connect(self.holo_config.conninfo, autocommit=True) as conn:
                drop_table(conn, self.conf.table_name)

    def build_job(self, thread_id: int) -> Callable:
        def job():
            conn = psycopg.connect(self.holo_config.conninfo, autocommit=True)
            try:
                tn = TableName.valueOf(self.conf.table_name)
                schema = load_table_schema_sync(conn, tn)
                write_columns = get_write_columns(self.conf, schema)
                rng = random.Random(thread_id)

                writer = CopyWriter(
                    conn,
                    schema,
                    mode=CopyMode.STREAM,
                    fmt=CopyFormat.TEXT,
                    on_conflict=self.holo_config.on_conflict_action,
                )
                with writer:
                    i = 0
                    while True:
                        pk = self.tic.increment_and_get()
                        i += 1

                        if i % 1000 == 0 and self.should_stop(i, pk):
                            break

                        put = Put(schema)
                        self.fill_record(put, pk, schema, write_columns, rng)

                        start_ns = time.monotonic_ns()
                        writer.write(put)
                        elapsed_ms = (time.monotonic_ns() - start_ns) / 1_000_000

                        self.meter.mark()
                        self.histogram.update(elapsed_ms)

                logger.info("Thread-%d: copy wrote %d records", thread_id, i)
            except Exception:
                logger.exception("Thread-%d failed", thread_id)
            finally:
                conn.close()

        return job
