"""Configuration dataclasses mirroring Java performance test configs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PutTestConf:
    """Config for write tests (INSERT / FIXED_COPY).

    Mirrors Java PutTestConf. Field names use snake_case but conf_loader
    matches them case-insensitively to Java's camelCase keys.
    """

    # Threading
    thread_size: int = 10
    test_time: int = 600000  # ms (10 minutes)
    row_number: int = 1000000
    test_by_time: bool = True

    # Table
    table_name: str = "holo_perf"
    column_count: int = 100
    column_size: int = 10
    data_column_type: str = "text"
    orientation: str = "column"
    shard_count: int = -1
    has_pk: bool = True
    enable_bitmap: bool = True

    # Partition
    partition: bool = False
    partition_count: int = 30
    partition_ratio: int = 10

    # Prefix PK (for scan tests)
    prefix_pk: bool = False
    record_count_per_prefix: int = 100

    # Columns
    addition_ts_column: bool = True
    fill_timestamp_with_now: bool = True
    write_column_count: int = -1

    # Lifecycle
    create_table_before_run: bool = True
    vacuum_table_before_run: bool = False
    delete_table_after_done: bool = True

    # Memory
    dump_memory_stat: bool = False


@dataclass
class GetTestConf:
    """Config for GET (point query) tests.

    Mirrors Java GetTestConf.
    """

    thread_size: int = 10
    test_time: int = 600000  # ms (10 minutes)
    table_name: str = "holo_perf"
    key_range_params: str = ""
    mode: str = "async-with-future"  # sync, async-with-future, async-with-coroutine
    batch_size: int = 100
    queue_size: int = -1  # -1 means auto: batch_size * thread_size * 2
    vacuum_table_before_run: bool = True
    delete_table_after_done: bool = False
    dump_memory_stat: bool = False


@dataclass
class ScanTestConf:
    """Config for SCAN (prefix scan) tests.

    Mirrors Java ScanTest config. Scans all rows matching a distribution key
    value via SELECT * FROM table WHERE id = ?.
    """

    thread_size: int = 10
    test_time: int = 600000  # ms (10 minutes)
    table_name: str = "holo_perf"
    key_range_params: str = ""
    vacuum_table_before_run: bool = True
    delete_table_after_done: bool = False
    dump_memory_stat: bool = False
