"""Record collector and action collector for batching write/read operations."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .record import Record
from .table_name import TableName
from .table_schema import TableSchema
from .types import MutationType, OnConflictAction

logger = logging.getLogger("hologres.collector")


@dataclass
class FlushBatch:
    """A batch of records ready to flush for a specific table shard."""

    table_name: TableName
    shard_id: int
    schema: TableSchema
    deletes: List[Record]
    inserts: List[Record]


class RecordCollector:
    """Accumulates records for a single table with deduplication.

    Tracks batch state and triggers flush when thresholds are met.
    """

    def __init__(
        self,
        max_records: int = 512,
        max_byte_size: int = 2 * 1024 * 1024,
        max_wait_time_ms: int = 10_000,
        on_conflict: OnConflictAction = None,
        enable_deduplication: bool = True,
        shard_count: int = 1,
        get_available_byte_size: Any = None,
    ):
        self._max_records = max_records
        self._max_byte_size = max_byte_size
        self._max_wait_time_ms = max_wait_time_ms
        self._on_conflict = on_conflict
        self._enable_deduplication = enable_deduplication
        self._shard_count = shard_count
        self._get_available_byte_size = get_available_byte_size

        # Separate maps for inserts and deletes, keyed by PK tuple
        self._inserts: Dict[tuple, Record] = {}
        self._deletes: Dict[tuple, Record] = {}
        self._byte_size: int = 0
        self._first_append_time: Optional[float] = None
        self._schema: Optional[TableSchema] = None

    @property
    def size(self) -> int:
        return len(self._inserts) + len(self._deletes)

    @property
    def byte_size(self) -> int:
        return self._byte_size

    @property
    def on_conflict(self) -> OnConflictAction:
        return self._on_conflict

    @property
    def schema(self) -> Optional[TableSchema]:
        return self._schema

    def is_empty(self) -> bool:
        return self.size == 0

    def append(self, record: Record) -> bool:
        """Append a record, deduplicating by primary key.

        Returns True if the batch is ready to flush.
        """
        if self._first_append_time is None:
            self._first_append_time = time.monotonic()
        self._schema = record.schema

        key = record.get_key_values() if record.schema.has_primary_key else id(record)

        if record.type == MutationType.DELETE:
            # Remove from inserts if present
            existing = self._inserts.pop(key, None)
            if existing:
                self._byte_size -= existing.byte_size
            old_del = self._deletes.get(key)
            if old_del:
                self._byte_size -= old_del.byte_size
            self._deletes[key] = record
            self._byte_size += record.byte_size

        elif self._enable_deduplication and key in self._inserts:
            existing = self._inserts[key]
            self._byte_size -= existing.byte_size

            if self._on_conflict == OnConflictAction.INSERT_OR_UPDATE:
                existing.merge(record)
            elif self._on_conflict == OnConflictAction.INSERT_OR_IGNORE:
                existing.cover(record)
            else:  # INSERT_OR_REPLACE
                self._inserts[key] = record

            self._byte_size += self._inserts[key].byte_size
        else:
            # TODO: review this
            # New insert
            old_del = self._deletes.pop(key, None)
            if old_del:
                self._byte_size -= old_del.byte_size
            self._inserts[key] = record
            self._byte_size += record.byte_size

        return self._is_batch_ready()

    def _is_batch_ready(self) -> bool:
        """Check if the batch should be flushed."""
        if self.size >= self._max_records:
            return True
        if self._byte_size >= self._max_byte_size:
            return True
        if self._is_time_exceeded():
            return True
        # Early commit heuristic at power-of-2 record counts (matching Java)
        size = self.size
        if size > 0 and (size & (size - 1)) == 0:
            if self._get_available_byte_size is not None:
                available = self._get_available_byte_size()
                if self._byte_size * self._shard_count > available:
                    return True
        return False

    def _is_time_exceeded(self) -> bool:
        if self._first_append_time is None:
            return False
        elapsed_ms = (time.monotonic() - self._first_append_time) * 1000
        return elapsed_ms >= self._max_wait_time_ms

    def get_records(self) -> Tuple[List[Record], List[Record]]:
        """Return (delete_records, insert_records) and clear the buffer."""
        deletes = list(self._deletes.values())
        inserts = list(self._inserts.values())
        self.clear()
        return deletes, inserts

    def clear(self) -> None:
        self._inserts.clear()
        self._deletes.clear()
        self._byte_size = 0
        self._first_append_time = None
        self._schema = None


class TableShardCollector:
    """Routes records to N RecordCollectors by distribution key hash.

    When num_shards == 1, behaves identically to a single RecordCollector.
    """

    def __init__(
        self, num_shards: int, get_available_byte_size: Any = None, **rc_kwargs
    ):
        self._num_shards = num_shards
        self._rc_kwargs = rc_kwargs
        self._shards: List[RecordCollector] = [
            RecordCollector(
                shard_count=num_shards,
                get_available_byte_size=get_available_byte_size,
                **rc_kwargs,
            )
            for _ in range(num_shards)
        ]

    @property
    def num_shards(self) -> int:
        return self._num_shards

    def append(self, record: Record) -> List[int]:
        """Append record to appropriate shard.

        Returns list of shard indices whose batches are ready to flush.
        """
        from ._shard import compute_shard

        shard_id = compute_shard(record, self._num_shards)
        ready = self._shards[shard_id].append(record)
        return [shard_id] if ready else []

    def get_shard(self, shard_id: int) -> RecordCollector:
        return self._shards[shard_id]

    def get_ready_shards(self, force: bool = False) -> List[int]:
        """Return shard indices that are ready to flush."""
        result = []
        for i, shard in enumerate(self._shards):
            if shard.is_empty():
                continue
            if force or shard._is_batch_ready():
                result.append(i)
        return result

    def has_pending(self) -> bool:
        return any(not s.is_empty() for s in self._shards)

    @property
    def total_byte_size(self) -> int:
        return sum(s.byte_size for s in self._shards)


class ActionCollector:
    """Manages TableShardCollectors across multiple tables.

    Provides append/flush interface used by HoloClient.
    Each table gets a TableShardCollector with num_shards RecordCollectors.
    """

    def __init__(
        self,
        max_records: int = 512,
        max_byte_size: int = 2 * 1024 * 1024,
        max_total_byte_size: int = 20 * 1024 * 1024,
        max_wait_time_ms: int = 10_000,
        on_conflict: OnConflictAction = OnConflictAction.INSERT_OR_REPLACE,
        enable_deduplication: bool = True,
        num_shards: int = 1,
    ):
        self._max_records = max_records
        self._max_byte_size = max_byte_size
        self._max_total_byte_size = max_total_byte_size
        self._max_wait_time_ms = max_wait_time_ms
        self._on_conflict = on_conflict
        self._enable_deduplication = enable_deduplication
        self._num_shards = num_shards

        self._collectors: Dict[TableName, TableShardCollector] = {}

    def _get_available_byte_size(self) -> int:
        """Return remaining global byte budget"""
        return self._max_total_byte_size - self.total_byte_size

    def _get_collector(self, table_name: TableName) -> TableShardCollector:
        collector = self._collectors.get(table_name)
        if collector is None:
            collector = TableShardCollector(
                num_shards=self._num_shards,
                get_available_byte_size=self._get_available_byte_size,
                max_records=self._max_records,
                max_byte_size=self._max_byte_size,
                max_wait_time_ms=self._max_wait_time_ms,
                on_conflict=self._on_conflict,
                enable_deduplication=self._enable_deduplication,
            )
            self._collectors[table_name] = collector
        return collector

    def append(self, record: Record) -> bool:
        """Append a record. Returns True if any shard's batch is ready."""
        collector = self._get_collector(record.table_name)
        ready_shards = collector.append(record)
        return len(ready_shards) > 0

    def append_with_batches(self, record: Record) -> List[FlushBatch]:
        """Append a record and return any FlushBatches that are ready.

        Used by the async writer path. Returns ready batches immediately
        so they can be enqueued as WriteActions.
        """
        table_name = record.table_name
        collector = self._get_collector(table_name)
        ready_shards = collector.append(record)

        batches = []
        for shard_id in ready_shards:
            rc = collector.get_shard(shard_id)
            schema = rc.schema
            deletes, inserts = rc.get_records()
            batches.append(FlushBatch(table_name, shard_id, schema, deletes, inserts))
        return batches

    def drain_ready_batches(self, force: bool = False) -> List[FlushBatch]:
        """Drain all ready batches across all tables and shards.

        If force=True, drain all non-empty shards.
        """
        batches = []
        for tn, tsc in self._collectors.items():
            for shard_id in tsc.get_ready_shards(force=force):
                rc = tsc.get_shard(shard_id)
                schema = rc.schema
                deletes, inserts = rc.get_records()
                batches.append(FlushBatch(tn, shard_id, schema, deletes, inserts))
        return batches

    def get_flushable_tables(self, force: bool = False) -> List[TableName]:
        """Return table names that have at least one shard ready to flush.

        If force=True, return all non-empty tables.
        """
        result = []
        for tn, tsc in self._collectors.items():
            if tsc.has_pending() and (force or tsc.get_ready_shards()):
                result.append(tn)
        return result

    def get_records(
        self, table_name: TableName
    ) -> Tuple[Optional[TableSchema], List[Record], List[Record]]:
        """Get and clear records for a table (all shards merged).

        Returns (schema, delete_records, insert_records).
        Used by the sync writer path.
        """
        tsc = self._collectors.get(table_name)
        if tsc is None or not tsc.has_pending():
            return None, [], []
        all_deletes: List[Record] = []
        all_inserts: List[Record] = []
        schema = None
        for shard_id in tsc.get_ready_shards(force=True):
            rc = tsc.get_shard(shard_id)
            if schema is None:
                schema = rc.schema
            deletes, inserts = rc.get_records()
            all_deletes.extend(deletes)
            all_inserts.extend(inserts)
        return schema, all_deletes, all_inserts

    def has_pending(self) -> bool:
        return any(c.has_pending() for c in self._collectors.values())

    @property
    def total_byte_size(self) -> int:
        return sum(c.total_byte_size for c in self._collectors.values())
