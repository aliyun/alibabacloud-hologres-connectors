# holo-client-py Put Pipeline: Architecture and Internals

This document walks through how a `put()` call in holo-client-py flows from the user-facing API all the way to SQL execution on Hologres. It mirrors the structure of the [Java holo-client put-pipeline doc](../../holo-client/docs/put-pipeline.md) for easy comparison.

---

## 1. High-Level Architecture

holo-client-py supports three write paths: **sync single-process** (write_parallelism=1), **sync multi-process** (write_parallelism>1), and **async multi-writer** (AsyncHoloClient).

### Single-Process Path (default, `write_parallelism == 1`)

```
User Thread                        Background Thread
──────────                         ─────────────────

HoloClient.put(Put)
      │
      ▼
ActionCollector.append(Record)     holo-bg-flush (periodic loop)
      │                                  │
      ▼                                  ▼
TableShardCollector.append(Record) HoloClient._do_flush(force=False)
      │  (compute_shard → shard 0)       │
      ▼                                  ▼
RecordCollector.append(Record)     ActionCollector.get_flushable_tables()
      │                                  │
      │  (when batch threshold met)      │  (when time threshold met)
      ▼                                  ▼
  ┌──────────────────────────────────────────┐
  │      HoloClient._do_flush(force)        │
  │  Lock _collector_lock → drain records   │
  └────────────────┬─────────────────────────┘
                   │
                   ▼
        _execute_batch(schema, deletes, inserts)
                   │
                   ▼
        build_delete_sql() / build_insert_sql()
                   │
                   ▼
        _execute_with_retry(conn, sql, params)
                   │
                   ▼
        psycopg cursor.execute(sql, params)
```

### Multi-Process Path (`write_parallelism > 1`)

```
User Thread                          Worker Process (× N)
──────────                           ────────────────────

HoloClient.put(Put)
      │
      ▼
compute_shard(record, N) → idx
      │
      ▼
serialize_record_msg(tn_key, record)
      │
      ▼
ShmRingBuffer[idx].write_message()
      ║ (shared memory IPC)            _writer_worker_shm()
      ║                                      │
      ╚══════════════════════════════════▶ ShmRingBuffer.read_message()
                                             │
                                             ▼
                                      ActionCollector.append(record)
                                             │
                                             ▼
                                      _do_flush() → _execute_batch()
                                             │
                                             ▼
                                      psycopg cursor.execute(sql, params)
```

### Async Multi-Writer Path (AsyncHoloClient, `write_parallelism > 1`)

```
User Code (async)                    Background Tasks (× N)
─────────────────                    ──────────────────────

await client.put(Put)
      │
      ▼
compute_shard(record, N) → idx
      │
      ▼
writer_collectors[idx].append(record)  _bg_loop_writer(i) (periodic flush)
      │                                      │
      │  (when batch threshold met)          │  (when time threshold met)
      ▼                                      ▼
_do_flush_writer(idx)                  _do_flush_writer(i)
      │                                      │
      ▼                                      ▼
build_insert_sql() / build_delete_sql()
      │
      ▼
writer_conns[idx].execute(sql, params)
```

Each writer has:
- Its own `ActionCollector` (with `max_total_byte_size / N`)
- Its own `asyncio.Lock`
- Its own `AsyncConnection` (lazy, created on first flush)
- Its own background flush task

`await client.flush()` triggers `asyncio.gather(*[_do_flush_writer(i, True) for i in range(N)])`.

### The pipeline classes

| Level | Class | Multiplicity | Key Responsibility |
|-------|-------|-------------|-------------------|
| 1 | `HoloClient` / `AsyncHoloClient` | 1 per user | User-facing API, write path routing |
| 2 | `ActionCollector` | 1 per client (single-process), 1 per worker process (multi-process), or N per AsyncHoloClient (multi-writer) | Dispatches records by table; owns per-table collectors |
| 3 | `TableShardCollector` | 1 per table | Routes records to shards by distribution key |
| 4 | `RecordCollector` | N per table (default N=1 for single-process) | Buffers records, deduplicates by primary key |
| 5 | `_sql.py` | Stateless | SQL generation (unnest or VALUES) |
| 6 | `psycopg` | 1 connection per client (single-process), N per multi-process, N per async multi-writer | SQL execution |

---

## 2. Detailed Class Roles

### 2.1 HoloClient

**File:** `hologres/client.py`

**Role:** User-facing entry point. Provides `put()`, `flush()`, and `close()`.

**What happens in `put(Put)`:**

1. `_check_state()` — verifies client is not closed and no background exception is pending.
2. Extracts `record = put.record`.
3. `_validate_put(record)` — for DELETE operations, checks that the table has a PK and all PK columns are set.
4. **Single-process path:** Acquires `_collector_lock` (a `threading.Lock`), calls `self._collector.append(record)`. If the batch threshold is met, immediately calls `_do_flush(force=False)` on the calling thread.
5. **Multi-process path:** Calls `_put_to_worker(record)` — serializes the record into a shared memory ring buffer for the appropriate worker process.

```python
# Single-process path:
with self._collector_lock:
    batch_ready = self._collector.append(record)
if batch_ready:
    self._do_flush(force=False)

# Multi-process path:
self._put_to_worker(record)
```

**Key difference from Java:** In Java, `put()` only appends to the collector; flushing always happens via background threads or explicit `flush()`. In Python's single-process path, `put()` may trigger an **inline flush on the calling thread** when a batch threshold is met, in addition to the periodic background flush.

---

### 2.2 ActionCollector

**File:** `hologres/_collector.py`

**Role:** The central dispatch hub. Routes records to per-table `TableShardCollector` instances.

**Key fields:**
```python
_collectors: Dict[TableName, TableShardCollector]  # per-table collectors
_max_total_byte_size: int                          # global byte limit (default 20MB)
```

**`append(Record) → bool`:**
1. Looks up (or creates) a `TableShardCollector` for the record's `table_name`.
2. Delegates to `tableShardCollector.append(record)`.
3. If any shard reports "batch ready", returns `True`.
4. Otherwise, checks total byte size across ALL tables/shards against `max_total_byte_size` (default 20MB). If exceeded, returns `True`.

**`get_flushable_tables(force) → List[TableName]`:**
- If `force=True`: returns all non-empty tables.
- If `force=False`: returns tables with at least one shard where `_is_batch_ready()` is true.

**`get_records(table_name) → (schema, deletes, inserts)`:**
- Iterates all shards for the table (force=True to drain everything).
- Merges all shards' deletes and inserts into one list per table.
- Clears the drained shards.

**Key difference from Java:** Java's `ActionCollector` uses a `ReentrantReadWriteLock` to allow concurrent appends with exclusive flush. Python uses a simple `threading.Lock` (`_collector_lock`) held by `HoloClient`, since the GIL already provides some thread safety, and the single-process path typically has only one writer thread.

---

### 2.3 TableShardCollector

**File:** `hologres/_collector.py`

**Role:** Per-table fan-out. Routes records to one of N `RecordCollector` instances using `compute_shard()`.

**Key fields:**
```python
_shards: List[RecordCollector]  # client-side shards
_num_shards: int                # number of shards
```

**`append(Record) → List[int]`:**
1. Computes shard index: `shard_id = compute_shard(record, num_shards)`.
2. Delegates to `_shards[shard_id].append(record)`.
3. Returns `[shard_id]` if that shard's batch is ready, else `[]`.

**Shard count:** In the single-process path, `num_shards=1` — all records go to shard 0 (no routing). In the multi-process path, shard routing happens at the `HoloClient` level (routing to worker processes), so each worker's collector also uses `num_shards=1`.

**Key difference from Java:** Java's `TableCollector` uses `writeThreadSize` shards (default matches thread pool size) to fan out records for parallel worker execution. Python's single-process path uses a single shard because there is only one write connection — there is no worker pool to fan out to.

---

### 2.4 RecordCollector (the buffer)

**File:** `hologres/_collector.py`

**Role:** In-memory buffer that accumulates and deduplicates records before they are flushed as a batch.

**Key fields:**
```python
_inserts: Dict[tuple, Record]          # insert/upsert records, keyed by PK tuple
_deletes: Dict[tuple, Record]          # delete records, keyed by PK tuple
_byte_size: int                        # total byte size
_first_append_time: Optional[float]    # timestamp of first record (for time-based flush)
_max_records: int                      # default 512
_max_byte_size: int                    # default 2MB
_max_wait_time_ms: int                 # default 10,000ms
```

**Deduplication logic in `append(Record)`:**

The dedup key is `record.get_key_values()` (tuple of PK column values) if the table has a primary key, otherwise `id(record)` (no dedup possible).

- **DELETE record:**
  - Removes any existing INSERT with the same key from `_inserts`.
  - Replaces any existing DELETE with the same key in `_deletes`.

- **INSERT with existing key** (dedup enabled):
  - `INSERT_OR_UPDATE`: calls `existing.merge(record)` — merges new values into existing, skipping `only_insert` columns.
  - `INSERT_OR_IGNORE`: calls `existing.cover(record)` — keeps existing values, prepends the new record's futures.
  - `INSERT_OR_REPLACE`: replaces entirely `_inserts[key] = record`.

- **INSERT with new key:**
  - Removes any existing DELETE with the same key (insert cancels delete).
  - Stores `_inserts[key] = record`.

**Batch readiness — `_is_batch_ready()`:**
Returns `True` when any threshold is met:
- Record count ≥ `max_records` (default 512)
- Byte size ≥ `max_byte_size` (default 2MB)
- Time since `_first_append_time` ≥ `max_wait_time_ms` (default 10,000ms)

**`get_records() → (deletes, inserts)`:**
Returns `(list(self._deletes.values()), list(self._inserts.values()))` and clears the buffer.

**Key difference from Java:** Java's `RecordCollector` has additional "early commit" heuristics (`TimeCondition`, `ByteSizeCondition`, etc.) that trigger flush when the batch is at a power-of-2 count and 40%+ of the threshold. Python uses simple threshold checks only. Java also distinguishes delete records and insert records with separate ordering guarantees; Python similarly executes deletes before inserts.

---

### 2.5 Record

**File:** `hologres/record.py`

**Role:** Internal mutable row representation. Created by `Put.__init__()`.

**Key fields (via `__slots__`):**
```python
schema: TableSchema              # table schema
table_name: TableName            # cached from schema
values: List[Any]                # column values, length = column_count
_set_columns: set[int]           # which column indices have been explicitly set
_only_insert_columns: set[int]   # columns marked for insert-only
type: MutationType               # INSERT or DELETE
byte_size: int                   # running estimate, updated on every set_object()
_futures: list                   # for async completion tracking
```

**`set_object(index, value)`:** Updates `values[index]`, adds `index` to `_set_columns`, and incrementally adjusts `byte_size` using type-specific size estimates.

**`get_key_values()`:** Returns a tuple of primary key column values, used as the deduplication key.

**`merge(other)`:** For `INSERT_OR_UPDATE` — copies set values from `other` that are NOT in `only_insert_columns`, appends `other._futures`.

**`cover(other)`:** For `INSERT_OR_IGNORE` — keeps this record's values but prepends `other._futures`.

**Key difference from Java:** Java's `Record` is used across multiple classes (`Put`, `PutAction`) with a richer lifecycle. Python's `Record` is simpler — no `PutAction` wrapper is needed because there is no worker pool or `CompletableFuture`-based pipeline.

---

### 2.6 Put

**File:** `hologres/put.py`

**Role:** User-facing wrapper around `Record`. Provides a fluent API for setting column values.

```python
put = Put(schema)
put.set_object("id", 1)
put.set_object("name", "Alice")
put.mutation_type = MutationType.DELETE  # for deletes
```

`Put.set_object(column_name, value)` resolves the column name to an index via `TableSchema.get_column_index()` and delegates to `Record.set_object()`. Returns `self` for method chaining.

---

### 2.7 SQL Generation

**File:** `hologres/_sql.py`

**Role:** Converts a batch of `Record` objects into SQL statements for execution.

#### INSERT — Two Strategies

`build_insert_sql()` first determines the union of all set columns across all records, then chooses a strategy:

**Unnest-based (preferred)** — when all columns support it:
```sql
INSERT INTO "schema"."table" ("c1", "c2")
SELECT unnest(%s::int4[]), unnest(%s::text[])
ON CONFLICT ("pk") DO UPDATE SET "c2"=EXCLUDED."c2"
```
Parameters are **columnar**: one Python list per column. The SQL text stays constant regardless of batch size, enabling server-side prepared statement caching.

Supported types for unnest: `bool`, `int2/4/8`, `float4/8`, `numeric`, `text`, `date`, `time`, `timestamp`, `timestamptz`, `bytea`, `json`, `jsonb`, `serial` types. NOT supported: `uuid`, `roaringbitmap`, arrays, `OTHER` types.

**VALUES-based (fallback)** — when any column doesn't support unnest:
```sql
INSERT INTO "schema"."table" ("c1", "c2")
VALUES (%s, %s), (%s, %s), ...
ON CONFLICT (...) DO UPDATE SET ...
```
Parameters are **row-major**: flattened list of all values.

The ON CONFLICT clause varies by `OnConflictAction`:
- `INSERT_OR_IGNORE` → `DO NOTHING`
- `INSERT_OR_UPDATE` / `INSERT_OR_REPLACE` → `DO UPDATE SET col=EXCLUDED.col` for non-only_insert columns

#### DELETE
```sql
DELETE FROM "schema"."table" WHERE ("pk1"=%s AND "pk2"=%s) OR (...)
```
Row-based WHERE clause (no unnest for deletes).

**Key difference from Java:** Java uses JDBC `PreparedStatement.executeBatch()` with `UpsertStatementBuilder` or `UnnestUpsertStatementBuilder`. Python uses psycopg's `cursor.execute()` with prepared statements (`prepare=True`). The unnest SQL format is identical.

---

### 2.8 Flush Pipeline: `_do_flush()`

**File:** `hologres/client.py`

**Role:** Drains the collector and executes batches via SQL.

```python
def _do_flush(self, force: bool) -> None:
    with self._collector_lock:
        tables = self._collector.get_flushable_tables(force=force)
        batches = []
        for tn in tables:
            schema, deletes, inserts = self._collector.get_records(tn)
            if schema and (deletes or inserts):
                batches.append((schema, deletes, inserts))

    # Execute OUTSIDE the lock
    for schema, deletes, inserts in batches:
        self._execute_batch(schema, deletes, inserts)
```

1. **Under `_collector_lock`:** Identifies flushable tables and drains their records.
2. **Outside the lock:** Executes each batch via `_execute_batch()`. This design prevents SQL execution from blocking new `put()` calls.

---

### 2.9 Batch Execution: `_execute_batch()`

**File:** `hologres/client.py`

**Role:** Executes a batch of deletes and inserts for a single table.

1. Ensures a write connection via `_ensure_write_conn()` (lazy creation with idle-timeout cleanup).
2. Executes **DELETEs first**, then **INSERTs**:
   - `build_delete_sql(schema, deletes)` → `_execute_with_retry(conn, sql, params)`
   - `build_insert_sql(schema, inserts, on_conflict)` → `_execute_with_retry(conn, sql, params)`
3. On dirty-data error with `WriteFailStrategy.TRY_ONE_BY_ONE`:
   - Calls `_try_one_by_one()` which re-executes each record individually.
   - Good records succeed; only truly bad records fail.
   - Failures are collected into `HoloClientWithDetailsException` with per-record details.

---

### 2.10 Retry Logic: `_execute_with_retry()`

**File:** `hologres/client.py`

Retries up to `retry_count` (default 3) times:

1. Executes `cursor.execute(sql, params, prepare=True)`.
2. On retryable error (`CONNECTION_ERROR`, `READ_ONLY`, `META_NOT_MATCH`, `TIMEOUT`, `BUSY`, `TOO_MANY_CONNECTIONS`):
   - Sleeps `retry_sleep_init_ms + attempt * retry_sleep_step_ms` (default: 1000ms, then 11000ms).
   - Closes and reconnects.
   - Retries.
3. On non-retryable error (dirty data, syntax, etc.): raises immediately.

---

### 2.11 Background Flush Thread

**File:** `hologres/client.py`

A daemon thread named `"holo-bg-flush"` runs in a loop:

```python
check_interval = max(0.5, write_max_interval_ms / 4000)  # e.g. 2.5s for 10s interval

while not self._bg_stop.wait(check_interval):
    self._do_flush(force=False)  # only flush batches that have met thresholds
```

This ensures time-based flushing (`max_wait_time_ms`) happens even if the user stops calling `put()`.

On error, stores the exception in `self._bg_exception`, which is checked and re-raised on the next `put()` or `flush()` call via `_check_state()`.

---

## 3. Multi-Process Path Details

### 3.1 Worker Startup

`_start_worker_processes()` spawns N worker processes:

1. Creates N `ShmRingBuffer` objects (8MB each, POSIX shared memory).
2. Spawns N `multiprocessing.Process` instances running `_writer_worker_shm`.
3. Starts an error monitor thread that polls a `multiprocessing.Queue` for errors and dead workers.

### 3.2 Record Serialization: `_put_to_worker()`

1. Computes `idx = compute_shard(record, write_parallelism)` — routes to a worker.
2. **Schema registration (once per table per worker):** Sends a `0xFFFF`-prefixed message containing pickled `(schema, table_name)` through shared memory.
3. **Record serialization:** `serialize_record_msg(tn_key, record)` — custom binary format avoiding pickle for common types:
   - `TAG_INT=1`, `TAG_FLOAT=2`, `TAG_STR=3`, `TAG_BOOL=4`, `TAG_BYTES=5`, `TAG_DATETIME=6`, `TAG_DATE=7`
   - `TAG_PICKLE=255` as fallback for uncommon types
4. Writes to `ShmRingBuffer` with retry on buffer-full (1s timeout, checks `_check_state()` between retries).

### 3.3 Worker Process: `_writer_worker_shm()`

**File:** `hologres/_worker.py`

Each worker process:
1. Creates its own `ActionCollector` (same config thresholds).
2. Creates its own database connection.
3. Starts its own background flush timer thread.
4. Main loop reads messages from the ring buffer:
   - **`MSG_RECORD`:** Deserializes, appends to collector; if batch ready, flushes immediately.
   - **`MSG_FLUSH`:** Force-flushes, sends `('flush_ack', ack_id, worker_id)` via result queue.
   - **`MSG_CLOSE`:** Force-flushes, sends `('close_ack', worker_id)`, exits.
5. On error, sends `('error', worker_id, msg, code)` via the result queue.

### 3.4 ShmRingBuffer

**File:** `hologres/_shm_buffer.py`

SPSC (single-producer, single-consumer) ring buffer:
- Backed by POSIX `SharedMemory` (8MB default).
- `write_pos` and `read_pos` are `multiprocessing.Value('Q')` — unsigned 64-bit, monotonically increasing.
- `write_message()`: spins waiting for space (100μs sleep), writes 5-byte header (4B length + 1B type) + payload, atomically updates `write_pos`.
- Max message size: `capacity // 2` (4MB for default 8MB buffer).

### 3.5 Multi-Process Flush

```python
def _flush_workers(self):
    ack_id = ++self._flush_ack_id
    # Send MSG_FLUSH to each worker's shm buffer
    for shm_buf in self._shm_buffers:
        shm_buf.write_message(MSG_FLUSH, struct.pack('<I', ack_id))
    # Wait for all ('flush_ack', ack_id, worker_id) messages (60s timeout)
    while len(acked) < n:
        msg = self._worker_result_queue.get(timeout=...)
```

---

## 3B. Async Multi-Writer Path Details

### 3B.1 Writer Initialization

`AsyncHoloClient.__init__()` creates N writers when `write_parallelism > 1`:

```python
self._num_writers = config.write_parallelism
self._writer_collectors: List[ActionCollector] = []  # N collectors
self._writer_locks: List[asyncio.Lock] = []          # N locks
self._writer_conns: List[Optional[AsyncConnection]] = []  # N connections (lazy)
per_writer_total = config.write_batch_total_byte_size // self._num_writers
```

Each collector has `max_total_byte_size = total / N`, ensuring the global memory limit is respected.

### 3B.2 Record Routing

`await client.put(put)`:
1. Extracts record, validates.
2. `idx = compute_shard(record, self._num_writers)` — same MurmurHash3-based routing as multi-process write.
3. Under `self._writer_locks[idx]`: appends to `self._writer_collectors[idx]`.
4. If batch ready, calls `_do_flush_writer(idx)`.

### 3B.3 Background Flush Tasks

N asyncio tasks (`_bg_loop_writer(i)`) run concurrently, each responsible for one writer:
- Sleeps for `write_max_interval_ms / 4` between checks.
- Calls `_do_flush_writer(i, force=False)` to flush batches that have met thresholds.

### 3B.4 Flush and Close

- `await client.flush()`: `asyncio.gather(*[_do_flush_writer(i, True) for i in range(N)])`
- `await client.close()`: Cancels all N background tasks, flushes all writers, closes all connections.

### 3B.5 Error Handling

Each `_do_flush_writer()` handles errors independently:
- On dirty-data error with `TRY_ONE_BY_ONE`: re-executes records individually.
- On connection error: closes and lazily reconnects on next flush.
- Background task errors are stored and re-raised on next `put()`.

---

## 4. Shard Routing

**File:** `hologres/_shard.py`

`compute_shard(record, num_shards)`:

1. If `num_shards <= 1`, returns 0.
2. Gets `distribution_key_index` from schema; falls back to `pk_index`; falls back to `hash(id(record)) % num_shards`.
3. Calls `hash_record(record, dk_indices)`:
   - For each distribution key column, converts the value to its "storage representation" via `_get_storage_value()` (timestamps → microseconds, dates → epoch days, decimals → 16-byte LE, UUIDs → 16-byte BE).
   - Hashes each value with MurmurHash3 32-bit, seed 104729 (matching Java's Guava `Hashing.murmur3_32(104729)`).
   - XORs the hashes together.
4. Maps to shard: `(unsigned_hash % 65536) * num_shards // 65536`.

This produces identical shard assignments to the Java HoloClient's `ShardUtil` for cross-language compatibility.

---

## 5. Communication Objects Summary

| From → To | Object | Type | Purpose |
|-----------|--------|------|---------|
| User → HoloClient | `Put` (wrapping `Record`) | Data | The record to insert/delete |
| HoloClient → ActionCollector | `Record` | Data | Dispatched by table name |
| ActionCollector → TableShardCollector | `Record` | Data | Passed through to shard routing |
| TableShardCollector → RecordCollector | `Record` | Data | Routed by distribution key hash |
| RecordCollector → `_do_flush()` | `(deletes, inserts)` lists | Data | Extracted via `get_records()` |
| `_do_flush()` → `_execute_batch()` | `(schema, deletes, inserts)` | Work unit | One batch per table |
| `_execute_batch()` → psycopg | SQL string + params | SQL | Via `cursor.execute()` |
| HoloClient → Worker (multi-process) | `ShmRingBuffer` | IPC | SPSC ring buffer in shared memory |
| Worker → HoloClient (multi-process) | `multiprocessing.Queue` | IPC | Acks and errors |
| HoloClient internal | `threading.Lock` (`_collector_lock`) | Mutual exclusion | Separates appends from flushes |
| Background thread → HoloClient | `_bg_exception` attribute | Error propagation | Deferred exception from background flush |

---

## 6. The Flush Guarantee

When `HoloClient.flush()` is called, the guarantee that **all prior puts are fully executed** is provided by:

### Single-Process Path

1. `_do_flush(force=True)` acquires `_collector_lock` — no new records can enter during drain.
2. `get_flushable_tables(force=True)` returns ALL non-empty tables.
3. `get_records()` drains ALL shards for each table, clearing the buffers.
4. SQL execution happens synchronously on the calling thread — `_execute_with_retry()` blocks until each `cursor.execute()` completes.
5. When `flush()` returns, every record has been submitted to the database.

```
flush()
  → _do_flush(force=True)
    → Lock _collector_lock
    → Drain ALL records from ALL tables/shards
    → Unlock _collector_lock
    → For each batch: _execute_batch() → cursor.execute() [blocking]
  → Return  ✓ (all records committed)
```

### Multi-Process Path

1. Sends `MSG_FLUSH` with a monotonic `ack_id` to each worker's shared memory buffer.
2. Each worker drains its own `ActionCollector` (force=True), executes all SQL, then sends `('flush_ack', ack_id, worker_id)`.
3. `_flush_workers()` blocks until all workers acknowledge (60s timeout).
4. When `flush()` returns, every worker has confirmed all records are committed.

---

## 7. Connection Management

Four connection types, all lazily created:

| Connection | Purpose | Endpoint | Idle Timeout |
|-----------|---------|----------|-------------|
| `_write_conn` | INSERT/DELETE | Fixed FE (if enabled) or regular FE | `connection_max_idle_ms` (60s) |
| `_read_conn` | GET/SCAN queries | Fixed FE (if enabled) or regular FE | `connection_max_idle_ms` (60s) |
| `_meta_conn` | Schema loading | Always regular FE | None |
| `_sql_conn` | Raw SQL, COPY | Always regular FE | None |

Write and read connections use double-checked locking for thread safety. When `enable_generate_binlog` is disabled (default), the write connection sets `hg_experimental_generate_binlog = off` on creation.

---

## 8. Key Differences from Java holo-client

| Aspect | Java holo-client | Python holo-client-py |
|--------|-----------------|----------------------|
| **Concurrency model** | Thread pool with N Workers, each owning a JDBC connection | Sync: single connection (parallelism=1) or N worker processes via shm (parallelism>1); Async: N writer tasks with N connections |
| **Shard count** | `writeThreadSize` shards per table → fan-out to worker threads | 1 shard per table within each worker; routing to workers at client level via `compute_shard()` |
| **Execution model** | Async pipeline: `TableShardCollector` → `PutAction` → `Worker` thread via `ObjectChan` | Sync: `put()` may flush inline or route to worker process; Async: `await put()` appends to sharded collector, flush via asyncio tasks |
| **Completion signaling** | `CompletableFuture<Void>` on `PutAction`, per-record futures for `putAsync()` | No `PutAction` object; `_bg_exception` for deferred error propagation (sync); direct exception raising (async) |
| **Concurrency control** | `ReentrantReadWriteLock` (concurrent appends, exclusive flush) + `Semaphore` (in-flight limit) | Sync: `threading.Lock`; Async: `asyncio.Lock` per writer |
| **Single-slot pipeline** | Each `TableShardCollector` has 1 buffer + 1 in-flight `PutAction` | No in-flight action concept; buffer is drained and executed synchronously |
| **Background flush** | `BackgroundJob` in `ExecutionPool` (1s loop) | Sync: `holo-bg-flush` daemon thread; Async: N `_bg_loop_writer` asyncio tasks |
| **Batch heuristics** | Power-of-2 early commit at 40%+ threshold | Simple threshold checks (count, bytes, time) |
| **Multi-connection writes** | N worker threads × N JDBC connections | Sync multi-process: N processes × N connections via shared memory IPC; Async: N asyncio tasks × N `AsyncConnection` |
| **SQL driver** | JDBC `PreparedStatement` | psycopg3 `cursor.execute(prepare=True)` |
| **Async write** | Same pipeline as sync (workers handle both) | Dedicated `AsyncHoloClient` with multi-writer tasks (`write_parallelism` controls task count) |
