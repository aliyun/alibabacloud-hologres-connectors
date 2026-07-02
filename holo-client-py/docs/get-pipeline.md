# holo-client-py Get Pipeline: Architecture and Internals

This document walks through how `get()`, `get_many()`, and `scan()` calls in holo-client-py flow from the user-facing API to SQL execution on Hologres.

---

## 1. High-Level Architecture

holo-client-py provides read APIs on both sync (`HoloClient`) and async (`AsyncHoloClient`) clients.

### Sync Get — `HoloClient.get()` / `get_many()`

All sync get operations use reader threads with `queue.Queue` for batching. Each reader thread has its own `psycopg.Connection` and receives get requests as Python objects directly (no serialization).

```
User Thread                          Reader Thread (× read_parallelism)
──────────                           ──────────────────────────────────

HoloClient.get(Get)
      │
      ▼
Create concurrent.futures.Future
      │
      ▼
compute_shard(get.record, N) → reader_idx
      │
      ▼
queue.Queue[reader_idx].put((schema, sorted_sel, pk_values, future))
      ║ (in-process queue)             _reader_worker_thread()
      ║                                      │
      ╚══════════════════════════════════▶ Accumulate items up to read_batch_size
                                              │
                                              ▼
                                       Group by (schema, selected_columns)
                                              │
                                              ▼
                                       _execute_get_batch(conn, ...)
                                              │
                                              ▼
                                       cursor.execute(sql, params, prepare=True)
                                              │
                                              ▼
                                       Build Record (fast construction)
                                              │
                                              ▼
                                       future.set_result(Record or None)
      │
      ▼
future.result() returns → caller unblocks
```

Thread-safe — multiple threads may share one `HoloClient` for reads (queue.Queue is thread-safe, and futures are resolved directly by reader threads).

### Async Get — `AsyncHoloClient.get()` — Queue-Based Batching

```
Caller Coroutine                   Reader Tasks (× read_parallelism)
────────────────                   ──────────────────────────────────

await client.get(Get)              _reader_task() (asyncio.Task)
      │                                  │
      ▼                                  │  same asyncio event loop
Create asyncio.Future                    │
Attach to get.future                     ▼
Set get.submit_ns                  await _get_queue.get() → first item
      │                            get_nowait() → drain up to read_batch_size
      ▼                                  │
_get_queue.put(get)  ════════════▶       ▼
      │                            Check queue-wait timeout → fail expired futures
      ▼                                  │
await future                             ▼
                                   Group by TableName
                                         │
                                         ▼
                                   _execute_get_batch(conn, gets)
                                         │
                                         ▼
                                   async cursor.execute(sql, params, prepare=True)
                                         │
                                         ▼
                                   Map rows → Records by PK
                                         │
                                         ▼
                                   future.set_result(Optional[Record])
```

Key design: multiple concurrent `await client.get()` calls naturally queue up, and reader tasks batch them into efficient multi-PK SQL queries.

### Scan — `HoloClient.scan()` / `AsyncHoloClient.scan()`

```
HoloClient.scan(Scan) / await AsyncHoloClient.scan(Scan)
      │
      ▼
Set GUC: hg_experimental_enable_fixed_dispatcher_for_scan = on  (once)
      │
      ▼
build_scan_sql(scan) → (sql, params)
      │  EqualsFilter → col = %s
      │  RangeFilter  → col >= %s AND col < %s
      │  SortKeys     → ORDER BY pk / ck / (none)
      ▼
cursor.execute(sql, params, prepare=True)
      │
      ▼
Convert rows → List[Record]
```

Scans are not batched — each scan is an independent query. Both sync and async scan use direct execution on a single connection.

### The Pipeline Classes

| Level | Class | Key Responsibility |
|-------|-------|-------------------|
| 1 | `HoloClient` | Sync API: `get()`, `get_many()`, `scan()` — reader threads via queue |
| 1 | `AsyncHoloClient` | Async API: `get()`, `get_many()`, `scan()` — queue-based get batching |
| 2 | `Get` | Point-query request: holds PK values + column projection |
| 3 | `Scan` | Filter-based query: holds filters, projection, sort order |
| 4 | `_sql.py` | SQL generation (`build_scan_sql`) |
| 5 | `_reader_worker_thread.py` | Reader worker thread: batches gets from queue, executes SQL, resolves futures |
| 6 | `psycopg` | SQL execution via `Connection` (sync) or `AsyncConnection` (async) |

---

## 2. Detailed Class Roles

### 2.1 Get

**File:** `hologres/get.py`

**Role:** Represents a point-query by primary key.

**Key fields (`__slots__`):**
```python
_record: Record                  # holds PK values
future: Optional[Any]            # Future set by client (concurrent.futures or asyncio)
full_column: bool                # True = return all columns
_selected_columns: set[int]      # column indices for projection
submit_ns: int                   # timestamp for timeout tracking
```

**Construction — two patterns:**

```python
# Direct construction
get = Get(schema)
get.set_primary_key("id", 1)
record = client.get(get)

# Builder pattern (validates all PKs are set)
get = (Get.builder(schema)
       .set_primary_key("id", 1)
       .with_selected_column("name")
       .build())
```

---

### 2.2 HoloClient.get() and get_many()

**File:** `hologres/client.py`

**Role:** Synchronous point-query with reader-thread-based batching.

**`get(Get) → Optional[Record]`:**
Creates a `concurrent.futures.Future`, calls `_submit_get()`, blocks on `future.result()`.

**`async_get(Get) → concurrent.futures.Future`:**
Same pipeline as `get()` but non-blocking — the caller can submit many gets before blocking on any Future.

**`get_many(List[Get]) → List[Optional[Record]]`:**
Creates a Future per Get via `async_get()`, returns `[f.result() for f in futures]`.

**`_submit_get(get, future)`:**
1. Computes `reader_idx = compute_shard(get.record, read_parallelism)`.
2. Builds `sorted_sel` = union of PK indices and selected columns.
3. Extracts PK values from the record.
4. Puts `(schema, sorted_sel, pk_values, future)` on the reader's queue.

The Future is completed directly by the reader thread when the SQL result is available.

---

### 2.3 AsyncHoloClient.get() — Queue-Based Batching

**File:** `hologres/async_client.py`

**Role:** Async point query with queue-based batching. Multiple concurrent `get()` calls are batched by background reader tasks for efficient execution.

**Entry point — `get(Get) → Optional[Record]`:**
```python
async def get(self, get: Get) -> Optional[Record]:
    await self._start_reader_tasks()
    future = loop.create_future()
    get.future = future
    get.submit_ns = time.monotonic_ns()
    await self._get_queue.put(get)
    return await future
```

**Reader task lifecycle — `_reader_task(reader_id)`:**
1. **Await first item** — `await _get_queue.get()`.
2. **Non-blocking drain** up to `read_batch_size - 1` more items.
3. **Timeout check** — Gets that have waited too long get their futures failed.
4. **Group by table** and execute via `_execute_get_batch()`.
5. Complete futures with results.

---

### 2.4 Scan

**File:** `hologres/scan.py` (data classes), `hologres/_sql.py` (SQL generation)

Both sync and async scan use direct execution on a single connection (no batching, no retry).

---

## 3. Reader Thread Details

### 3.1 Reader Thread Startup

`_start_reader_threads()` spawns N daemon threads:

1. Creates N `queue.Queue` objects (bounded by `read_batch_queue_size`).
2. Spawns N `threading.Thread` instances running `_reader_worker_thread`.
3. Each thread connects to the database with its own `psycopg.Connection`.

### 3.2 Reader Worker: Accumulation Loop

**File:** `hologres/_reader_worker_thread.py`

Each reader thread:
1. Creates its own `psycopg.Connection` (with `autocommit=True`).
2. Sets `statement_timeout` if configured (and not using Fixed FE).
3. Maintains a SQL cache keyed by `(schema_name, table_name, selected_columns, batch_size)`.
4. Main loop:
   - **Wait for first item** from the queue (0.1s timeout, checks stop event).
   - **Non-blocking drain** up to `batch_size - 1` more items from the queue.
   - **Group by (schema, selected_columns)** for efficient SQL batching.
   - **Execute batched SQL** via `_execute_get_batch()` with `prepare=True`.
   - **Resolve futures** directly with fast Record construction (see §3.3).
5. On DB error: fails all futures in the batch, attempts reconnection.
6. On sentinel (None): exits cleanly.

### 3.3 Fast Record Construction

Reader threads bypass `Record.__init__()` and `set_object()` to avoid per-column overhead (isinstance checks, bounds checking, size estimation). Instead:

```python
rec = Record.__new__(Record)
rec.schema = schema
rec.table_name = table_name_obj
values = [None] * n_cols
for col_pos, sel_idx in enumerate(sorted_sel):
    values[sel_idx] = row[col_pos]
rec.values = values
rec._set_columns = set_cols
rec._only_insert_columns = frozenset()
rec.type = MutationType.INSERT
rec.byte_size = 0
rec._futures = []
future.set_result(rec)
```

This eliminates the `set_object()` overhead that was the primary bottleneck in read-heavy workloads.

### 3.4 SQL Template

Batched gets use OR-chain WHERE clauses:

```sql
SELECT col1, col2, ... FROM schema.table
WHERE (pk1=%s AND pk2=%s) OR (pk1=%s AND pk2=%s) OR ...
```

SQL strings are cached per `(schema_name, table_name, selected_columns, batch_size)` and executed with `prepare=True` for server-side prepared statement reuse.

---

## 4. Connection Management

| Connection | Purpose | Client | Lifetime |
|-----------|---------|--------|----------|
| Reader thread connections | `HoloClient.get()` batching | Sync (per reader thread) | Thread lifetime |
| `_read_conn` | `HoloClient.scan()`, `sql()` | Sync | Idle-timeout managed |
| Reader task connections | `AsyncHoloClient.get()` batching | Async (per reader task) | Per-task |
| `_read_conn` | `AsyncHoloClient.scan()` | Async | Client lifetime |

---

## 5. Configuration Options

| Setting | Default | Effect on Reads |
|---------|---------|----------------|
| `read_batch_size` | 128 | Max gets per SQL batch (both sync reader threads and async reader tasks) |
| `read_batch_queue_size` | 256 | Max pending gets per reader queue (sync) / total async queue size |
| `read_parallelism` | 4 | Number of reader threads (sync) / reader tasks (async) |
| `read_timeout_ms` | 0 (disabled) | Server-side `statement_timeout`; queue-wait timeout for async get |
| `use_fixed_fe` | False | Routes to Fixed FE; blocks `timestamptz` columns |

---

## 6. Get vs. Scan Comparison

| Aspect | Get (point query) | Scan (filter query) |
|--------|-------------------|---------------------|
| **Lookup key** | Primary key values (required) | Equality/range filters on any column |
| **Result cardinality** | 0 or 1 record per Get | Multiple records |
| **SQL shape** | `WHERE (pk=?) OR (pk=?)` | `WHERE col=? AND col>=? ORDER BY ...` |
| **Batching** | Multiple PKs batched by reader (up to `read_batch_size`) | Single query per scan |
| **Sync API** | Reader threads via queue.Queue | Direct execution on `_read_conn` |
| **Async API** | Queue-based batching with N reader tasks | Direct execution on `_read_conn` |
| **SQL template caching** | Yes (by column set + batch size) | No |

---

## 7. Key Differences from Java holo-client

| Aspect | Java holo-client | Python holo-client-py |
|--------|-----------------|----------------------|
| **Sync get** | `get()` → `ArrayBlockingQueue` → single `ActionWatcher` thread drains up to `readBatchSize` → dispatches `GetAction` to worker threads | `get()` → `queue.Queue` (per reader) → reader thread accumulates up to `read_batch_size` → executes SQL directly |
| **Async get** | Same as sync (Java has no async distinction) | `asyncio.Queue` → N reader tasks drain up to `read_batch_size` → execute SQL |
| **Batching consumer** | Single `ActionWatcher` thread (1 per client) | N reader threads (sync) or N reader tasks (async) |
| **Connection model** | Shared connection pool, bounded by `readSemaphore` | One connection per reader thread/task |
| **SQL caching** | JDBC `PreparedStatement` | Dict with rendered SQL strings + psycopg `prepare=True` |
| **Thread safety** | Thread-safe (queue-based) | Thread-safe (queue.Queue + futures for sync; asyncio for async) |
| **IPC mechanism** | In-process queues (Java threads share heap) | In-process queues (Python threads share heap, GIL released during I/O) |
