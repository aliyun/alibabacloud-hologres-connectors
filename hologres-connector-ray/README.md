# Hologres Connector for Ray

[Ray Data](https://docs.ray.io/en/latest/data/data.html) connector for Hologres,
providing high-performance parallel read and write between Ray Datasets and
Hologres tables.

- **Read** — shard-parallel reads with Arrow (LZ4) bulk export via `COPY ... TO STDOUT`,
  column projection and WHERE pushdown.
- **Write** — parallel `COPY` bulk-load / stream / internal-stage, text and
  binary formats, on-conflict `UPDATE` / `IGNORE`, and automatic
  reshuffle-by-shard for tables with primary keys.

It is built on top of the [`hologres-client`](../holo-client-py) Python package.

## Installation

```bash
pip install hologres-connector-ray
```

Or install from source (editable), which also pulls in the local `hologres-client`:

```bash
pip install -e ./holo-client-py
pip install -e ./hologres-connector-ray
```

Requirements: Python >= 3.8, `ray[data] >= 2.50`, `pyarrow >= 12.0`, `lz4`.

## Usage

```python
import ray
from hologres_ray import read_hologres, write_hologres, OnConflictAction, WriteMode

CONN = "postgresql://user:password@host:port/database"

# Read a table into a Ray Dataset
ds = read_hologres(
    table_name="my_table",
    connection_uri=CONN,
    schema_name="public",
    columns=["id", "name", "value"],   # optional; defaults to all columns
    where_filter="value > 100",        # optional; SQL without the WHERE keyword
    read_mode="copy_to",               # "copy_to" (Arrow bulk) or "select"
    is_compressed=True,                 # LZ4 for copy_to mode
)

# Write a Ray Dataset to a table
write_hologres(
    ds,
    table_name="my_table",
    connection_uri=CONN,
    schema_name="public",
    write_mode=WriteMode.COPY_BULK_LOAD,      # or COPY_STREAM / COPY_STAGE
    on_conflict_action=OnConflictAction.UPDATE,  # UPDATE / IGNORE / NONE
    format="text",                             # "text" or "binary"
    enable_serverless_computing=False,
)
```

For tables with a primary key in `COPY_BULK_LOAD` mode, data is automatically
reshuffled by shard so each write task targets a distinct set of shards, enabling
parallel bulk load with reduced lock contention.

## Parameters

### `read_hologres`

All arguments are keyword-only. Returns a `ray.data.Dataset`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `table_name` | `str` | *required* | Name of the table to read from. Must already exist, otherwise a `ValueError` is raised. |
| `connection_uri` | `str` | *required* | PostgreSQL connection URI, e.g. `postgresql://user:pass@host:port/database`. |
| `schema_name` | `str` | `"public"` | Schema the table lives in. |
| `columns` | `List[str]` \| `None` | `None` | Columns to project. `None` selects all columns. |
| `connection_options` | `Dict[str, Any]` \| `None` | `None` | Extra kwargs passed to `psycopg.connect`, e.g. `{"connect_timeout": 10}`. |
| `where_filter` | `str` \| `None` | `None` | SQL filter pushed down to the server, written **without** the `WHERE` keyword, e.g. `"value > 100"`. |
| `read_mode` | `str` | `"copy_to"` | `"copy_to"` uses `COPY ... TO STDOUT` with Arrow output (fast bulk path). Any other value, e.g. `"select"`, falls back to a plain `SELECT` fetched through psycopg. |
| `is_compressed` | `bool` | `True` | Use LZ4-compressed Arrow (`arrow_lz4`) instead of plain `arrow`. Only affects `read_mode="copy_to"`. |
| `ray_remote_args` | `Dict[str, Any]` \| `None` | `None` | kwargs forwarded to `ray.remote` for the read tasks, e.g. `{"num_cpus": 2}`. |
| `concurrency` | `int` \| `None` | `None` | Maximum number of read tasks running concurrently. |
| `override_num_blocks` | `int` \| `None` | `None` | Override the number of output blocks. Read parallelism is capped by the table's shard count regardless. |

Read tasks are split by `hg_shard_id`: the connector queries the table's shard
count and distributes shards evenly across `min(parallelism, shard_count)`
tasks. If the shard count cannot be determined, it falls back to a single task.

### `write_hologres`

`dataset` is positional; everything else is keyword-only. Returns `None`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dataset` | `ray.data.Dataset` | *required* | The dataset to write. |
| `table_name` | `str` | *required* | Target table name. |
| `connection_uri` | `str` | *required* | PostgreSQL connection URI. |
| `schema_name` | `str` | `"public"` | Schema the target table lives in. |
| `connection_options` | `Dict[str, Any]` \| `None` | `None` | Extra kwargs passed to `psycopg.connect`. |
| `on_conflict_action` | `OnConflictAction` \| `None` | `None` | Behaviour on conflict, only meaningful for tables with a primary key:<ul><li>`UPDATE` — replace the existing row</li><li>`IGNORE` — keep the existing row, drop the incoming one</li><li>`NONE` — no conflict handling; a conflicting row errors out</li><li>unset — behaves like `NONE`</li></ul>In `COPY_STAGE` mode on a table with a primary key, `NONE`/unset behaves like `UPDATE`: the stage load always emits an `ON CONFLICT` clause, so "error on conflict" is not available. |
| `write_mode` | `WriteMode` | `WriteMode.COPY_BULK_LOAD` | <ul><li>`COPY_BULK_LOAD` — for tables with a primary key the dataset is reshuffled by shard first so each task targets a distinct shard set</li><li>`COPY_STREAM` — streaming COPY; no reshuffle, and no minimum Hologres version for on-conflict handling</li><li>`COPY_STAGE` — each task uploads Arrow IPC to one shared internal stage, then a single `INSERT...SELECT` loads the whole stage after all tasks finish. No reshuffle. Data lands twice (stage, then table), so it is slower than the COPY modes for a plain one-shot load. Requires Hologres >= 4.1.0</li></ul> |
| `format` | `str` | `"text"` | COPY wire format: `"text"` or `"binary"`. Any other value raises `ValueError`. `"binary"` skips server-side text parsing but requires loading column type info first. Ignored by `COPY_STAGE`, which always uploads Arrow IPC. |
| `enable_serverless_computing` | `bool` | `False` | Run the load with `hg_computing_resource = 'serverless'`. Only applied in `COPY_BULK_LOAD` mode. |
| `ray_remote_args` | `Dict[str, Any]` \| `None` | `None` | kwargs forwarded to `ray.remote` for the write tasks. |
| `concurrency` | `int` \| `None` | `None` | Maximum number of write tasks running concurrently. |
