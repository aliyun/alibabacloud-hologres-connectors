"""Ray Data connector for Hologres.

Usage::

    from hologres_ray import read_hologres, write_hologres

    # Read
    ds = read_hologres(
        table_name="my_table",
        connection_uri="postgresql://user:pass@host:port/database",
    )

    # Write
    write_hologres(
        ds,
        table_name="my_table",
        connection_uri="postgresql://user:pass@host:port/database",
    )
"""

from typing import Any, Dict, List, Optional

import ray.data

from ._version import __version__
from .datasink import HologresDatasink, OnConflictAction, WriteMode
from .datasource import HologresDatasource

__all__ = [
    "HologresDatasink",
    "HologresDatasource",
    "OnConflictAction",
    "WriteMode",
    "read_hologres",
    "write_hologres",
    "__version__",
]


def read_hologres(
    *,
    table_name: str,
    connection_uri: str,
    schema_name: str = "public",
    columns: Optional[List[str]] = None,
    connection_options: Optional[Dict[str, Any]] = None,
    where_filter: Optional[str] = None,
    read_mode: str = "copy_to",
    is_compressed: bool = True,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> ray.data.Dataset:
    """Read a Hologres table into a Ray Dataset.

    Args:
        table_name: Name of the table to read from.
        connection_uri: PostgreSQL connection URI
            (e.g. ``postgresql://user:pass@host:port/database``).
        schema_name: Schema name. Defaults to "public".
        columns: Columns to select. Defaults to all.
        connection_options: Extra psycopg connection kwargs.
        where_filter: SQL WHERE condition (without ``WHERE`` keyword).
        read_mode: "copy_to" (default, Arrow bulk read) or "select".
        is_compressed: LZ4 compression for copy_to mode. Defaults to True.
        ray_remote_args: kwargs passed to ``ray.remote`` in read tasks.
        concurrency: Max concurrent Ray read tasks.
        override_num_blocks: Override the number of output blocks.

    Returns:
        A Ray Dataset with the table data.
    """
    datasource = HologresDatasource(
        schema=schema_name,
        table=table_name,
        connection_uri=connection_uri,
        columns=columns,
        connection_options=connection_options,
        where_filter=where_filter,
        read_mode=read_mode,
        is_compressed=is_compressed,
    )
    return ray.data.read_datasource(
        datasource,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )


def write_hologres(
    dataset: ray.data.Dataset,
    *,
    table_name: str,
    connection_uri: str,
    schema_name: str = "public",
    connection_options: Optional[Dict[str, Any]] = None,
    on_conflict_action: OnConflictAction = None,
    write_mode: WriteMode = WriteMode.COPY_BULK_LOAD,
    format: str = "text",
    enable_serverless_computing: bool = False,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
) -> None:
    """Write a Ray Dataset to a Hologres table.

    For tables with primary keys in COPY_BULK_LOAD mode, data is
    automatically reshuffled by shard to enable parallel writes with
    reduced lock contention.

    COPY_STAGE uploads each block to a shared Hologres internal stage as
    Arrow IPC, then loads the whole stage with a single INSERT...SELECT once
    every write task has finished. It needs no reshuffle (the load is
    server-side) and requires Hologres >= 4.1.0.

    Args:
        dataset: The Ray Dataset to write.
        table_name: Target table name.
        connection_uri: PostgreSQL connection URI.
        schema_name: Schema name. Defaults to "public".
        connection_options: Extra psycopg connection kwargs.
        on_conflict_action: Action on PK conflict: UPDATE, IGNORE, or NONE.
        write_mode: COPY_BULK_LOAD (default), COPY_STREAM, or COPY_STAGE.
        format: "text" (default) or "binary" for COPY protocol. Ignored by
            COPY_STAGE, which always uploads Arrow IPC.
        enable_serverless_computing: Use serverless resources for bulk load.
            Only applied in COPY_BULK_LOAD mode.
        ray_remote_args: kwargs passed to ``ray.remote`` in write tasks.
        concurrency: Max concurrent Ray write tasks.
    """
    import pandas as pd

    from hologres import HoloClient, HoloConfig
    from hologres._shard import compute_shard_for_values

    # The stage commit is a single INSERT...SELECT that must declare the columns
    # staged by the tasks, so resolve them here rather than relying on Ray to
    # hand the datasink a schema.
    columns = None
    if write_mode == WriteMode.COPY_STAGE:
        dataset_schema = dataset.schema()
        if dataset_schema is not None:
            columns = list(dataset_schema.names)

    datasink = HologresDatasink(
        schema_name=schema_name,
        table_name=table_name,
        connection_uri=connection_uri,
        connection_options=connection_options,
        on_conflict_action=on_conflict_action,
        write_mode=write_mode,
        format=format,
        enable_serverless_computing=enable_serverless_computing,
        columns=columns,
    )

    if write_mode == WriteMode.COPY_BULK_LOAD:
        config = HoloConfig.from_connection_uri(connection_uri)
        with HoloClient(config) as client:
            full_name = f"{schema_name}.{table_name}"
            table_schema = client.get_table_schema(full_name)

        if table_schema.has_primary_key:
            dk_names = table_schema.distribution_keys
            if not dk_names:
                dk_names = table_schema.primary_keys
            dk_columns = [
                table_schema.get_column(table_schema.get_column_index(n))
                for n in dk_names
            ]
            shard_count = table_schema.shard_count

            def add_shard_id(batch: pd.DataFrame) -> pd.DataFrame:
                if isinstance(batch, dict):
                    batch = pd.DataFrame(batch)
                batch["__hg_shard_id__"] = batch.apply(
                    lambda row: compute_shard_for_values(
                        [row[k] for k in dk_names],
                        dk_columns,
                        shard_count,
                    ),
                    axis=1,
                )
                return batch

            reshuffled = dataset.map_batches(add_shard_id).repartition(
                num_blocks=shard_count, keys=["__hg_shard_id__"]
            )

            reshuffled.write_datasink(
                datasink,
                ray_remote_args=ray_remote_args,
                concurrency=concurrency,
            )
            return

    dataset.write_datasink(
        datasink,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
    )
