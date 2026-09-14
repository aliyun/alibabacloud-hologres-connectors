import logging
import uuid
from contextlib import ExitStack
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa
import psycopg

from hologres import CopyStageWriter, HoloClient, HoloConfig
from hologres import OnConflictAction as HoloOnConflictAction
from hologres.copy import (
    _build_copy_sql,
    _encode_binary_header,
    _encode_binary_row,
    _load_column_type_info,
    CopyFormat,
    CopyMode,
)
from hologres.table_name import TableName

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteReturnType

logger = logging.getLogger(__name__)


class OnConflictAction(Enum):
    NONE = 1
    UPDATE = 2
    IGNORE = 3


class WriteMode(Enum):
    COPY_BULK_LOAD = 1
    COPY_STREAM = 2
    COPY_STAGE = 3


def _to_holo_on_conflict(action: OnConflictAction) -> HoloOnConflictAction:
    if action == OnConflictAction.UPDATE:
        return HoloOnConflictAction.INSERT_OR_UPDATE
    elif action == OnConflictAction.IGNORE:
        return HoloOnConflictAction.INSERT_OR_IGNORE
    return HoloOnConflictAction.INSERT_OR_REPLACE


def _to_copy_mode(mode: WriteMode) -> CopyMode:
    if mode == WriteMode.COPY_STREAM:
        return CopyMode.STREAM
    return CopyMode.BULK_LOAD


class HologresDatasink(Datasink):
    NAME = "Hologres"

    def __init__(
        self,
        schema_name: str,
        table_name: str,
        connection_uri: str,
        on_conflict_action: OnConflictAction,
        write_mode: WriteMode = WriteMode.COPY_BULK_LOAD,
        format: str = "text",
        enable_serverless_computing: bool = False,
        connection_options: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
    ) -> None:
        self._schema_name = schema_name
        self._table_name = table_name
        self._connection_uri = connection_uri
        self._on_conflict_action = on_conflict_action
        self._write_mode = write_mode
        if format not in ("text", "binary"):
            raise ValueError("Format must be either 'text' or 'binary'")
        self._format = format
        self._enable_serverless_computing = enable_serverless_computing
        self._connection_options = connection_options or {}

        self._config = HoloConfig.from_connection_uri(connection_uri)

        # One stage shared by every write task; the name is generated here on the
        # driver so all tasks receive it when the datasink is serialized to them.
        self._stage_name: Optional[str] = None
        if write_mode == WriteMode.COPY_STAGE:
            self._stage_name = f"ray_{schema_name}_{table_name}_{uuid.uuid4().hex[:8]}"[
                :63
            ]
        # Columns the tasks upload, and therefore what the commit must select.
        # Resolved on the driver from the dataset schema so that tasks and the
        # coordinator agree on the exact list and its order.
        self._commit_columns: Optional[List[str]] = list(columns) if columns else None

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    def _execute_copy_from(
        self,
        arrow_table: pa.Table,
        has_pk: bool,
        column_type_oids: Dict[str, int],
        column_type_names: Dict[str, str],
        holo_version: tuple,
        shard_id_str: str,
    ):
        column_names = arrow_table.column_names
        if not column_names:
            raise ValueError("Arrow table has no columns")

        table_name_obj = TableName(self._schema_name, self._table_name)
        holo_on_conflict = _to_holo_on_conflict(self._on_conflict_action)
        copy_mode = _to_copy_mode(self._write_mode)

        if has_pk and self._on_conflict_action != OnConflictAction.NONE:
            if copy_mode == CopyMode.BULK_LOAD:
                if holo_version < (3, 1, 0):
                    raise ValueError(
                        "Cannot use COPY_BULK_LOAD mode and specify on_conflict "
                        "action with hologres version < 3.1.0, consider using "
                        "COPY_STREAM mode or upgrade hologres version"
                    )
                copy_mode = CopyMode.BULK_LOAD_ON_CONFLICT

        copy_fmt = CopyFormat.BINARY if self._format == "binary" else CopyFormat.TEXT

        copy_sql = _build_copy_sql(
            table_name=table_name_obj,
            columns=column_names,
            fmt=copy_fmt,
            mode=copy_mode,
            on_conflict=holo_on_conflict,
            has_pk=has_pk,
        )

        conn = psycopg.connect(self._config.conninfo, **self._connection_options)
        try:
            with conn.cursor() as cursor:
                if shard_id_str:
                    cursor.execute(
                        f"SET hg_experimental_target_shard_list = '{shard_id_str}'"
                    )
                if (
                    self._write_mode == WriteMode.COPY_BULK_LOAD
                    and self._enable_serverless_computing
                ):
                    cursor.execute("SET hg_computing_resource = 'serverless'")

                if self._format == "text":
                    with cursor.copy(copy_sql) as copy:
                        for batch in arrow_table.to_batches():
                            rows = zip(*batch.to_pydict().values())
                            for row in rows:
                                copy.write_row(row)
                else:
                    with cursor.copy(copy_sql) as copy:
                        copy.write(_encode_binary_header())
                        for batch in arrow_table.to_batches():
                            rows = zip(*batch.to_pydict().values())
                            for row in rows:
                                binary_row = _encode_binary_row(
                                    row,
                                    column_names,
                                    column_type_oids,
                                    column_type_names,
                                    cursor,
                                )
                                copy.write(binary_row)
        finally:
            conn.close()

    def _new_stage_writer(self, columns: Optional[List[str]]) -> CopyStageWriter:
        """Build a stage writer that uploads without committing.

        The commit is a single INSERT...SELECT over the whole shared stage, so
        letting each task commit would make every task re-read the other tasks'
        files.  The coordinator commits once in on_write_complete() instead.
        """
        with HoloClient(self._config) as client:
            schema = client.get_table_schema(f"{self._schema_name}.{self._table_name}")
        return CopyStageWriter(
            config=self._config,
            schema=schema,
            stage_name=self._stage_name,
            on_conflict=_to_holo_on_conflict(self._on_conflict_action),
            columns=columns,
            auto_commit=False,
        )

    def on_write_start(self, schema: Optional["pa.Schema"] = None) -> None:
        if self._write_mode != WriteMode.COPY_STAGE:
            return

        with HoloClient(self._config) as client:
            holo_version = client.get_holo_version()
            if holo_version < (4, 1, 0):
                raise ValueError(
                    f"COPY_STAGE requires Hologres >= 4.1.0, but the instance "
                    f"reports {'.'.join(map(str, holo_version))}. Use "
                    f"COPY_STREAM or COPY_BULK_LOAD instead."
                )
            client.create_stage(self._stage_name)

        # Only released Ray versions that pass a schema reach this; the columns
        # are normally resolved by the caller before the datasink is built.
        if self._commit_columns is None and schema is not None:
            self._commit_columns = list(schema.names)
        logger.info(
            "COPY_STAGE: created stage %s for %s.%s",
            self._stage_name,
            self._schema_name,
            self._table_name,
        )

    def on_write_complete(self, write_result) -> None:
        if self._write_mode != WriteMode.COPY_STAGE:
            return
        try:
            writer = self._new_stage_writer(self._commit_columns)
            writer.commit()
        finally:
            self._drop_stage()

    def on_write_failed(self, error: Exception) -> None:
        if self._write_mode == WriteMode.COPY_STAGE:
            self._drop_stage()

    def _drop_stage(self) -> None:
        """Best-effort stage cleanup; never mask the original failure."""
        try:
            with HoloClient(self._config) as client:
                client.drop_stage(self._stage_name)
        except Exception as e:
            logger.warning("Failed to drop stage %s: %s", self._stage_name, e)

    def _write_stage(self, blocks: Iterable[Block]) -> int:
        """Upload blocks to the shared stage as Arrow IPC files."""
        total = 0
        writer = None
        with ExitStack() as stack:
            for block in blocks:
                accessor = BlockAccessor.for_block(block)
                if accessor.num_rows() == 0:
                    logger.warning("Skipping empty block")
                    continue
                arrow_table = accessor.to_arrow()
                if writer is None:
                    # Prefer the driver-resolved list so the staged files and the
                    # commit's column declaration match exactly; the block's own
                    # columns are the fallback when it could not be resolved.
                    writer = stack.enter_context(
                        self._new_stage_writer(
                            self._commit_columns or list(arrow_table.column_names)
                        )
                    )
                writer.write_table(arrow_table)
                total += arrow_table.num_rows
        return total

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> WriteReturnType:

        if self._write_mode == WriteMode.COPY_STAGE:
            try:
                return [self._write_stage(blocks)]
            except Exception as e:
                logger.error(
                    f"Failed to write block(s) to stage {self._stage_name} for "
                    f"table {self._schema_name}.{self._table_name}: {e}"
                )
                raise

        total_inserted = 0
        try:
            with HoloClient(self._config) as client:
                table_name = f"{self._schema_name}.{self._table_name}"
                schema = client.get_table_schema(table_name)
                has_pk = schema.has_primary_key
                holo_version = client.get_holo_version()

            column_type_oids = {}
            column_type_names = {}
            if self._format == "binary":
                table_name_obj = TableName(self._schema_name, self._table_name)
                conn = psycopg.connect(self._config.conninfo)
                try:
                    column_type_oids, column_type_names = _load_column_type_info(
                        conn, table_name_obj
                    )
                finally:
                    conn.close()

            for block in blocks:
                accessor = BlockAccessor.for_block(block)
                if accessor.num_rows() == 0:
                    logger.warning("Skipping empty block")
                    continue
                arrow_table = accessor.to_arrow()
                total_inserted += arrow_table.num_rows

                shard_id_str = None
                if has_pk and self._write_mode == WriteMode.COPY_BULK_LOAD:
                    shard_id_col = arrow_table.column("__hg_shard_id__")
                    shard_id_str = ",".join([str(s) for s in shard_id_col.unique()])
                    arrow_table = arrow_table.drop(["__hg_shard_id__"])

                self._execute_copy_from(
                    arrow_table,
                    has_pk,
                    column_type_oids,
                    column_type_names,
                    holo_version,
                    shard_id_str,
                )

        except Exception as e:
            logger.error(
                f"Failed to write block(s) to table "
                f"{self._schema_name}.{self._table_name}: {e}"
            )
            raise
        return [total_inserted]

    def get_name(self) -> str:
        return self.NAME
