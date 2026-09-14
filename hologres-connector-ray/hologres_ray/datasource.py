import logging
from typing import Any, Callable, Dict, Iterable, List, Optional

import pyarrow as pa
from psycopg import sql

from hologres import HoloClient, HoloConfig

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)


class HologresReadTask(ReadTask):
    def __init__(
        self,
        execute_fn: Callable[[], Iterable[Block]],
        metadata: BlockMetadata,
        shard_list: List[int],
        per_task_row_limit=None,
    ):
        super().__init__(
            read_fn=execute_fn, metadata=metadata, per_task_row_limit=per_task_row_limit
        )
        self._shard_list = shard_list

    def get_shard_list(self) -> List[int]:
        return self._shard_list


class HologresDatasource(Datasource):
    def __init__(
        self,
        table: str,
        connection_uri: str,
        schema: Optional[str] = "public",
        columns: Optional[List[str]] = None,
        connection_options: Optional[Dict[str, Any]] = None,
        where_filter: Optional[str] = None,
        read_mode: str = "copy_to",
        is_compressed: bool = True,
    ):
        self._schema = schema
        self._table = table
        self._connection_uri = connection_uri
        self._columns = columns
        self._connection_options = connection_options or {}
        self._where_filter = where_filter
        self._read_mode = read_mode
        self._is_compressed = is_compressed

        self._config = HoloConfig.from_connection_uri(connection_uri)

        if not self._table_exists():
            raise ValueError(f"Table {schema}.{table} does not exist in the database.")

        self._query = self._generate_query()

    def _table_exists(self) -> bool:
        with HoloClient(self._config) as client:
            return client.table_exists(self._table, self._schema)

    def _generate_query(self) -> str:
        select_clause = (
            sql.SQL(", ").join(map(sql.Identifier, self._columns))
            if self._columns
            else sql.SQL("*")
        )
        query = sql.SQL("SELECT {0} FROM {1}.{2}").format(
            select_clause, sql.Identifier(self._schema), sql.Identifier(self._table)
        )
        query_str = query.as_string()

        if self._where_filter:
            query_str += f" WHERE {self._where_filter}"

        return query_str

    def _build_read_sql(self, shard_list: List[int], original_query: str) -> str:
        if shard_list is None or len(shard_list) == 0:
            return original_query

        in_clause = f"hg_shard_id IN ({', '.join(map(str, shard_list))})"

        if "WHERE" in original_query.upper():
            return original_query + f" AND {in_clause}"
        else:
            return original_query + f" WHERE {in_clause}"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[Any] = None,
    ) -> List[ReadTask]:
        num_shards = self._get_shard_count()
        if num_shards is None or num_shards == 0:
            logger.warning(
                "Could not determine shard count, falling back to single task"
            )
            return [
                ReadTask(
                    lambda: [self._execute_read_sql(self._query)],
                    BlockMetadata(
                        num_rows=None,
                        size_bytes=None,
                        exec_stats=None,
                        input_files=None,
                    ),
                    per_task_row_limit=per_task_row_limit,
                )
            ]

        actual_parallelism = min(parallelism, num_shards)
        shards_per_task = num_shards // actual_parallelism
        extra_shards = num_shards % actual_parallelism

        def _get_read_task(shard_list: List[int]) -> ReadTask:
            return HologresReadTask(
                lambda sql=self._build_read_sql(shard_list, self._query): [
                    self._execute_read_sql(sql)
                ],
                BlockMetadata(
                    num_rows=None,
                    size_bytes=None,
                    exec_stats=None,
                    input_files=None,
                ),
                shard_list,
                per_task_row_limit=per_task_row_limit,
            )

        read_tasks = []
        shard_start = 0
        for i in range(actual_parallelism):
            shard_count_for_task = shards_per_task
            if i < extra_shards:
                shard_count_for_task += 1
            shard_list = list(range(shard_start, shard_start + shard_count_for_task))
            read_tasks.append(_get_read_task(shard_list))
            shard_start += shard_count_for_task

        return read_tasks

    def _get_shard_count(self) -> Optional[int]:
        try:
            with HoloClient(self._config) as client:
                table_name = f"{self._schema}.{self._table}"
                schema = client.get_table_schema(table_name)
                return schema.shard_count if schema.shard_count > 0 else None
        except Exception as e:
            logger.warning(f"Failed to get shard count from Hologres: {e}")
        return None

    def _execute_read_sql(self, query: str) -> Block:
        if self._read_mode == "copy_to":
            return self._execute_copy_to(query)
        else:
            return self._execute_select(query)

    def _execute_select(self, query: str) -> Block:
        import pandas as pd
        import psycopg
        from psycopg.rows import dict_row

        conn = psycopg.connect(self._config.conninfo, **self._connection_options)
        try:
            with conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

                if rows:
                    return pa.Table.from_pandas(pd.DataFrame(rows))
                names = [col.name for col in cursor.description or ()]
                return pa.table({name: pa.nulls(0) for name in names})
        except Exception as e:
            raise RuntimeError(f"Failed to execute block query: {e}")
        finally:
            conn.close()

    def _execute_copy_to(self, query: str) -> Block:
        try:
            with HoloClient(self._config) as client:
                fmt = "arrow_lz4" if self._is_compressed else "arrow"
                return client.copy_to(query, format=fmt)
        except Exception as e:
            raise RuntimeError(f"Failed to execute copy_to query: {e}")
