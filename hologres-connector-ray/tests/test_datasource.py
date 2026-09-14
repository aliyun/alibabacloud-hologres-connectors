from types import SimpleNamespace
from unittest import mock
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from hologres_ray.datasource import HologresDatasource


class TestHologresDatasource:
    @pytest.fixture
    def datasource(self):
        with mock.patch("hologres_ray.datasource.HoloClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.table_exists.return_value = True

            mock_schema = MagicMock()
            mock_schema.shard_count = 50
            mock_client.get_table_schema.return_value = mock_schema

            datasource = HologresDatasource(
                table="table_name",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
                columns=["column1", "column2"],
                connection_options={"connect_timeout": 10},
            )
            return datasource

    def test_init(self, datasource):
        expected_query = 'SELECT "column1", "column2" FROM "public"."table_name"'
        assert datasource._query == expected_query
        assert datasource._schema == "public"
        assert datasource._table == "table_name"

    def test_init_without_schema(self):
        with mock.patch("hologres_ray.datasource.HoloClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.table_exists.return_value = True

            datasource = HologresDatasource(
                table="my_table",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
            )
            expected_query = 'SELECT * FROM "public"."my_table"'
            assert datasource._query == expected_query
            assert datasource._schema == "public"
            assert datasource._table == "my_table"

    def test_init_with_schema(self):
        with mock.patch("hologres_ray.datasource.HoloClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.table_exists.return_value = True

            datasource = HologresDatasource(
                schema="myschema",
                table="my_table",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
            )
            expected_query = 'SELECT * FROM "myschema"."my_table"'
            assert datasource._query == expected_query
            assert datasource._schema == "myschema"
            assert datasource._table == "my_table"

            datasource = HologresDatasource(
                schema="MYSCHEMA",
                table='TABL"E',
                connection_uri="postgresql://user:password@localhost:5432/testdb",
            )
            expected_query = 'SELECT * FROM "MYSCHEMA"."TABL""E"'
            assert datasource._query == expected_query

    def test_generate_query_columns(self, datasource):
        datasource._columns = ["field1"]
        query = datasource._generate_query()
        assert 'SELECT "field1" FROM' in query

        datasource._columns = ["field1", "field2"]
        query = datasource._generate_query()
        assert 'SELECT "field1", "field2" FROM' in query

        datasource._columns = None
        query = datasource._generate_query()
        assert "SELECT * FROM" in query

    def test_generate_query_with_where_filter(self):
        with mock.patch("hologres_ray.datasource.HoloClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.table_exists.return_value = True

            datasource = HologresDatasource(
                table="table_name",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
                columns=["column1", "column2"],
                where_filter="status = 'active'",
            )

            query = datasource._generate_query()
            expected = (
                'SELECT "column1", "column2" FROM "public"."table_name"'
                " WHERE status = 'active'"
            )
            assert query == expected

    def test_build_read_sql_with_shard_list(self, datasource):
        original_query = 'SELECT * FROM "public"."table_name"'
        result = datasource._build_read_sql([0, 1, 2], original_query)

        assert "hg_shard_id IN (0, 1, 2)" in result
        assert 'SELECT * FROM "public"."table_name"' in result
        assert "WHERE" in result

    def test_build_read_sql_with_existing_where_clause(self, datasource):
        original_query = 'SELECT * FROM "public"."table_name" WHERE age > 18'
        result = datasource._build_read_sql([0, 1, 2], original_query)

        assert "hg_shard_id IN (0, 1, 2)" in result
        assert "WHERE age > 18 AND hg_shard_id IN (0, 1, 2)" in result

    def test_get_shard_count(self):
        with mock.patch("hologres_ray.datasource.HoloClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.table_exists.return_value = True

            mock_schema = MagicMock()
            mock_schema.shard_count = 5
            mock_client.get_table_schema.return_value = mock_schema

            datasource = HologresDatasource(
                table="test_table",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
            )
            shard_count = datasource._get_shard_count()
            assert shard_count == 5

    def test_get_read_tasks_with_shards(self):
        with mock.patch("hologres_ray.datasource.HoloClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.table_exists.return_value = True

            mock_schema = MagicMock()
            mock_schema.shard_count = 50
            mock_client.get_table_schema.return_value = mock_schema

            datasource = HologresDatasource(
                table="test_table",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
            )

            batch = pa.record_batch(
                [pa.array([1, 2]), pa.array(["a", "b"])], names=["id", "name"]
            )
            with mock.patch.object(datasource, "_execute_read_sql", return_value=batch):
                read_tasks = datasource.get_read_tasks(parallelism=4)

                assert len(read_tasks) == 4

                total_shard_nums = []
                for task in read_tasks:
                    shard_list = task.get_shard_list()
                    assert shard_list is not None
                    total_shard_nums.extend(shard_list)

                total_shard_nums.sort()
                assert total_shard_nums == list(range(50))

    def test_get_read_tasks_when_no_shards_found(self):
        with mock.patch("hologres_ray.datasource.HoloClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.table_exists.return_value = True

            mock_schema = MagicMock()
            mock_schema.shard_count = 0
            mock_client.get_table_schema.return_value = mock_schema

            datasource = HologresDatasource(
                table="test_table",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
            )

            read_tasks = datasource.get_read_tasks(parallelism=4)
            assert len(read_tasks) == 1

    def test_select_empty_result_keeps_columns(self, datasource):
        datasource._read_mode = "select"

        cursor = MagicMock()
        cursor.fetchall.return_value = []
        cursor.description = [
            SimpleNamespace(name="column1"),
            SimpleNamespace(name="column2"),
        ]
        conn = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with mock.patch("psycopg.connect", return_value=conn):
            block = datasource._execute_read_sql(datasource._query)

        assert block.column_names == ["column1", "column2"]
        assert block.num_rows == 0


class TestReadHologres:
    @mock.patch("hologres_ray.HologresDatasource")
    def test_read_hologres_with_schema_and_table_params(self, mock_datasource_class):
        from hologres_ray import read_hologres

        mock_datasource_instance = MagicMock()
        mock_datasource_class.return_value = mock_datasource_instance

        with mock.patch("ray.data.read_datasource") as mock_read_datasource:
            mock_dataset = MagicMock()
            mock_read_datasource.return_value = mock_dataset

            result = read_hologres(
                schema_name="myschema",
                table_name="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
            )

            mock_datasource_class.assert_called_once_with(
                schema="myschema",
                table="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=None,
                connection_options=None,
                where_filter=None,
                read_mode="copy_to",
                is_compressed=True,
            )
            mock_read_datasource.assert_called_once()
            assert result == mock_dataset

    @mock.patch("hologres_ray.HologresDatasource")
    def test_read_hologres_with_default_schema(self, mock_datasource_class):
        from hologres_ray import read_hologres

        mock_datasource_instance = MagicMock()
        mock_datasource_class.return_value = mock_datasource_instance

        with mock.patch("ray.data.read_datasource") as mock_read_datasource:
            mock_dataset = MagicMock()
            mock_read_datasource.return_value = mock_dataset

            read_hologres(
                table_name="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
            )

            mock_datasource_class.assert_called_once_with(
                schema="public",
                table="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=None,
                connection_options=None,
                where_filter=None,
                read_mode="copy_to",
                is_compressed=True,
            )

    @mock.patch("hologres_ray.HologresDatasource")
    def test_read_hologres_with_where_filter(self, mock_datasource_class):
        from hologres_ray import read_hologres

        mock_datasource_instance = MagicMock()
        mock_datasource_class.return_value = mock_datasource_instance

        with mock.patch("ray.data.read_datasource") as mock_read_datasource:
            mock_dataset = MagicMock()
            mock_read_datasource.return_value = mock_dataset

            read_hologres(
                table_name="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                where_filter="age > 18 AND status = 'active'",
            )

            mock_datasource_class.assert_called_once_with(
                schema="public",
                table="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=None,
                connection_options=None,
                where_filter="age > 18 AND status = 'active'",
                read_mode="copy_to",
                is_compressed=True,
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
