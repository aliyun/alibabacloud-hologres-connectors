from unittest import mock
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from hologres_ray.datasink import HologresDatasink, OnConflictAction, WriteMode


class TestHologresDatasink:
    def test_init(self):
        sink = HologresDatasink(
            schema_name="public",
            table_name="test_table",
            connection_uri="postgresql://user:pass@localhost:5432/testdb",
            on_conflict_action=OnConflictAction.NONE,
        )
        assert sink._table_name == "test_table"
        assert sink._connection_uri == "postgresql://user:pass@localhost:5432/testdb"

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Format must be"):
            HologresDatasink(
                schema_name="public",
                table_name="test_table",
                connection_uri="postgresql://user:pass@localhost:5432/testdb",
                on_conflict_action=OnConflictAction.NONE,
                format="invalid",
            )

    @mock.patch("hologres_ray.datasink.psycopg")
    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_write_with_copy_protocol(self, mock_client_cls, mock_psycopg):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_schema = MagicMock()
        mock_schema.has_primary_key = False
        mock_client.get_table_schema.return_value = mock_schema
        mock_client.get_holo_version.return_value = (4, 0, 0)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        sink = HologresDatasink(
            schema_name="public",
            table_name="test_table",
            connection_uri="postgresql://user:pass@localhost:5432/testdb",
            on_conflict_action=OnConflictAction.NONE,
        )

        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        table = pa.Table.from_pandas(df)

        mock_block = MagicMock()
        mock_block_accessor = MagicMock()
        mock_block_accessor.num_rows.return_value = 3
        mock_block_accessor.to_arrow.return_value = table

        with mock.patch(
            "ray.data.block.BlockAccessor.for_block",
            return_value=mock_block_accessor,
        ):
            result = sink.write([mock_block], MagicMock())

            mock_cursor.copy.assert_called()
            assert len(result) == 1
            assert result[0] == 3

    @mock.patch("hologres_ray.datasink.psycopg")
    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_write_with_on_conflict_update(self, mock_client_cls, mock_psycopg):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_schema = MagicMock()
        mock_schema.has_primary_key = False
        mock_client.get_table_schema.return_value = mock_schema
        mock_client.get_holo_version.return_value = (4, 0, 0)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        sink = HologresDatasink(
            schema_name="public",
            table_name="test_table",
            connection_uri="postgresql://user:pass@localhost:5432/testdb",
            on_conflict_action=OnConflictAction.UPDATE,
        )

        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        table = pa.Table.from_pandas(df)

        mock_block = MagicMock()
        mock_block_accessor = MagicMock()
        mock_block_accessor.num_rows.return_value = 3
        mock_block_accessor.to_arrow.return_value = table

        with mock.patch(
            "ray.data.block.BlockAccessor.for_block",
            return_value=mock_block_accessor,
        ):
            result = sink.write([mock_block], MagicMock())

            mock_cursor.copy.assert_called()
            assert len(result) == 1
            assert result[0] == 3

    @mock.patch("hologres_ray.datasink.psycopg")
    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_write_with_on_conflict_ignore(self, mock_client_cls, mock_psycopg):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_schema = MagicMock()
        mock_schema.has_primary_key = False
        mock_client.get_table_schema.return_value = mock_schema
        mock_client.get_holo_version.return_value = (4, 0, 0)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg.connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        sink = HologresDatasink(
            schema_name="public",
            table_name="test_table",
            connection_uri="postgresql://user:pass@localhost:5432/testdb",
            on_conflict_action=OnConflictAction.IGNORE,
        )

        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        table = pa.Table.from_pandas(df)

        mock_block = MagicMock()
        mock_block_accessor = MagicMock()
        mock_block_accessor.num_rows.return_value = 3
        mock_block_accessor.to_arrow.return_value = table

        with mock.patch(
            "ray.data.block.BlockAccessor.for_block",
            return_value=mock_block_accessor,
        ):
            result = sink.write([mock_block], MagicMock())

            mock_cursor.copy.assert_called()
            assert len(result) == 1
            assert result[0] == 3


class TestCopyStageMode:
    def _sink(self, **kwargs):
        return HologresDatasink(
            schema_name="public",
            table_name="test_table",
            connection_uri="postgresql://user:pass@localhost:5432/testdb",
            on_conflict_action=OnConflictAction.UPDATE,
            write_mode=WriteMode.COPY_STAGE,
            **kwargs,
        )

    def test_stage_name_generated_only_for_stage_mode(self):
        assert self._sink()._stage_name.startswith("ray_public_test_table_")

        stream = HologresDatasink(
            schema_name="public",
            table_name="test_table",
            connection_uri="postgresql://user:pass@localhost:5432/testdb",
            on_conflict_action=OnConflictAction.UPDATE,
            write_mode=WriteMode.COPY_STREAM,
        )
        assert stream._stage_name is None

    def test_stage_names_are_unique_per_sink(self):
        assert self._sink()._stage_name != self._sink()._stage_name

    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_on_write_start_rejects_old_hologres(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get_holo_version.return_value = (4, 0, 0)

        with pytest.raises(ValueError, match="COPY_STAGE requires Hologres >= 4.1.0"):
            self._sink().on_write_start(None)
        mock_client.create_stage.assert_not_called()

    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_on_write_start_creates_stage_and_records_columns(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.get_holo_version.return_value = (4, 1, 0)

        sink = self._sink()
        sink.on_write_start(pa.schema([("id", pa.int64()), ("name", pa.string())]))

        mock_client.create_stage.assert_called_once_with(sink._stage_name)
        assert sink._commit_columns == ["id", "name"]

    @mock.patch("hologres_ray.datasink.CopyStageWriter")
    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_write_uploads_without_committing(self, mock_client_cls, mock_writer_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_writer = MagicMock()
        mock_writer_cls.return_value.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer_cls.return_value.__exit__ = MagicMock(return_value=False)

        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        accessor = MagicMock()
        accessor.num_rows.return_value = 3
        accessor.to_arrow.return_value = table

        sink = self._sink()
        with mock.patch(
            "ray.data.block.BlockAccessor.for_block", return_value=accessor
        ):
            result = sink.write([MagicMock()], MagicMock())

        assert result == [3]
        # Tasks must never commit: the coordinator does it once for the whole stage.
        assert mock_writer_cls.call_args.kwargs["auto_commit"] is False
        # Columns come from the block, not the table schema.
        assert mock_writer_cls.call_args.kwargs["columns"] == ["id", "name"]
        mock_writer.write_table.assert_called_once_with(table)
        mock_writer.commit.assert_not_called()

    @mock.patch("hologres_ray.datasink.CopyStageWriter")
    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_write_skips_empty_blocks(self, mock_client_cls, mock_writer_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        accessor = MagicMock()
        accessor.num_rows.return_value = 0

        sink = self._sink()
        with mock.patch(
            "ray.data.block.BlockAccessor.for_block", return_value=accessor
        ):
            assert sink.write([MagicMock()], MagicMock()) == [0]

        # No block means no writer, so no stage file is opened at all.
        mock_writer_cls.assert_not_called()

    @mock.patch("hologres_ray.datasink.CopyStageWriter")
    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_on_write_complete_commits_once_then_drops(
        self, mock_client_cls, mock_writer_cls
    ):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer

        sink = self._sink()
        sink._commit_columns = ["id", "name"]
        sink.on_write_complete(MagicMock())

        mock_writer.commit.assert_called_once()
        mock_client.drop_stage.assert_called_once_with(sink._stage_name)

    @mock.patch("hologres_ray.datasink.CopyStageWriter")
    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_stage_dropped_even_if_commit_fails(self, mock_client_cls, mock_writer_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_writer = MagicMock()
        mock_writer.commit.side_effect = RuntimeError("boom")
        mock_writer_cls.return_value = mock_writer

        sink = self._sink()
        with pytest.raises(RuntimeError, match="boom"):
            sink.on_write_complete(MagicMock())

        mock_client.drop_stage.assert_called_once_with(sink._stage_name)

    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_on_write_failed_drops_stage(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        sink = self._sink()
        sink.on_write_failed(RuntimeError("task died"))

        mock_client.drop_stage.assert_called_once_with(sink._stage_name)

    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_hooks_are_noops_for_non_stage_modes(self, mock_client_cls):
        sink = HologresDatasink(
            schema_name="public",
            table_name="test_table",
            connection_uri="postgresql://user:pass@localhost:5432/testdb",
            on_conflict_action=OnConflictAction.UPDATE,
            write_mode=WriteMode.COPY_STREAM,
        )
        sink.on_write_start(pa.schema([("id", pa.int64())]))
        sink.on_write_complete(MagicMock())
        sink.on_write_failed(RuntimeError("x"))

        mock_client_cls.assert_not_called()

    @mock.patch("hologres_ray.datasink.CopyStageWriter")
    @mock.patch("hologres_ray.datasink.HoloClient")
    def test_driver_columns_used_by_tasks_and_commit(
        self, mock_client_cls, mock_writer_cls
    ):
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_writer = MagicMock()
        mock_writer_cls.return_value = mock_writer
        mock_writer_cls.return_value.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer_cls.return_value.__exit__ = MagicMock(return_value=False)

        # The block carries an extra column, so relying on the block alone would
        # stage columns the commit does not declare.
        table = pa.table({"id": [1], "name": ["a"], "extra": [2]})
        accessor = MagicMock()
        accessor.num_rows.return_value = 1
        accessor.to_arrow.return_value = table

        sink = self._sink(columns=["id", "name"])
        with mock.patch(
            "ray.data.block.BlockAccessor.for_block", return_value=accessor
        ):
            sink.write([MagicMock()], MagicMock())
        assert mock_writer_cls.call_args.kwargs["columns"] == ["id", "name"]

        sink.on_write_complete(MagicMock())
        assert mock_writer_cls.call_args.kwargs["columns"] == ["id", "name"]
        mock_writer.commit.assert_called_once()

    def test_ray_schema_does_not_override_driver_columns(self):
        sink = self._sink(columns=["id"])
        with mock.patch("hologres_ray.datasink.HoloClient") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_client.get_holo_version.return_value = (4, 1, 0)

            sink.on_write_start(pa.schema([("id", pa.int64()), ("name", pa.string())]))

        assert sink._commit_columns == ["id"]


class TestWriteHologres:
    def _write(self, schema, **kwargs):
        from hologres_ray import write_hologres

        dataset = MagicMock()
        dataset.schema.return_value = schema
        with mock.patch("hologres_ray.HologresDatasink") as sink_cls:
            write_hologres(
                dataset,
                table_name="test_table",
                connection_uri="postgresql://user:pass@localhost:5432/testdb",
                **kwargs,
            )
        return sink_cls.call_args.kwargs

    def test_stage_commit_columns_come_from_dataset_schema(self):
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        kwargs = self._write(schema, write_mode=WriteMode.COPY_STAGE)
        assert kwargs["columns"] == ["id", "name"]

    def test_stage_tolerates_unknown_dataset_schema(self):
        kwargs = self._write(None, write_mode=WriteMode.COPY_STAGE)
        assert kwargs["columns"] is None

    def test_copy_modes_do_not_resolve_columns(self):
        schema = pa.schema([("id", pa.int64())])
        kwargs = self._write(schema, write_mode=WriteMode.COPY_STREAM)
        assert kwargs["columns"] is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
