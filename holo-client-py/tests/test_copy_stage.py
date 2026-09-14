"""Unit tests for CopyStageWriter and AsyncCopyStageWriter."""

import pytest

from hologres import Column, OnConflictAction, Record, TableName, TableSchema
from hologres.column import INTEGER, VARCHAR


def _make_schema(with_pk=True, with_generated=False):
    table_name = TableName.valueOf("test_table")
    columns = [
        Column(
            name="id",
            type_name="int4",
            type=INTEGER,
            allow_null=False,
            is_primary_key=with_pk,
        ),
        Column(name="name", type_name="text", type=VARCHAR),
    ]
    if with_generated:
        columns.append(
            Column(
                name="gen_col", type_name="int4", type=INTEGER, is_generated_column=True
            )
        )
    return TableSchema(table_name, columns)


class TestCopyStageWriterInit:
    """Test CopyStageWriter initialization and column resolution."""

    def _make_config(self):
        from hologres import HoloConfig

        return HoloConfig(
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        )

    def test_default_columns_exclude_generated(self):

        from hologres.copy_stage import CopyStageWriter

        schema = _make_schema(with_generated=True)
        writer = CopyStageWriter(
            config=self._make_config(),
            schema=schema,
            stage_name="test_stage",
        )

        assert writer._column_names == ["id", "name"]
        assert writer._column_indices == [0, 1]

    def test_specific_columns(self):

        from hologres.copy_stage import CopyStageWriter

        schema = _make_schema()
        writer = CopyStageWriter(
            config=self._make_config(),
            schema=schema,
            stage_name="test_stage",
            columns=["name"],
        )

        assert writer._column_names == ["name"]
        assert writer._column_indices == [1]

    def test_invalid_column_raises(self):

        from hologres.copy_stage import CopyStageWriter

        schema = _make_schema()
        with pytest.raises(ValueError, match="not found"):
            CopyStageWriter(
                config=self._make_config(),
                schema=schema,
                stage_name="test_stage",
                columns=["nonexistent"],
            )


class TestCopyStageWriterFileNaming:
    """Test file index incrementing."""

    def test_file_prefix_contains_table_name(self):

        from hologres import HoloConfig
        from hologres.copy_stage import CopyStageWriter

        schema = _make_schema()
        config = HoloConfig(
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        )
        writer = CopyStageWriter(
            config=config,
            schema=schema,
            stage_name="test_stage",
        )
        assert "public_test_table_" in writer._file_prefix
        assert writer._file_index == 0


class TestWriteTable:
    """Test the Arrow-table fast path (write_table / put_table)."""

    def _make_writer(self, schema=None, **kwargs):
        from hologres import HoloConfig
        from hologres.copy_stage import CopyStageWriter

        config = HoloConfig(
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        )
        return CopyStageWriter(
            config=config,
            schema=schema or _make_schema(),
            stage_name="test_stage",
            **kwargs,
        )

    def test_write_table_counts_rows_without_uploading(self):
        pa = pytest.importorskip("pyarrow")

        writer = self._make_writer()
        writer.write_table(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))

        assert writer.count == 3
        # Well under the file size limit, so nothing is flushed to the stage yet.
        assert writer._file_index == 0
        assert writer._arrow_writer.data_size > 0

    def test_write_table_casts_and_reorders_columns(self):
        pa = pytest.importorskip("pyarrow")

        writer = self._make_writer()
        # Reversed order, int64 id (schema wants int32), and an extra column that
        # is not part of the table — all three must be handled.
        table = pa.table(
            {
                "extra": [9, 9],
                "name": pa.array(["x", "y"], type=pa.large_string()),
                "id": pa.array([1, 2], type=pa.int64()),
            }
        )
        writer.write_table(table)

        assert writer.count == 2
        data = writer._arrow_writer.end_and_get_bytes()
        got = pa.ipc.open_stream(data).read_all()
        assert got.column_names == ["id", "name"]
        assert got.schema.field("id").type == pa.int32()
        assert got.to_pydict() == {"id": [1, 2], "name": ["x", "y"]}

    def test_write_table_preserves_order_with_interleaved_puts(self):
        pa = pytest.importorskip("pyarrow")

        from hologres import Put

        writer = self._make_writer()
        put = Put(writer.schema)
        put.set_object("id", 1)
        put.set_object("name", "from_put")
        writer.write(put)
        writer.write_table(pa.table({"id": [2], "name": ["from_table"]}))

        data = writer._arrow_writer.end_and_get_bytes()
        got = pa.ipc.open_stream(data).read_all()
        assert got.to_pydict()["name"] == ["from_put", "from_table"]
        assert writer.count == 2

    def test_write_table_ignores_empty_table(self):
        pa = pytest.importorskip("pyarrow")

        writer = self._make_writer()
        writer.write_table(
            pa.table(
                {
                    "id": pa.array([], type=pa.int32()),
                    "name": pa.array([], type=pa.string()),
                }
            )
        )

        assert writer.count == 0
        assert writer._arrow_writer.end_and_get_bytes() == b""

    def test_write_table_rejects_missing_column(self):
        pa = pytest.importorskip("pyarrow")

        writer = self._make_writer()
        with pytest.raises(KeyError):
            writer.write_table(pa.table({"id": [1]}))


class TestAutoCommit:
    """auto_commit=False lets many writers share one stage with a single commit."""

    def _make_writer(self, **kwargs):
        from hologres import HoloConfig
        from hologres.copy_stage import CopyStageWriter

        config = HoloConfig(
            host="localhost",
            port=5432,
            database="test",
            username="user",
            password="pass",
        )
        return CopyStageWriter(
            config=config,
            schema=_make_schema(),
            stage_name="test_stage",
            **kwargs,
        )

    def test_defaults_to_true(self):
        assert self._make_writer()._auto_commit is True

    def test_exit_skips_commit_when_disabled(self):
        from unittest import mock

        writer = self._make_writer(auto_commit=False)
        with mock.patch.object(writer, "commit") as commit, mock.patch.object(
            writer, "flush"
        ) as flush:
            writer.__exit__(None, None, None)

        flush.assert_called_once()
        commit.assert_not_called()

    def test_exit_commits_when_enabled(self):
        from unittest import mock

        writer = self._make_writer(auto_commit=True)
        with mock.patch.object(writer, "commit") as commit, mock.patch.object(
            writer, "flush"
        ) as flush:
            writer.__exit__(None, None, None)

        flush.assert_called_once()
        commit.assert_called_once()


class TestPyarrowImportError:
    """Test behavior when pyarrow is not available."""

    def test_arrow_import_error(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "pyarrow":
                raise ImportError("No module named 'pyarrow'")
            return real_import(name, *args, **kwargs)

        # Clear cached import
        import sys

        saved = sys.modules.pop("pyarrow", None)
        try:
            monkeypatch.setattr(builtins, "__import__", mock_import)
            from hologres._arrow import _import_pyarrow

            with pytest.raises(ImportError, match="pyarrow is required"):
                _import_pyarrow()
        finally:
            if saved is not None:
                sys.modules["pyarrow"] = saved
