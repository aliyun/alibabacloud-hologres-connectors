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
