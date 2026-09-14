"""Tests for session.read_files (local files -> stage/scratch materialization).

Nothing here reaches Hologres: FakeConnection scripts the version probe, the
FixedFE upload is stubbed, and files are written to pytest's ``tmp_path``. The
point is to nail down what read_files does on the client side -- format
inference, option validation, glob expansion, schema propagation, laziness --
before it ever touches the network.
"""

from __future__ import annotations

import gzip
import json

import pytest

import hologres.dataframe as hg
from hologres.dataframe import _plan
from hologres.dataframe.exceptions import InvalidArgumentError
from tests._fake_conn import OID_TEXT, FakeColumnInfo, make_session

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def version_response(version="5.1.0"):
    return (
        [(f"Hologres {version} (tag: {version})",)],
        [FakeColumnInfo("v", OID_TEXT)],
    )


@pytest.fixture
def stage_session(monkeypatch):
    """Session at Hologres 5.1.0 with the FixedFE upload captured."""
    session, conn = make_session(script=[version_response("5.1.0")])
    session._conninfo = "host=h port=80 dbname=d user=u password=p"
    payloads = []

    class _Copy:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def write(self, data):
            payloads.append(data)

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def copy(self, statement):
            return _Copy()

    class _Conn:
        def cursor(self):
            return _Cursor()

        def close(self):
            pass

    monkeypatch.setattr(
        "hologres.copy_stage._create_fixed_fe_conn", lambda config: _Conn()
    )
    return session, conn, payloads


@pytest.fixture
def csv_path(tmp_path):
    """A small csv with two typed columns."""
    p = tmp_path / "data.csv"
    p.write_text("id,name\n1,a\n2,b\n3,c\n")
    return str(p)


def write_parquet(path, data):
    pq.write_table(pa.table(data), str(path))
    return str(path)


class TestFormatInference:
    def test_infers_csv_from_extension(self, csv_path, stage_session):
        session, *_ = stage_session
        assert session.read_files(csv_path).columns == ["id", "name"]

    @pytest.mark.parametrize("ext", ["json", "jsonl", "ndjson"])
    def test_infers_json_from_extension(self, ext, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / f"data.{ext}"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        assert session.read_files(str(p)).columns == ["a"]

    @pytest.mark.parametrize("ext", ["parquet", "pq"])
    def test_infers_parquet_from_extension(self, ext, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / f"data.{ext}"
        write_parquet(p, {"id": [1, 2]})
        assert session.read_files(str(p)).columns == ["id"]

    def test_gzip_extension_looks_through_to_the_real_format(
        self, tmp_path, stage_session
    ):
        session, *_ = stage_session
        p = tmp_path / "data.csv.gz"
        with gzip.open(p, "wt") as f:
            f.write("id,name\n1,a\n")
        assert session.read_files(str(p)).columns == ["id", "name"]

    def test_explicit_format_wins(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / "custom.dat"
        p.write_text("id,name\n1,a\n")
        assert session.read_files(str(p), format="csv").columns == ["id", "name"]

    def test_unknown_extension_is_rejected(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / "custom.dat"
        p.write_text("id,name\n1,a\n")
        with pytest.raises(InvalidArgumentError, match="cannot tell the format"):
            session.read_files(str(p))

    def test_unknown_explicit_format_is_rejected(self, csv_path, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="unknown format"):
            session.read_files(csv_path, format="avro")


class TestCsv:
    def test_first_row_becomes_column_names(self, csv_path, stage_session):
        session, *_ = stage_session
        df = session.read_files(csv_path)
        assert df.columns == ["id", "name"]
        assert df.schema["id"].datatype == hg.LongType()
        assert df.schema["name"].datatype == hg.StringType()

    def test_delimiter_option_is_honored(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / "pipe.csv"
        p.write_text("id|name\n1|a\n")
        assert session.read_files(str(p), delimiter="|").columns == ["id", "name"]

    def test_skip_rows_option_is_honored(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / "banner.csv"
        p.write_text("# generated file\nid,name\n1,a\n")
        assert session.read_files(str(p), skip_rows=1).columns == ["id", "name"]

    def test_null_values_option_is_honored(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / "nulls.csv"
        p.write_text("id,name\n1,MISSING\n2,b\n")
        df = session.read_files(str(p), null_values=["MISSING"])
        assert df.schema["name"].datatype == hg.StringType()

    def test_unknown_option_is_rejected(self, csv_path, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="unknown read option"):
            session.read_files(csv_path, sep=",")


class TestJson:
    def test_line_delimited_json(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / "data.json"
        p.write_text(
            json.dumps({"a": 1, "b": "x"})
            + "\n"
            + json.dumps({"a": 2, "b": "y"})
            + "\n"
        )
        df = session.read_files(str(p))
        assert df.columns == ["a", "b"]

    def test_unknown_option_is_rejected(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / "data.json"
        p.write_text('{"a": 1}\n')
        with pytest.raises(InvalidArgumentError, match="unknown read option"):
            session.read_files(str(p), delimiter=",")


class TestParquet:
    def test_reads_native_schema(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = write_parquet(tmp_path / "data.parquet", {"id": [1, 2], "name": ["a", "b"]})
        df = session.read_files(p)
        assert df.columns == ["id", "name"]
        assert df.schema["id"].datatype == hg.LongType()

    def test_columns_option_projects_early(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = write_parquet(
            tmp_path / "wide.parquet", {"id": [1], "name": ["a"], "extra": ["x"]}
        )
        assert session.read_files(p, columns=["id", "name"]).columns == ["id", "name"]


class TestMultipleFiles:
    def test_glob_matches_all_files(self, tmp_path, stage_session):
        session, *_ = stage_session
        (tmp_path / "a.csv").write_text("id,name\n1,a\n")
        (tmp_path / "b.csv").write_text("id,name\n2,b\n")
        df = session.read_files(str(tmp_path / "*.csv"))
        assert df.columns == ["id", "name"]

    def test_list_of_paths(self, tmp_path, stage_session):
        session, *_ = stage_session
        p1 = tmp_path / "a.csv"
        p1.write_text("id,name\n1,a\n")
        p2 = tmp_path / "b.csv"
        p2.write_text("id,name\n2,b\n")
        df = session.read_files([str(p1), str(p2)])
        assert df.columns == ["id", "name"]

    def test_schema_conflict_reports_the_offending_file(self, tmp_path, stage_session):
        session, *_ = stage_session
        (tmp_path / "a.csv").write_text("id,name\n1,a\n")
        (tmp_path / "b.csv").write_text("id,other\n2,b\n")
        with pytest.raises(
            InvalidArgumentError, match="all files must share one schema"
        ):
            session.read_files(str(tmp_path / "*.csv"))

    def test_no_match_is_rejected_immediately(self, tmp_path, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="no file matches"):
            session.read_files(str(tmp_path / "missing_*.csv"))

    def test_missing_file_is_rejected_immediately(self, tmp_path, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="not a readable file"):
            session.read_files(str(tmp_path / "missing.csv"))

    def test_empty_list_is_rejected(self, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="empty list"):
            session.read_files([])

    def test_mixed_extensions_are_rejected(self, tmp_path, stage_session):
        session, *_ = stage_session
        (tmp_path / "a.csv").write_text("id\n1\n")
        (tmp_path / "b.json").write_text('{"id": 2}\n')
        with pytest.raises(InvalidArgumentError, match="mix formats"):
            session.read_files([str(tmp_path / "a.csv"), str(tmp_path / "b.json")])

    def test_incompatible_types_across_files_report_the_group(
        self, tmp_path, stage_session
    ):
        session, *_ = stage_session
        # Two parquet files whose 'v' column has incompatible types.
        pq.write_table(
            pa.table({"v": pa.array([1], pa.int64())}), str(tmp_path / "a.parquet")
        )
        pq.write_table(pa.table({"v": ["x"]}), str(tmp_path / "b.parquet"))
        with pytest.raises(InvalidArgumentError, match="and 1 more file"):
            session.read_files(str(tmp_path / "*.parquet"))


class TestSchemaOverride:
    def test_relabels_columns(self, csv_path, stage_session):
        session, *_ = stage_session
        schema = hg.StructType(
            [hg.StructField("a", hg.LongType()), hg.StructField("b", hg.StringType())]
        )
        df = session.read_files(csv_path, schema=schema)
        assert df.columns == ["a", "b"]

    def test_field_count_must_match(self, csv_path, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="schema has 1 field"):
            session.read_files(
                csv_path, schema=hg.StructType([hg.StructField("a", hg.LongType())])
            )

    def test_must_be_a_struct_type(self, csv_path, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="must be a StructType"):
            session.read_files(csv_path, schema="id bigint, name text")


class TestLaziness:
    def test_reads_the_file_immediately(self, tmp_path, stage_session):
        # This is what makes df.schema free -- the local read runs now.
        session, *_ = stage_session
        p = tmp_path / "data.csv"
        p.write_text("id,name\n1,a\n")
        df = session.read_files(str(p))
        p.unlink()  # gone, but the frame is still fine
        assert df.columns == ["id", "name"]

    def test_upload_is_deferred_to_the_first_action(self, csv_path, stage_session):
        session, conn, payloads = stage_session
        df = session.read_files(csv_path)
        assert isinstance(df._plan, _plan.PendingSource)
        assert payloads == []
        assert not any("hg_create_internal_stage" in q for q in conn.recorded)

    def test_upload_happens_once(self, csv_path, stage_session):
        session, _, payloads = stage_session
        df = session.read_files(csv_path)
        df._to_sql()
        df._to_sql()
        df._to_sql()
        assert len(payloads) == 1

    def test_empty_file_is_rejected_at_read_time(self, tmp_path, stage_session):
        session, *_ = stage_session
        p = tmp_path / "empty.csv"
        p.write_text("id,name\n")  # header only
        with pytest.raises(InvalidArgumentError, match="contained no rows"):
            session.read_files(str(p))


class TestChannels:
    def test_reads_back_via_the_stage(self, csv_path, stage_session):
        session, *_ = stage_session
        df = session.read_files(csv_path)
        out = df._to_sql().as_string()
        assert out.startswith(
            "SELECT * FROM external_files(path='internal_stage://_hg_df_tmp_"
        )

    def test_old_server_creates_a_table(self, tmp_path, monkeypatch):
        session, conn = make_session(script=[version_response("4.0.1")])
        p = tmp_path / "data.csv"
        p.write_text("id,name\n1,a\n2,b\n")
        session.read_files(str(p))._to_sql()
        assert any("CREATE TABLE" in q for q in conn.recorded)
        assert conn.copied_rows == [(1, "a"), (2, "b")]

    def test_explicit_table_name_is_left_to_the_caller(self, csv_path, stage_session):
        session, *_ = stage_session
        df = session.read_files(csv_path, table_name="public.mine")
        df._to_sql()
        assert session._scratch_tables == []


class TestValidation:
    def test_non_string_path_rejected(self, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="must be a string"):
            session.read_files(123)

    def test_non_string_entry_in_list_rejected(self, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="non-empty string"):
            session.read_files(["a.csv", 7])

    def test_directory_is_rejected(self, tmp_path, stage_session):
        session, *_ = stage_session
        with pytest.raises(InvalidArgumentError, match="not a readable file"):
            session.read_files(str(tmp_path))
