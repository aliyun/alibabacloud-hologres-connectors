"""Tests for the public package facade.

Guards against a common failure mode: a name listed in ``__all__`` that does not
actually exist, which only surfaces on ``from hologres.dataframe import *``.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import hologres.dataframe as hg

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.9/3.10 test environments
    import tomli as tomllib


class TestFacade:
    def test_version_is_exposed(self):
        assert hg.__version__ == "0.1.1"

    def test_every_exported_name_resolves(self):
        missing = [name for name in hg.__all__ if not hasattr(hg, name)]
        assert missing == []

    def test_all_has_no_duplicates(self):
        assert len(hg.__all__) == len(set(hg.__all__))

    def test_star_import_works(self):
        namespace = {}
        exec("from hologres.dataframe import *", namespace)  # noqa: S102
        for name in hg.__all__:
            assert name in namespace or name.startswith("_")

    def test_no_private_names_exported(self):
        assert [n for n in hg.__all__ if n.startswith("_") and n != "__version__"] == []

    def test_types_reachable_from_facade(self):
        assert hg.LongType().sql_name == "bigint"
        assert hg.StringType().sql_name == "text"
        assert (
            hg.StructType([hg.StructField("a", hg.LongType())]).to_ddl() == "a bigint"
        )

    def test_errors_reachable_from_facade(self):
        assert issubclass(hg.InvalidArgumentError, hg.HoloDataFrameError)

    def test_row_reachable_from_facade(self):
        assert hg.Row(("a",), (1,))["a"] == 1

    def test_docstring_present(self):
        assert hg.__doc__ and "Hologres" in hg.__doc__


class TestNamespacePackaging:
    def test_lives_in_hologres_namespace(self):
        assert hg.__name__ == "hologres.dataframe"

    def test_coexists_with_hologres_client(self):
        # hologres-client owns hologres/__init__.py and extends __path__ via
        # pkgutil so this distribution can add hologres.dataframe.
        import hologres

        assert len(hologres.__path__) >= 1
        assert hasattr(hologres, "dataframe")

    def test_coexists_across_separate_sys_path_roots(self, tmp_path):
        repository = Path(__file__).parents[2]
        client_root = tmp_path / "client"
        dataframe_root = tmp_path / "dataframe"
        shutil.copytree(
            repository / "holo-client-py" / "hologres", client_root / "hologres"
        )
        marker = dataframe_root / "hologres" / "dataframe" / "__init__.py"
        marker.parent.mkdir(parents=True)
        marker.write_text("SPLIT_NAMESPACE_MARKER = True\n")

        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join((str(client_root), str(dataframe_root)))
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import hologres.dataframe as d; assert d.SPLIT_NAMESPACE_MARKER",
            ],
            cwd=tmp_path,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr


class TestDependencyMetadata:
    @staticmethod
    def _project():
        path = Path(__file__).parents[1] / "pyproject.toml"
        return tomllib.loads(path.read_text())["project"]

    def test_pandas_and_arrow_are_required_dependencies(self):
        dependencies = self._project()["dependencies"]
        assert "pandas>=1.3" in dependencies
        assert "pyarrow>=12.0" in dependencies

    def test_client_version_includes_namespace_support(self):
        dependencies = self._project()["dependencies"]
        assert "hologres-client>=0.1.2" in dependencies

    def test_fc_is_installed_by_default(self):
        dependencies = self._project()["dependencies"]
        optional = self._project()["optional-dependencies"]
        assert "pandas" not in optional
        assert "alibabacloud-fc20230330>=4.7.9,<5" in dependencies
        assert optional["fc"] == []
