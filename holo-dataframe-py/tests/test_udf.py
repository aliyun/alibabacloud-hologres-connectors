"""Offline tests for Function Compute remote UDF and UDTF support."""

from __future__ import annotations

import gzip
import io
import json
import math
import sys
import types
import zipfile
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from hologres.table_name import TableName
from psycopg import sql

import hologres.dataframe as hg
from hologres.dataframe import _expr, _plan
from hologres.dataframe._fc import (
    DefaultFCDeployer,
    _definitions_in_expression,
    _definitions_in_value,
    _is_literal_binding,
    _is_not_found,
    _plan_values,
    _render_global_bindings,
    _render_user_code,
    build_drop_function_sql,
    build_function_zip,
    build_registration_sql,
    iter_remote_definitions,
    requires_function_recreate,
    validate_fc_config,
)
from hologres.dataframe.dataframe import DataFrame
from hologres.dataframe.exceptions import ExecutionError, InvalidArgumentError
from hologres.dataframe.types import LongType, StringType, StructField, StructType
from tests._fake_conn import OID_BOOL, OID_INT8, FakeColumnInfo, FakeConnection

PREFIX = "score="
CEIL = math.ceil
COMPLEX_LITERAL = {"values": [1, (2, 3)], "flags": {True, False}}


@hg.udf(name="test_grade")
def grade(n: int) -> str:
    return PREFIX + str(n)


@hg.udtf(
    name="test_split",
    output_schema=StructType([StructField("word", StringType())]),
)
class SplitWords:
    def process(self, text: str):
        for word in text.split():
            yield (word,)


@hg.udf(name="test_ceil")
def ceil_value(n: float) -> int:
    return CEIL(n) + len(COMPLEX_LITERAL["values"])


@hg.udf(name="test_floor")
def floor_value(n: float) -> int:
    return math.floor(n)


def _source_df(session=None):
    schema = StructType([StructField("id", LongType())])
    return DataFrame(
        _plan.TableScan(TableName.valueOf("public.source")),
        session=session,
        schema=schema,
    )


class FakeDeployer:
    def __init__(self):
        self.definitions = []

    def deploy(self, definition):
        self.definitions.append(definition)
        return f"123.cn-hangzhou-internal.fc.aliyuncs.com/{definition.fc_function_name}"


def _fc_session(deployer=None):
    from hologres.dataframe.session import _session_from_connection

    conn = FakeConnection(
        script=[
            ([(True,)], [FakeColumnInfo("exists", OID_BOOL)]),
            ([], []),
            ([], []),
            ([(7,)], [FakeColumnInfo("test_grade", OID_INT8)]),
        ]
    )
    session = _session_from_connection(
        conn,
        fc_config={
            "endpoint": "123.cn-hangzhou-internal.fc.aliyuncs.com",
            "region": "cn-hangzhou",
        },
        fc_deployer=deployer or FakeDeployer(),
    )
    return session, conn


class TestUDFDefinition:
    def test_annotations_are_inferred_and_call_is_lazy(self):
        assert grade.definition.input_types == (hg.LongType(),)
        assert grade.definition.return_type == hg.StringType()
        assert grade.definition.strict is True
        assert grade.definition.compression == "GZIP"
        assert grade.definition.fc_function_name == "test_grade"
        assert grade(hg.col("id"))._to_sql().as_string() == '"test_grade"("id")'
        assert grade("id")._to_sql().as_string() == '"test_grade"("id")'
        assert grade(3)._to_sql().as_string() == '"test_grade"(3)'
        assert repr(grade) == "UDF<test_grade>"

    def test_explicit_types_and_schema_qualified_name(self):
        @hg.udf(
            name="analytics.plus_one",
            input_types=[hg.IntegerType()],
            return_type=hg.IntegerType(),
            compression=None,
            version="v1",
        )
        def plus_one(n):
            return n + 1

        assert plus_one.definition.sql_name_parts == ("analytics", "plus_one")
        assert plus_one.definition.fc_function_name == "analytics_plus_one"
        assert plus_one("n")._to_sql().as_string() == ('"analytics"."plus_one"("n")')

    def test_function_can_be_used_without_parenthesized_decorator(self):
        @hg.udf
        def identity(value: str) -> str:
            return value

        assert identity("value").output_name == "identity"

    def test_with_column_preserves_remote_return_type(self):
        @hg.udf
        def plus_one(value: int) -> int:
            return value + 1

        frame = _source_df().with_column("next_id", plus_one("id"))
        assert frame.schema["next_id"].datatype == hg.LongType()

    def test_argument_count_is_checked(self):
        with pytest.raises(InvalidArgumentError, match="expects 1 arguments"):
            grade()
        with pytest.raises(InvalidArgumentError, match="expects 1 arguments"):
            grade("a", "b")

    @pytest.mark.parametrize("decorated", [1, object(), str])
    def test_udf_requires_function(self, decorated):
        with pytest.raises(InvalidArgumentError, match="Python function"):
            hg.udf(decorated)

    @pytest.mark.parametrize("name", ["", "bad-name", "a.b.c", 1])
    def test_invalid_name(self, name):
        with pytest.raises(InvalidArgumentError, match="UDF name"):

            @hg.udf(name=name)
            def fn(n: int) -> int:
                return n

    def test_missing_parameter_annotation(self):
        with pytest.raises(InvalidArgumentError, match="type annotation"):

            @hg.udf(return_type=hg.LongType())
            def fn(n):
                return n

    def test_missing_return_annotation(self):
        with pytest.raises(InvalidArgumentError, match="return annotation"):

            @hg.udf
            def fn(n: int):
                return n

    @pytest.mark.parametrize("annotation", [bytes, float | str])
    def test_unsupported_or_unmappable_annotation(self, annotation):
        def fn(n):
            return n

        fn.__annotations__ = {"n": annotation, "return": int}
        with pytest.raises(InvalidArgumentError):
            hg.udf(fn)

    def test_input_types_shape_is_validated(self):
        def fn(a, b):
            return a

        with pytest.raises(InvalidArgumentError, match="list or tuple"):
            hg.udf(fn, input_types="integer", return_type=hg.IntegerType())
        with pytest.raises(InvalidArgumentError, match="2 parameters"):
            hg.udf(fn, input_types=[hg.IntegerType()], return_type=hg.IntegerType())

    def test_explicit_types_survive_unresolved_annotations(self):
        def fn(n):
            return n

        fn.__annotations__ = {"n": "MissingType", "return": "MissingType"}
        wrapped = hg.udf(
            fn,
            input_types=[hg.IntegerType()],
            return_type=hg.IntegerType(),
        )
        assert wrapped.definition.input_types == (hg.IntegerType(),)

    @pytest.mark.parametrize(
        "signature_source",
        [
            "def fn(*values: int) -> int:\n    return 1",
            "def fn(**values: int) -> int:\n    return 1",
            "def fn(*, value: int) -> int:\n    return value",
        ],
    )
    def test_non_positional_parameters_are_rejected(self, signature_source):
        namespace = {}
        exec(signature_source, namespace)
        with pytest.raises(InvalidArgumentError, match="must be positional"):
            hg.udf(namespace["fn"])

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"packages": "numpy"}, "packages"),
            ({"packages": ["numpy", "numpy"]}, "duplicates"),
            ({"imports": "utils.py"}, "imports"),
            ({"imports": [1]}, "imports entry"),
            ({"imports": [""]}, "path string"),
            ({"imports": [(None, "x")]}, "path string"),
            ({"runtime": ""}, "runtime"),
            ({"runtime": "nodejs20"}, "runtime"),
            ({"replace": 1}, "replace"),
            ({"strict": 1}, "strict"),
            ({"max_batch_size": 0}, "max_batch_size"),
            ({"compression": "zip"}, "compression"),
            ({"version": ""}, "version"),
        ],
    )
    def test_deployment_options_are_validated(self, kwargs, message):
        with pytest.raises(InvalidArgumentError, match=message):

            @hg.udf(**kwargs)
            def fn(n: int) -> int:
                return n


class TestUDTFDefinition:
    def test_single_output_schema_and_direct_select(self):
        call = SplitWords("text")
        assert call._to_sql().as_string() == '"test_split"("text")'
        assert call.output_name == "word"
        assert SplitWords.definition.output_name == "word"
        assert repr(SplitWords) == "UDTF<test_split>"

        schema = StructType([StructField("text", StringType())])
        frame = DataFrame(
            _plan.TableScan(TableName.valueOf("public.docs")), schema=schema
        ).select("text", call)
        assert frame._to_sql().as_string() == (
            'SELECT "text", "test_split"("text") AS "word" FROM "public"."docs"'
        )
        assert frame.schema.names == ("text", "word")
        assert frame.schema["word"].datatype == hg.StringType()

    def test_udtf_must_decorate_class_with_process(self):
        with pytest.raises(InvalidArgumentError, match="Python class"):
            hg.udtf(lambda: None, output_schema=StructType())
        with pytest.raises(InvalidArgumentError, match="define process"):

            @hg.udtf(output_schema=StructType([StructField("x", StringType())]))
            class MissingProcess:
                pass

    @pytest.mark.parametrize(
        "schema",
        [
            None,
            [],
            StructType(),
            StructType(
                [StructField("a", StringType()), StructField("b", StringType())]
            ),
        ],
    )
    def test_output_schema_must_be_single_field(self, schema):
        with pytest.raises(InvalidArgumentError, match="output_schema"):

            @hg.udtf(output_schema=schema)
            class F:
                def process(self, value: str):
                    yield (value,)

    def test_output_type_is_fc_limited(self):
        with pytest.raises(InvalidArgumentError, match="not supported"):

            @hg.udtf(output_schema=StructType([StructField("x", hg.TimestampType())]))
            class F:
                def process(self, value: str):
                    yield (value,)

    def test_process_must_start_with_self(self):
        with pytest.raises(InvalidArgumentError, match="start with self"):

            @hg.udtf(output_schema=StructType([StructField("x", StringType())]))
            class F:
                @staticmethod
                def process(value: str):
                    yield (value,)


class TestRegistrationSQL:
    def test_scalar_registration(self):
        rendered = build_registration_sql(
            grade.definition,
            "123.cn-hangzhou-internal.fc.aliyuncs.com/test_grade",
        ).as_string()
        assert rendered == (
            'CREATE OR REPLACE FUNCTION "test_grade" ("arg0" bigint) '
            "RETURNS text LANGUAGE function_compute "
            "AS '123.cn-hangzhou-internal.fc.aliyuncs.com/test_grade' STRICT "
            "SET function_compute.max_batch_size TO 1024 "
            "SET function_compute.compression TO 'GZIP' "
            "SET function_compute.qualifier TO 'LATEST'"
        )

    def test_udtf_registration_and_optional_clauses(self):
        @hg.udtf(
            output_schema=StructType([StructField("word", StringType())]),
            replace=False,
            strict=False,
            compression=None,
        )
        class Words:
            def process(self, value: str):
                yield (value,)

        rendered = build_registration_sql(
            Words.definition, "endpoint/Words"
        ).as_string()
        assert rendered.startswith('CREATE FUNCTION "Words" ("arg0" text)')
        assert 'RETURNS TABLE ("word" text)' in rendered
        assert " STRICT " not in rendered
        assert "compression" not in rendered

    def test_drop_sql_and_recreate_compatibility(self):
        @hg.udf(name="test_grade")
        def changed_return(value: int) -> int:
            return value

        assert build_drop_function_sql(changed_return.definition).as_string() == (
            'DROP FUNCTION "test_grade" (bigint)'
        )
        assert requires_function_recreate(grade.definition, changed_return.definition)
        assert not requires_function_recreate(grade.definition, grade.definition)


class TestPlanDiscoveryAndSession:
    def test_nested_definitions_are_discovered_once(self):
        nested = hg.function("coalesce", grade("id"), grade("id"))
        frame = _source_df().select(nested.alias("value"))
        definitions = list(iter_remote_definitions(frame._plan))
        assert definitions == [grade.definition]

    def test_conflicting_definitions_in_one_plan_are_rejected(self):
        @hg.udf(name="conflict")
        def first(value: int) -> str:
            return str(value)

        @hg.udf(name="conflict")
        def second(value: int) -> str:
            return f"value={value}"

        frame = _source_df().select(
            first("id").alias("first"), second("id").alias("second")
        )
        with pytest.raises(InvalidArgumentError, match="conflicting definitions"):
            list(iter_remote_definitions(frame._plan))

    def test_relation_rewrite_preserves_remote_definition(self):
        left = _source_df()
        right = DataFrame(
            _plan.TableScan(TableName.valueOf("public.other")),
            schema=StructType([StructField("other_id", LongType())]),
        )
        joined = left.join(right, on=left["id"] == right["other_id"]).select(
            grade(left["id"]).alias("value")
        )
        assert '"test_grade"("t0"."id")' in joined._to_sql().as_string()
        assert list(iter_remote_definitions(joined._plan)) == [grade.definition]

    def test_first_action_deploys_registers_and_caches(self):
        deployer = FakeDeployer()
        session, conn = _fc_session(deployer)
        frame = _source_df(session).select(grade("id"))

        assert frame._to_sql().as_string().startswith("SELECT")
        assert deployer.definitions == []
        assert frame.collect()[0][0] == 7
        assert len(deployer.definitions) == 1
        assert conn.recorded[:4] == [
            "SELECT EXISTS (SELECT 1 FROM pg_available_extensions "
            "WHERE name = 'function_compute')",
            "CREATE EXTENSION IF NOT EXISTS function_compute",
            build_registration_sql(
                grade.definition,
                "123.cn-hangzhou-internal.fc.aliyuncs.com/test_grade",
            ).as_string(),
            'SELECT "test_grade"("id") FROM "public"."source"',
        ]

        frame.collect()
        assert len(deployer.definitions) == 1
        assert conn.recorded[-1].startswith("SELECT")

    def test_missing_fc_config_fails_only_at_action(self):
        from hologres.dataframe.session import _session_from_connection

        session = _session_from_connection(FakeConnection())
        frame = _source_df(session).select(grade("id"))
        assert frame._to_sql().as_string().startswith("SELECT")
        with pytest.raises(InvalidArgumentError, match="fc_config"):
            frame.collect()

    def test_changed_definition_redeploys_when_replace_is_true(self):
        deployer = FakeDeployer()
        session, _ = _fc_session(deployer)
        _source_df(session).select(grade("id")).collect()

        @hg.udf(name="test_grade")
        def another(n: int) -> str:
            return str(n)

        _source_df(session).select(another("id")).collect()
        assert deployer.definitions == [grade.definition, another.definition]

    def test_changed_return_type_is_dropped_and_recreated_in_one_transaction(self):
        deployer = FakeDeployer()
        session, _ = _fc_session(deployer)
        _source_df(session).select(grade("id")).collect()

        @hg.udf(name="test_grade")
        def changed_return(n: int) -> int:
            return n

        with patch.object(type(session), "_execute_write_transaction") as transaction:
            _source_df(session).select(changed_return("id")).collect()

        statements = transaction.call_args.args[0]
        assert [statement.as_string() for statement in statements] == [
            'DROP FUNCTION "test_grade" (bigint)',
            build_registration_sql(
                changed_return.definition,
                "123.cn-hangzhou-internal.fc.aliyuncs.com/test_grade",
            ).as_string(),
        ]

    def test_changed_definition_is_rejected_when_replace_is_false(self):
        session, _ = _fc_session()
        _source_df(session).select(grade("id")).collect()

        @hg.udf(name="test_grade", replace=False)
        def another(n: int) -> str:
            return str(n)

        with pytest.raises(InvalidArgumentError, match="replace=False"):
            _source_df(session).select(another("id")).collect()

    def test_default_deployer_is_created_from_session_config(self):
        session, _ = _fc_session()
        session._fc_deployer = None
        deployer = FakeDeployer()
        with patch(
            "hologres.dataframe._fc.DefaultFCDeployer", return_value=deployer
        ) as factory:
            _source_df(session).select(grade("id")).collect()
        factory.assert_called_once_with(session._fc_config)
        assert deployer.definitions == [grade.definition]

    def test_prepare_sql_without_session_is_rejected(self):
        with pytest.raises(InvalidArgumentError, match="no session"):
            _source_df()._prepare_sql()


class TestFCConfig:
    def test_normalizes_config(self):
        config = validate_fc_config(
            {
                "endpoint": "123.cn-hangzhou-internal.fc.aliyuncs.com/",
                "region": "cn-hangzhou",
                "account_id": "123",
                "role_arn": "acs:ram::123:role/fc",
                "api_endpoint": "custom-api",
                "memory_size": 1024,
                "timeout": 90,
            }
        )
        assert config["endpoint"] == "123.cn-hangzhou-internal.fc.aliyuncs.com"
        assert config["memory_size"] == 1024
        assert validate_fc_config(None) is None

    @pytest.mark.parametrize(
        ("config", "message"),
        [
            ([], "dict"),
            ({"endpoint": "x", "region": "r", "unknown": 1}, "unknown"),
            ({"region": "r"}, "endpoint"),
            ({"endpoint": "https://x.fc.aliyuncs.com", "region": "r"}, "hostname"),
            ({"endpoint": "x.example.com", "region": "r"}, "internal FC"),
            (
                {"endpoint": "x.cn-hangzhou-internal.fc.aliyuncs.com", "region": ""},
                "region",
            ),
            (
                {
                    "endpoint": "x.cn-hangzhou-internal.fc.aliyuncs.com",
                    "region": "r",
                    "role_arn": "",
                },
                "role_arn",
            ),
            (
                {
                    "endpoint": "x.cn-hangzhou-internal.fc.aliyuncs.com",
                    "region": "r",
                    "memory_size": 0,
                },
                "memory_size",
            ),
        ],
    )
    def test_invalid_config(self, config, message):
        with pytest.raises(InvalidArgumentError, match=message):
            validate_fc_config(config)


class TestPackaging:
    def test_zip_contains_executable_scalar_handler_and_global_binding(self):
        archive = zipfile.ZipFile(io.BytesIO(build_function_zip(grade.definition)))
        assert {"user_code.py", "handler.py"}.issubset(archive.namelist())
        user_code = archive.read("user_code.py").decode()
        handler_code = archive.read("handler.py").decode()
        assert "@hg.udf" not in user_code
        assert "PREFIX = 'score='" in user_code

        user_module = types.ModuleType("user_code")
        exec(user_code, user_module.__dict__)
        with patch.dict(sys.modules, {"user_code": user_module}):
            handler_module = types.ModuleType("handler")
            exec(handler_code, handler_module.__dict__)
        assert handler_module.handler({"data": [[2], [3]]}, None) == {
            "results": [["score=2"], ["score=3"]]
        }
        assert handler_module.handler(b'{"data": [[4]]}', None) == {
            "results": [["score=4"]]
        }
        compressed = handler_module.handler(
            gzip.compress(b'{"data": [[5], [6]]}'), None
        )
        assert json.loads(gzip.decompress(compressed)) == {
            "results": [["score=5"], ["score=6"]]
        }

    def test_zip_contains_executable_udtf_handler(self):
        archive = zipfile.ZipFile(io.BytesIO(build_function_zip(SplitWords.definition)))
        user_module = types.ModuleType("user_code")
        exec(archive.read("user_code.py"), user_module.__dict__)
        with patch.dict(sys.modules, {"user_code": user_module}):
            handler_module = types.ModuleType("handler")
            exec(archive.read("handler.py"), handler_module.__dict__)
        assert handler_module.handler({"data": [["a b"], ["c"]]}, None) == {
            "results": [[0, "a"], [0, "b"], [1, "c"]]
        }
        compressed = handler_module.handler(
            gzip.compress(b'{"data": [["d e"], ["f"]]}'), None
        )
        assert json.loads(gzip.decompress(compressed)) == {
            "results": [[0, "d"], [0, "e"], [1, "f"]]
        }

    def test_import_file_is_copied(self, tmp_path):
        helper = tmp_path / "helper.py"
        helper.write_text("VALUE = 1\n")

        @hg.udf(imports=[(str(helper), "pkg.helper")])
        def fn(n: int) -> int:
            return n

        archive = zipfile.ZipFile(io.BytesIO(build_function_zip(fn.definition)))
        assert "pkg/helper.py" in archive.namelist()

        @hg.udf(imports=[str(helper)])
        def plain(n: int) -> int:
            return n

        plain_archive = zipfile.ZipFile(
            io.BytesIO(build_function_zip(plain.definition))
        )
        assert "helper.py" in plain_archive.namelist()

    def test_import_directory_is_copied(self, tmp_path):
        package = tmp_path / "helpers"
        package.mkdir()
        (package / "__init__.py").write_text("VALUE = 1\n")

        @hg.udf(imports=[(str(package), "helpers")])
        def fn(n: int) -> int:
            return n

        assert fn.definition.fingerprint
        archive = zipfile.ZipFile(io.BytesIO(build_function_zip(fn.definition)))
        assert "helpers/__init__.py" in archive.namelist()

    def test_import_content_participates_in_deployment_fingerprint(self, tmp_path):
        helper = tmp_path / "helper.py"
        helper.write_text("VALUE = 1\n")

        @hg.udf(imports=[str(helper)])
        def fn(n: int) -> int:
            return n

        before = fn.definition.fingerprint
        helper.write_text("VALUE = 2\n")
        assert fn.definition.fingerprint != before

    def test_missing_import_and_bad_target_are_rejected(self, tmp_path):
        @hg.udf(imports=[(str(tmp_path / "missing.py"), "missing")])
        def missing(n: int) -> int:
            return n

        assert missing.definition.fingerprint
        with pytest.raises(InvalidArgumentError, match="does not exist"):
            build_function_zip(missing.definition)

        helper = tmp_path / "helper.py"
        helper.write_text("VALUE = 1\n")

        @hg.udf(imports=[(str(helper), "../helper")])
        def bad_target(n: int) -> int:
            return n

        with pytest.raises(InvalidArgumentError, match="invalid FC import target"):
            build_function_zip(bad_target.definition)

    def test_package_install_failure_is_wrapped(self):
        @hg.udf(packages=["missing-package"])
        def fn(n: int) -> int:
            return n

        completed = SimpleNamespace(returncode=1, stderr="not found", stdout="")
        with patch("hologres.dataframe._fc.subprocess.run", return_value=completed):
            with pytest.raises(ExecutionError, match="not found"):
                build_function_zip(fn.definition)

    def test_dynamic_or_async_source_is_rejected(self):
        namespace = {}
        exec("def dynamic(n: int) -> int:\n    return n", namespace)
        dynamic = hg.udf(namespace["dynamic"])
        assert dynamic.definition.fingerprint
        with pytest.raises(InvalidArgumentError, match="cannot read source"):
            build_function_zip(dynamic.definition)

        async def asynchronous(n: int) -> int:
            return n

        asynchronous = hg.udf(asynchronous)
        with pytest.raises(InvalidArgumentError, match="async"):
            build_function_zip(asynchronous.definition)

    def test_unserializable_global_is_rejected(self):
        unsupported = object()

        @hg.udf
        def fn(n: int) -> int:
            return n if unsupported else 0

        assert fn.definition.fingerprint
        with pytest.raises(InvalidArgumentError, match="global"):
            build_function_zip(fn.definition)

    def test_same_module_helper_is_rejected(self):
        def helper(n: int) -> int:
            return n + 1

        @hg.udf
        def fn(n: int) -> int:
            return helper(n)

        assert fn.definition.fingerprint
        with pytest.raises(InvalidArgumentError, match="global callable 'helper'"):
            build_function_zip(fn.definition)

    def test_fingerprint_covers_udtf_and_global_binding_shapes(self):
        assert SplitWords.definition.fingerprint
        assert ceil_value.definition.fingerprint
        assert floor_value.definition.fingerprint

        with patch(
            "hologres.dataframe.udf.inspect.getclosurevars", side_effect=TypeError
        ):
            assert grade.definition.fingerprint

    def test_multiple_source_definitions_are_rejected(self):
        with patch(
            "hologres.dataframe._fc.inspect.getsource",
            return_value="def a():\n    pass\ndef b():\n    pass\n",
        ):
            with pytest.raises(InvalidArgumentError, match="cannot isolate"):
                _render_user_code(grade.definition)

    def test_global_binding_variants(self):
        rendered = _render_global_bindings(ceil_value.definition)
        assert "from math import ceil as CEIL" in rendered
        assert "COMPLEX_LITERAL =" in rendered
        assert "import math as math" in _render_global_bindings(floor_value.definition)

        with patch(
            "hologres.dataframe._fc.inspect.getclosurevars", side_effect=TypeError
        ):
            assert _render_global_bindings(grade.definition) == ""

        assert _is_literal_binding((1, [2], {3}, {"x": False}))
        assert not _is_literal_binding(object())


class TestDefaultDeployer:
    class Models:
        class InputCodeLocation:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class CreateFunctionInput:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class UpdateFunctionInput:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class CreateFunctionRequest:
            def __init__(self, body):
                self.body = body

        class UpdateFunctionRequest:
            def __init__(self, body):
                self.body = body

        class GetFunctionRequest:
            pass

    class Client:
        def __init__(self, exists=False, error=None):
            self.exists = exists
            self.error = error
            self.created = []
            self.updated = []

        def get_function(self, name, request):
            if self.error is not None:
                raise self.error
            if not self.exists:
                error = RuntimeError("missing")
                error.status_code = 404
                raise error
            return object()

        def create_function(self, request):
            self.created.append(request)

        def update_function(self, name, request):
            self.updated.append((name, request))

    def _deployer(self, client):
        deployer = DefaultFCDeployer(
            {
                "endpoint": "123.cn-hangzhou-internal.fc.aliyuncs.com",
                "region": "cn-hangzhou",
                "memory_size": 512,
                "timeout": 60,
            }
        )
        deployer._client = (client, self.Models)
        return deployer

    def test_create_update_and_keep_existing(self):
        created = self.Client(exists=False)
        path = self._deployer(created).deploy(grade.definition)
        assert path.endswith("/test_grade")
        assert len(created.created) == 1

        updated = self.Client(exists=True)
        self._deployer(updated).deploy(grade.definition)
        assert updated.updated[0][0] == "test_grade"

        @hg.udf(name="keep", replace=False)
        def keep(n: int) -> int:
            return n

        existing = self.Client(exists=True)
        self._deployer(existing).deploy(keep.definition)
        assert existing.created == [] and existing.updated == []

    def test_get_error_other_than_not_found_is_propagated(self):
        error = RuntimeError("denied")
        client = self.Client(error=error)
        with pytest.raises(RuntimeError, match="denied"):
            self._deployer(client).deploy(grade.definition)

    def test_sdk_client_is_built_lazily(self):
        captured = {}

        class CredentialClient:
            pass

        class Config:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        class Client:
            def __init__(self, config):
                captured["client_config"] = config

        modules = {
            "alibabacloud_credentials.client": SimpleNamespace(Client=CredentialClient),
            "alibabacloud_fc20230330": SimpleNamespace(models=self.Models),
            "alibabacloud_fc20230330.client": SimpleNamespace(Client=Client),
            "alibabacloud_tea_openapi": SimpleNamespace(
                models=SimpleNamespace(Config=Config)
            ),
        }
        deployer = self._deployer(self.Client())
        deployer._client = None
        with patch.dict(sys.modules, modules):
            client, models = deployer._sdk()
            assert deployer._sdk() == (client, models)
        assert captured["region_id"] == "cn-hangzhou"
        assert captured["endpoint"] == "fcv3.cn-hangzhou.aliyuncs.com"

    def test_missing_sdk_has_actionable_error(self):
        deployer = self._deployer(self.Client())
        deployer._client = None
        original_import = __import__

        def missing_import(name, *args, **kwargs):
            if name.startswith("alibabacloud_"):
                raise ImportError(name)
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=missing_import):
            with pytest.raises(ImportError, match="dependencies are missing"):
                deployer._sdk()

    @pytest.mark.parametrize(
        "error",
        [
            SimpleNamespace(status_code=404),
            SimpleNamespace(code="FunctionNotFound"),
            SimpleNamespace(data={"statusCode": 404}),
        ],
    )
    def test_not_found_classification(self, error):
        assert _is_not_found(error)
        assert not _is_not_found(RuntimeError("other"))


class TestDiscoveryBranches:
    def test_expression_walk_covers_all_composite_nodes(self):
        remote = grade("id")._expr
        expressions = [
            _expr.UnaryExpr("NOT", remote),
            _expr.PredicateExpr(
                sql.SQL("{} IN ({})"),
                [remote, _expr.Literal(1)],
            ),
            _expr.CastExpr(remote, "text"),
            _expr.SortExpr(remote),
            _expr.WindowExpr("max", [remote], [_expr.ColumnRef("id")]),
        ]
        for expression in expressions:
            assert list(_definitions_in_expression(expression)) == [grade.definition]

    def test_value_and_slot_walk_helpers(self):
        assert list(_definitions_in_value({"fn": grade("id")})) == [grade.definition]

        class SingleSlot:
            __slots__ = "value"

            def __init__(self, value):
                self.value = value

        values = list(_plan_values(SingleSlot(grade("id"))))
        assert len(values) == 1 and isinstance(values[0], hg.Column)
