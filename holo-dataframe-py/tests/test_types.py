"""Tests for the data type system."""

import dataclasses
import datetime
import decimal
import sys
import typing

import pytest

from hologres.dataframe.exceptions import (
    InvalidArgumentError,
    UnsupportedOperationError,
)
from hologres.dataframe.types import (
    FC_SUPPORTED_TYPES,
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    GeometryType,
    IntegerType,
    JsonbType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VectorType,
    _FractionalType,
    _IntegralType,
    _NumericType,
    from_arrow_type,
    from_python_type,
    parse_type,
)

pa = pytest.importorskip("pyarrow")


SCALAR_SQL_NAMES = [
    (BooleanType(), "boolean"),
    (ByteType(), '"char"'),
    (ShortType(), "smallint"),
    (IntegerType(), "integer"),
    (LongType(), "bigint"),
    (FloatType(), "real"),
    (DoubleType(), "double precision"),
    (BinaryType(), "bytea"),
    (DateType(), "date"),
    (TimeType(), "time"),
    (JsonbType(), "jsonb"),
    (GeographyType(), "geography"),
    (GeometryType(), "geometry"),
]


class TestSqlName:
    @pytest.mark.parametrize("datatype, expected", SCALAR_SQL_NAMES)
    def test_scalar_sql_names(self, datatype, expected):
        assert datatype.sql_name == expected

    @pytest.mark.parametrize("datatype, expected", SCALAR_SQL_NAMES)
    def test_str_is_sql_name(self, datatype, expected):
        assert str(datatype) == expected

    def test_base_class_sql_name_not_implemented(self):
        with pytest.raises(NotImplementedError, match="must define sql_name"):
            _ = DataType().sql_name


class TestNumericHierarchy:
    @pytest.mark.parametrize(
        "datatype",
        [ByteType(), ShortType(), IntegerType(), LongType()],
    )
    def test_integral_types(self, datatype):
        assert isinstance(datatype, _IntegralType)
        assert isinstance(datatype, _NumericType)
        assert not isinstance(datatype, _FractionalType)

    @pytest.mark.parametrize("datatype", [FloatType(), DoubleType(), DecimalType()])
    def test_fractional_types(self, datatype):
        assert isinstance(datatype, _FractionalType)
        assert isinstance(datatype, _NumericType)
        assert not isinstance(datatype, _IntegralType)

    @pytest.mark.parametrize(
        "datatype", [BooleanType(), StringType(), DateType(), JsonbType()]
    )
    def test_non_numeric_types(self, datatype):
        assert not isinstance(datatype, _NumericType)


class TestImmutabilityAndEquality:
    ALL = [
        BooleanType(),
        ByteType(),
        ShortType(),
        IntegerType(),
        LongType(),
        FloatType(),
        DoubleType(),
        DecimalType(10, 2),
        StringType(64),
        BinaryType(),
        DateType(),
        TimeType(),
        TimestampType(),
        JsonbType(),
        GeographyType(),
        GeometryType(),
        ArrayType(LongType()),
        VectorType(float, 8),
        StructType([StructField("a", LongType())]),
    ]

    @pytest.mark.parametrize("datatype", ALL)
    def test_frozen(self, datatype):
        with pytest.raises(dataclasses.FrozenInstanceError):
            datatype.some_new_attr = 1

    @pytest.mark.parametrize("datatype", ALL)
    def test_hashable_and_deduplicated(self, datatype):
        twin = type(datatype)(
            *[getattr(datatype, f.name) for f in dataclasses.fields(datatype)]
        )
        assert datatype == twin
        assert len({datatype, twin}) == 1

    def test_different_types_not_equal(self):
        assert IntegerType() != LongType()
        assert StringType() != StringType(10)
        assert DecimalType(10, 2) != DecimalType(10, 3)

    def test_struct_field_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            StructField("a", LongType()).name = "b"


class TestTypeNameAndSimpleString:
    @pytest.mark.parametrize(
        "datatype, expected",
        [
            (LongType(), "long"),
            (StringType(), "string"),
            (DoubleType(), "double"),
            (ArrayType(LongType()), "array"),
            (JsonbType(), "jsonb"),
            (StructType(), "struct"),
        ],
    )
    def test_type_name(self, datatype, expected):
        assert datatype.type_name == expected

    @pytest.mark.parametrize(
        "datatype, expected",
        [
            (LongType(), "long"),
            (StringType(), "string"),
            (StringType(20), "string(20)"),
            (DecimalType(), "decimal(38,0)"),
            (DecimalType(None, None), "decimal"),
            (TimestampType(), "timestamp"),
            (TimestampType(TimestampTimeZone.TZ), "timestamp_tz"),
            (ArrayType(IntegerType()), "array<integer>"),
            (ArrayType(ArrayType(StringType())), "array<array<string>>"),
            (VectorType(float, 3), "vector<float,3>"),
        ],
    )
    def test_simple_string(self, datatype, expected):
        assert datatype.simple_string() == expected

    def test_struct_simple_string(self):
        schema = StructType(
            [StructField("id", LongType()), StructField("name", StringType())]
        )
        assert schema.simple_string() == "struct<id:long,name:string>"


class TestDecimalType:
    def test_default_precision_and_scale(self):
        assert DecimalType().precision == 38
        assert DecimalType().scale == 0
        assert DecimalType().sql_name == "numeric(38,0)"

    def test_explicit_precision_and_scale(self):
        assert DecimalType(10, 2).sql_name == "numeric(10,2)"

    def test_unconstrained(self):
        assert DecimalType(None, None).sql_name == "numeric"

    @pytest.mark.parametrize("precision, scale", [(38, None), (None, 0)])
    def test_half_none_rejected(self, precision, scale):
        with pytest.raises(InvalidArgumentError, match="both be set or both be None"):
            DecimalType(precision, scale)

    def test_precision_must_be_positive(self):
        with pytest.raises(InvalidArgumentError, match="precision must be >= 1"):
            DecimalType(0, 0)

    def test_scale_must_be_non_negative(self):
        with pytest.raises(InvalidArgumentError, match="scale must be >= 0"):
            DecimalType(10, -1)

    def test_scale_cannot_exceed_precision(self):
        with pytest.raises(InvalidArgumentError, match="exceeds precision"):
            DecimalType(10, 11)

    @pytest.mark.parametrize("precision, scale", [(True, 0), (10, True)])
    def test_bool_rejected(self, precision, scale):
        with pytest.raises(InvalidArgumentError, match="must be an int"):
            DecimalType(precision, scale)


class TestStringType:
    def test_unbounded_is_text(self):
        assert StringType().sql_name == "text"

    def test_bounded_is_varchar(self):
        assert StringType(64).sql_name == "varchar(64)"

    def test_length_must_be_positive(self):
        with pytest.raises(InvalidArgumentError, match="positive int"):
            StringType(0)

    def test_bool_length_rejected(self):
        # isinstance(True, int) is True, so bool must be rejected explicitly.
        with pytest.raises(InvalidArgumentError, match="positive int"):
            StringType(True)


class TestTimestampType:
    @pytest.mark.parametrize(
        "timezone, expected",
        [
            (TimestampTimeZone.DEFAULT, "timestamp"),
            (TimestampTimeZone.NTZ, "timestamp"),
            (TimestampTimeZone.TZ, "timestamptz"),
        ],
    )
    def test_sql_name(self, timezone, expected):
        assert TimestampType(timezone).sql_name == expected

    def test_default_is_normalized_to_ntz(self):
        # DEFAULT and NTZ are indistinguishable in Hologres, so DEFAULT is
        # normalized on construction to keep equality and round-trips stable.
        assert TimestampType().timezone is TimestampTimeZone.NTZ
        assert TimestampType() == TimestampType(TimestampTimeZone.NTZ)
        assert TimestampType(TimestampTimeZone.DEFAULT) == TimestampType()

    def test_ltz_unsupported(self):
        with pytest.raises(UnsupportedOperationError, match="local-time-zone"):
            TimestampType(TimestampTimeZone.LTZ)

    def test_timezone_must_be_enum(self):
        with pytest.raises(InvalidArgumentError, match="must be a TimestampTimeZone"):
            TimestampType("tz")


class TestByteType:
    def test_compiles_to_single_byte_char(self):
        assert ByteType().sql_name == '"char"'

    def test_is_integral_type(self):
        assert isinstance(ByteType(), _IntegralType)


class TestArrayType:
    def test_element_sql_name_suffixed(self):
        assert ArrayType(StringType()).sql_name == "text[]"

    def test_nesting(self):
        assert ArrayType(ArrayType(IntegerType())).sql_name == "integer[][]"

    def test_bounded_string_element(self):
        assert ArrayType(StringType(10)).sql_name == "varchar(10)[]"

    @pytest.mark.parametrize("bad", ["text", int, None])
    def test_element_must_be_datatype(self, bad):
        with pytest.raises(InvalidArgumentError, match="must be a DataType"):
            ArrayType(bad)


class TestVectorType:
    def test_sql_name(self):
        assert VectorType(float, 768).sql_name == "vector(768)"

    def test_element_type_defaults_to_float(self):
        assert VectorType(dimension=3).element_type is float

    def test_int_element_unsupported(self):
        with pytest.raises(UnsupportedOperationError, match="32-bit floats"):
            VectorType(int, 3)

    @pytest.mark.parametrize("dimension", [0, -1, "3", None, True])
    def test_dimension_must_be_positive_int(self, dimension):
        with pytest.raises(InvalidArgumentError, match="dimension must be >= 1"):
            VectorType(float, dimension)


class TestStructField:
    def test_str_is_ddl_fragment(self):
        assert str(StructField("word", StringType())) == "word text"

    def test_nullable_defaults_true(self):
        assert StructField("a", LongType()).nullable is True

    @pytest.mark.parametrize("bad_name", ["", None, 123])
    def test_name_must_be_non_empty_string(self, bad_name):
        with pytest.raises(InvalidArgumentError, match="non-empty string"):
            StructField(bad_name, LongType())

    def test_datatype_must_be_datatype(self):
        with pytest.raises(InvalidArgumentError, match="must be a DataType"):
            StructField("a", "bigint")


class TestStructType:
    @pytest.fixture
    def schema(self):
        return StructType(
            [StructField("id", LongType()), StructField("word", StringType())]
        )

    def test_to_ddl(self, schema):
        assert schema.to_ddl() == "id bigint, word text"

    def test_empty_struct(self):
        empty = StructType()
        assert empty.to_ddl() == ""
        assert len(empty) == 0
        assert empty.names == ()

    def test_sql_name_is_record(self, schema):
        # A struct has no standalone SQL type name; to_ddl() emits the field list.
        assert schema.sql_name == "record"

    def test_names(self, schema):
        assert schema.names == ("id", "word")

    def test_len_and_iter(self, schema):
        assert len(schema) == 2
        assert [f.name for f in schema] == ["id", "word"]

    def test_getitem_by_index(self, schema):
        assert schema[0].name == "id"
        assert schema[-1].name == "word"

    def test_getitem_by_name(self, schema):
        assert schema["word"].datatype == StringType()

    def test_getitem_index_out_of_range(self, schema):
        with pytest.raises(IndexError):
            _ = schema[5]

    def test_getitem_unknown_name(self, schema):
        with pytest.raises(KeyError, match="No field named 'nope'"):
            _ = schema["nope"]

    def test_list_is_normalized_to_tuple(self, schema):
        assert isinstance(schema.fields, tuple)

    def test_duplicate_names_rejected(self):
        with pytest.raises(
            InvalidArgumentError, match=r"duplicate field names: \['a'\]"
        ):
            StructType([StructField("a", LongType()), StructField("a", StringType())])

    def test_fields_must_be_struct_fields(self):
        with pytest.raises(InvalidArgumentError, match="must be StructField instances"):
            StructType([("a", LongType())])


class TestFcSupportedTypes:
    def test_matches_function_compute_whitelist(self):
        # Per the design doc: BOOLEAN / INTEGER / BIGINT / REAL /
        # DOUBLE PRECISION / TEXT and their array forms.
        expected_scalars = {
            "boolean",
            "integer",
            "bigint",
            "real",
            "double precision",
            "text",
        }
        actual = {t.sql_name for t in FC_SUPPORTED_TYPES}
        assert actual == expected_scalars | {f"{s}[]" for s in expected_scalars}

    @pytest.mark.parametrize(
        "datatype",
        [
            DecimalType(38, 2),
            TimestampType(),
            DateType(),
            JsonbType(),
            ShortType(),
            ByteType(),
            StringType(10),
            BinaryType(),
        ],
    )
    def test_excluded_types(self, datatype):
        assert datatype not in FC_SUPPORTED_TYPES

    def test_membership_works_by_value(self):
        assert LongType() in FC_SUPPORTED_TYPES
        assert ArrayType(StringType()) in FC_SUPPORTED_TYPES


class TestFromPythonType:
    @pytest.mark.parametrize(
        "python_type, expected",
        [
            (bool, BooleanType()),
            (int, LongType()),
            (float, DoubleType()),
            (str, StringType()),
            (bytes, BinaryType()),
            (decimal.Decimal, DecimalType(38, 18)),
            (datetime.date, DateType()),
            (datetime.time, TimeType()),
            (datetime.datetime, TimestampType()),
        ],
    )
    def test_scalars(self, python_type, expected):
        assert from_python_type(python_type) == expected

    def test_bool_wins_over_int(self):
        # bool subclasses int in Python; the mapping must not widen it.
        assert from_python_type(bool) == BooleanType()

    def test_decimal_keeps_fractional_digits(self):
        # DecimalType()'s default scale of 0 would truncate; inference must not
        # use it.
        assert from_python_type(decimal.Decimal).scale == 18

    def test_datatype_passes_through(self):
        given = StringType(32)
        assert from_python_type(given) is given

    @pytest.mark.parametrize(
        "annotation, expected_sql",
        [
            (typing.List[int], "bigint[]"),
            (typing.Set[str], "text[]"),
            (typing.FrozenSet[float], "double precision[]"),
            (typing.Tuple[bool], "boolean[]"),
            (typing.List[typing.List[int]], "bigint[][]"),
        ],
    )
    def test_collections_become_arrays(self, annotation, expected_sql):
        assert from_python_type(annotation).sql_name == expected_sql

    def test_variadic_tuple_becomes_array(self):
        # Tuple[X, ...] is the idiomatic homogeneous tuple.
        assert from_python_type(typing.Tuple[int, ...]).sql_name == "bigint[]"

    @pytest.mark.parametrize(
        "annotation", [typing.Optional[str], typing.Union[str, None]]
    )
    def test_optional_is_unwrapped(self, annotation):
        assert from_python_type(annotation) == StringType()

    @pytest.mark.skipif(
        sys.version_info < (3, 10),
        reason="PEP 604 (X | None) operator requires Python 3.10+",
    )
    def test_pep604_optional_is_unwrapped(self):
        assert from_python_type(int | None) == LongType()

    def test_pep585_builtin_generic(self):
        assert from_python_type(list[str]).sql_name == "text[]"

    def test_multi_member_union_rejected(self):
        with pytest.raises(InvalidArgumentError, match="only Optional"):
            from_python_type(typing.Union[int, str])

    def test_multi_arg_generic_rejected(self):
        with pytest.raises(InvalidArgumentError, match="exactly one element type"):
            from_python_type(typing.Tuple[int, str])

    def test_unsupported_generic_rejected(self):
        with pytest.raises(InvalidArgumentError, match="Cannot map generic type"):
            from_python_type(typing.Dict[str, int])

    @pytest.mark.parametrize("bad", [complex, object, type(None)])
    def test_unsupported_scalar_rejected(self, bad):
        with pytest.raises(InvalidArgumentError, match="Cannot map Python type"):
            from_python_type(bad)


class TestParseType:
    @pytest.mark.parametrize(
        "name, expected",
        [
            ("bool", BooleanType()),
            ("boolean", BooleanType()),
            ('"char"', ByteType()),
            ("int2", ShortType()),
            ("smallint", ShortType()),
            ("int", IntegerType()),
            ("int4", IntegerType()),
            ("integer", IntegerType()),
            ("int8", LongType()),
            ("bigint", LongType()),
            ("float4", FloatType()),
            ("real", FloatType()),
            ("float8", DoubleType()),
            ("double precision", DoubleType()),
            ("text", StringType()),
            ("varchar", StringType()),
            ("date", DateType()),
            ("time", TimeType()),
            ("timestamp", TimestampType()),
            ("timestamptz", TimestampType(TimestampTimeZone.TZ)),
            ("timestamp with time zone", TimestampType(TimestampTimeZone.TZ)),
            ("timestamp without time zone", TimestampType()),
            ("jsonb", JsonbType()),
            ("bytea", BinaryType()),
            ("geography", GeographyType()),
            ("geometry", GeometryType()),
            ("bpchar", StringType()),
        ],
    )
    def test_scalar_aliases(self, name, expected):
        assert parse_type(name) == expected

    @pytest.mark.parametrize(
        "name, expected_sql",
        [
            ("numeric", "numeric"),
            ("decimal", "numeric"),
            ("numeric(10)", "numeric(10,0)"),
            ("numeric(10,2)", "numeric(10,2)"),
            ("decimal(38, 6)", "numeric(38,6)"),
            ("varchar(64)", "varchar(64)"),
            ("character varying(20)", "varchar(20)"),
            ("vector(768)", "vector(768)"),
            ("char(10)", "varchar(10)"),
            ("character(5)", "varchar(5)"),
            ("bpchar(8)", "varchar(8)"),
        ],
    )
    def test_parametric(self, name, expected_sql):
        assert parse_type(name).sql_name == expected_sql

    def test_bare_numeric_is_unconstrained(self):
        # PostgreSQL reports plain "numeric" for unconstrained columns, which
        # differs from DecimalType()'s default (38, 0).
        parsed = parse_type("numeric")
        assert parsed.precision is None
        assert parsed != DecimalType()

    @pytest.mark.parametrize(
        "name, expected_sql",
        [
            ("text[]", "text[]"),
            ("integer[][]", "integer[][]"),
            ("numeric(10,2)[]", "numeric(10,2)[]"),
            ("timestamptz[]", "timestamptz[]"),
        ],
    )
    def test_arrays(self, name, expected_sql):
        assert parse_type(name).sql_name == expected_sql

    @pytest.mark.parametrize(
        "name", ["BIGINT", "  bigint  ", "Double   Precision", "VARCHAR(64)"]
    )
    def test_case_and_whitespace_insensitive(self, name):
        assert parse_type(name).sql_name in (
            "bigint",
            "double precision",
            "varchar(64)",
        )

    def test_bare_vector_requires_dimension(self):
        with pytest.raises(InvalidArgumentError, match="explicit dimension"):
            parse_type("vector")

    @pytest.mark.parametrize("name", ["", "   ", None, 42])
    def test_empty_or_non_string(self, name):
        with pytest.raises(InvalidArgumentError, match="non-empty string"):
            parse_type(name)

    @pytest.mark.parametrize(
        "name",
        [
            "nosuchtype",
            "json",  # only jsonb is modelled
            "hstore",
            "varchar(10,2)",
            "vector(3,4)",
            "money",
        ],
    )
    def test_unrecognized(self, name):
        with pytest.raises(InvalidArgumentError, match="Unrecognized"):
            parse_type(name)

    @pytest.mark.parametrize(
        "datatype",
        [
            BooleanType(),
            ShortType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            StringType(),
            StringType(20),
            BinaryType(),
            DateType(),
            TimeType(),
            TimestampType(),
            TimestampType(TimestampTimeZone.TZ),
            JsonbType(),
            GeographyType(),
            GeometryType(),
            ByteType(),
            DecimalType(10, 2),
            VectorType(float, 5),
            ArrayType(LongType()),
        ],
    )
    def test_sql_name_round_trips(self, datatype):
        assert parse_type(datatype.sql_name) == datatype


class TestArrowConversion:
    @pytest.mark.parametrize(
        "datatype, arrow_type",
        [
            (BooleanType(), pa.bool_()),
            (ByteType(), pa.int8()),
            (ShortType(), pa.int16()),
            (IntegerType(), pa.int32()),
            (LongType(), pa.int64()),
            (FloatType(), pa.float32()),
            (DoubleType(), pa.float64()),
            (StringType(), pa.utf8()),
            (StringType(20), pa.utf8()),
            (BinaryType(), pa.binary()),
            (DateType(), pa.date32()),
            (TimeType(), pa.time64("us")),
            (DecimalType(10, 2), pa.decimal128(10, 2)),
            (DecimalType(None, None), pa.decimal128(38, 0)),
            (TimestampType(), pa.timestamp("us")),
            (TimestampType(TimestampTimeZone.TZ), pa.timestamp("us", tz="UTC")),
            (JsonbType(), pa.utf8()),
            (GeographyType(), pa.utf8()),
            (GeometryType(), pa.utf8()),
            (ArrayType(LongType()), pa.list_(pa.int64())),
            (VectorType(float, 3), pa.list_(pa.float32(), 3)),
        ],
    )
    def test_to_arrow(self, datatype, arrow_type):
        assert datatype.to_arrow() == arrow_type

    def test_to_arrow_struct(self):
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType(), nullable=False),
            ]
        )
        assert schema.to_arrow() == pa.struct(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("name", pa.utf8(), nullable=False),
            ]
        )

    def test_to_arrow_unsupported(self):
        class WeirdType(DataType):
            @property
            def sql_name(self):
                return "weird"

        with pytest.raises(UnsupportedOperationError, match="No Arrow equivalent"):
            WeirdType().to_arrow()

    @pytest.mark.parametrize(
        "arrow_type, expected",
        [
            (pa.bool_(), BooleanType()),
            (pa.int8(), ByteType()),
            (pa.int16(), ShortType()),
            (pa.int32(), IntegerType()),
            (pa.int64(), LongType()),
            (pa.float32(), FloatType()),
            (pa.float64(), DoubleType()),
            (pa.utf8(), StringType()),
            (pa.large_utf8(), StringType()),
            (pa.binary(), BinaryType()),
            (pa.large_binary(), BinaryType()),
            (pa.date32(), DateType()),
            (pa.date64(), DateType()),
            (pa.time64("us"), TimeType()),
            (pa.decimal128(10, 2), DecimalType(10, 2)),
            (pa.timestamp("us"), TimestampType(TimestampTimeZone.NTZ)),
            (pa.timestamp("us", tz="UTC"), TimestampType(TimestampTimeZone.TZ)),
            (pa.list_(pa.int64()), ArrayType(LongType())),
            (pa.large_list(pa.utf8()), ArrayType(StringType())),
            (pa.list_(pa.float32(), 4), VectorType(float, 4)),
            (pa.list_(pa.int32(), 4), ArrayType(IntegerType())),
        ],
    )
    def test_from_arrow_type(self, arrow_type, expected):
        assert from_arrow_type(arrow_type) == expected

    def test_from_arrow_struct(self):
        arrow_struct = pa.struct(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("name", pa.utf8(), nullable=False),
            ]
        )
        assert from_arrow_type(arrow_struct) == StructType(
            [
                StructField("id", LongType(), nullable=True),
                StructField("name", StringType(), nullable=False),
            ]
        )

    def test_from_arrow_unsupported(self):
        with pytest.raises(UnsupportedOperationError, match="No Hologres equivalent"):
            from_arrow_type(pa.duration("s"))

    def test_to_arrow_without_pyarrow(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pyarrow":
                raise ImportError("No module named 'pyarrow'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(ImportError, match="pyarrow is required"):
            LongType().to_arrow()
