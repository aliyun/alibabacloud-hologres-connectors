"""Integration tests for HoloClient.

These tests require a running Hologres/PostgreSQL database.
They are similar to the Java HoloClientTest cases.

To run integration tests:
    export HOLO_TEST_HOST=localhost
    export HOLO_TEST_PORT=5432
    export HOLO_TEST_DATABASE=postgres  # Base database to connect for creating test db
    export HOLO_TEST_USERNAME=test_user
    export HOLO_TEST_PASSWORD=test_pass
    pytest tests/test_integration.py -v

Or skip them:
    pytest tests/ -v --ignore=tests/test_integration.py
"""

import datetime
import json
import os
import time
from decimal import Decimal

import psycopg
import pytest

from hologres import (
    AsyncHoloClient,
    HoloClient,
    HoloConfig,
    Put,
    Get,
    Scan,
    SortKeys,
    TableSchema,
    OnConflictAction,
    MutationType,
    HoloClientException,
    ExceptionCode,
    CopyMode,
    CopyFormat,
    Column,
    TableName,
)

_ALL_TYPE_COLUMNS_DDL = """\
    col_smallint SMALLINT,
    col_int INT,
    col_bigint BIGINT,
    col_bool BOOL,
    col_float4 FLOAT4,
    col_float8 FLOAT8,
    col_decimal NUMERIC(6,5),
    col_timestamp TIMESTAMP,
    col_timestamptz TIMESTAMPTZ,
    col_date DATE,
    col_time TIME,
    col_json JSON,
    col_jsonb JSONB,
    col_bytea BYTEA,
    col_char CHAR(5),
    col_varchar VARCHAR(20),
    col_text TEXT,
    col_int_array int4[],
    col_bigint_array int8[],
    col_float_array float4[],
    col_bool_array bool[],
    col_text_array text[]\
"""


_ALL_TYPE_COLUMNS_DDL_NO_TIMESTAMPTZ = """\
    col_smallint SMALLINT,
    col_int INT,
    col_bigint BIGINT,
    col_bool BOOL,
    col_float4 FLOAT4,
    col_float8 FLOAT8,
    col_decimal NUMERIC(6,5),
    col_timestamp TIMESTAMP,
    col_date DATE,
    col_time TIME,
    col_json JSON,
    col_jsonb JSONB,
    col_bytea BYTEA,
    col_char CHAR(5),
    col_varchar VARCHAR(20),
    col_text TEXT,
    col_int_array int4[],
    col_bigint_array int8[],
    col_float_array float4[],
    col_bool_array bool[],
    col_text_array text[]\
"""


def _create_all_type_table(client, table_name: str, include_timestamptz: bool = True):
    """Create a table with all supported data type columns + id PK."""
    ddl = (
        _ALL_TYPE_COLUMNS_DDL
        if include_timestamptz
        else _ALL_TYPE_COLUMNS_DDL_NO_TIMESTAMPTZ
    )

    def create(conn):
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    id INT NOT NULL,
                    {ddl},
                    PRIMARY KEY (id)
                )
            """)

    client.sql(create)


def _make_all_type_values(i: int) -> dict:
    """Generate test values for row index *i* (0-9), matching Java DataTypeTestUtil."""
    return {
        "col_smallint": i,
        "col_int": i,
        "col_bigint": i,
        "col_bool": i % 2 == 0,
        "col_float4": float(i),
        "col_float8": float(i),
        "col_decimal": Decimal(str(i)),
        "col_timestamp": datetime.datetime(2024, 1, 1, 0, 0, i),
        "col_timestamptz": datetime.datetime(
            2024, 1, 1, 0, 0, i, tzinfo=datetime.timezone.utc
        ),
        "col_date": datetime.date(2024, 6, 15),
        "col_time": datetime.time(10, 30, i),
        "col_json": json.dumps({"a": str(i)}),
        "col_jsonb": json.dumps({"a": str(i)}),
        "col_bytea": bytes([i, i + 1, i + 2]),
        "col_char": str(i),
        "col_varchar": str(i),
        "col_text": str(i),
        "col_int_array": [i, i + 1],
        "col_bigint_array": [i, i + 1],
        "col_float_array": [float(i), float(i + 1)],
        "col_bool_array": [i % 2 == 0, i % 2 != 0],
        "col_text_array": [str(i), str(i + 1)],
    }


def _verify_all_type_record(record, i: int, *, skip_cols: set = None):
    """Assert that a Record returned by client.get() matches expected values for row *i*.

    *skip_cols* is an optional set of column names to skip verification for
    (e.g. when a column was not written due to format limitations).
    """
    skip = skip_cols or set()
    vals = _make_all_type_values(i)
    assert record is not None, f"row i={i} should exist"

    assert record.get_object("col_smallint") == vals["col_smallint"]
    assert record.get_object("col_int") == vals["col_int"]
    assert record.get_object("col_bigint") == vals["col_bigint"]
    assert record.get_object("col_bool") == vals["col_bool"]
    # float4 may lose precision
    assert abs(record.get_object("col_float4") - vals["col_float4"]) < 1e-5
    assert record.get_object("col_float8") == vals["col_float8"]
    # decimal comes back with trailing zeros
    assert Decimal(str(record.get_object("col_decimal"))) == Decimal(f"{i}.00000")

    assert record.get_object("col_timestamp") == vals["col_timestamp"]
    # timestamptz: server returns in session timezone (e.g. PRC +08:00) but
    # Python tz-aware datetime == compares the underlying instant correctly.
    if "col_timestamptz" not in skip:
        assert record.get_object("col_timestamptz") == vals["col_timestamptz"]
    assert record.get_object("col_date") == vals["col_date"]
    assert record.get_object("col_time") == vals["col_time"]

    if "col_char" not in skip:
        assert record.get_object("col_char") == str(i).ljust(5)  # right-padded
    assert record.get_object("col_varchar") == vals["col_varchar"]
    assert record.get_object("col_text") == vals["col_text"]
    assert bytes(record.get_object("col_bytea")) == vals["col_bytea"]

    # json / jsonb
    json_val = record.get_object("col_json")
    if isinstance(json_val, str):
        json_val = json.loads(json_val)
    assert json_val == {"a": str(i)}
    jsonb_val = record.get_object("col_jsonb")
    if isinstance(jsonb_val, str):
        jsonb_val = json.loads(jsonb_val)
    assert jsonb_val == {"a": str(i)}

    # arrays
    assert list(record.get_object("col_int_array")) == vals["col_int_array"]
    assert list(record.get_object("col_bigint_array")) == vals["col_bigint_array"]
    assert list(record.get_object("col_text_array")) == vals["col_text_array"]
    assert list(record.get_object("col_bool_array")) == vals["col_bool_array"]
    # float array: compare with tolerance
    fa = list(record.get_object("col_float_array"))
    for a, b in zip(fa, vals["col_float_array"]):
        assert abs(a - b) < 1e-5


def _verify_all_type_null_record(record, *, skip_cols: set = None):
    """Assert that all non-PK columns are None in the null row."""
    skip = skip_cols or set()
    assert record is not None
    for col in [
        "col_smallint",
        "col_int",
        "col_bigint",
        "col_bool",
        "col_float4",
        "col_float8",
        "col_decimal",
        "col_timestamp",
        "col_timestamptz",
        "col_date",
        "col_time",
        "col_json",
        "col_jsonb",
        "col_bytea",
        "col_char",
        "col_varchar",
        "col_text",
        "col_int_array",
        "col_bigint_array",
        "col_float_array",
        "col_bool_array",
        "col_text_array",
    ]:
        if col in skip:
            continue
        assert record.get_object(col) is None, f"{col} should be None in null row"


def get_base_config():
    """Get base configuration from environment variables for connecting to create test db."""
    host = os.environ.get("HOLO_TEST_HOST")
    port_str = os.environ.get("HOLO_TEST_PORT")
    database = os.environ.get("HOLO_TEST_DATABASE")
    username = os.environ.get("HOLO_TEST_USERNAME")
    password = os.environ.get("HOLO_TEST_PASSWORD")

    missing = []
    if not host:
        missing.append("HOLO_TEST_HOST")
    if not port_str:
        missing.append("HOLO_TEST_PORT")
    if not database:
        missing.append("HOLO_TEST_DATABASE")
    if not username:
        missing.append("HOLO_TEST_USERNAME")
    if not password:
        missing.append("HOLO_TEST_PASSWORD")
    if missing:
        raise EnvironmentError(
            f"Integration test environment not configured. "
            f"Missing environment variables: {', '.join(missing)}"
        )

    port = int(port_str)
    return HoloConfig(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
    )


def get_test_db_name():
    """Generate a unique test database name with timestamp."""
    timestamp = int(time.time())
    return f"holo_client_py_test_{timestamp}"


@pytest.fixture(scope="module")
def test_database():
    """Create a test database for the module and drop it after tests complete."""
    base_config = get_base_config()
    test_db_name = get_test_db_name()

    # Connect to base database and create test database
    conn = psycopg.connect(
        host=base_config.host,
        port=base_config.port,
        dbname=base_config.database,
        user=base_config.username,
        password=base_config.password,
        autocommit=True,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{test_db_name}"')
            cur.execute(
                f'ALTER DATABASE "{test_db_name}" SET hg_experimental_enable_shard_count_cap = off'
            )
    finally:
        conn.close()

    yield test_db_name

    # Drop test database after all tests complete
    conn = psycopg.connect(
        host=base_config.host,
        port=base_config.port,
        dbname=base_config.database,
        user=base_config.username,
        password=base_config.password,
        autocommit=True,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{test_db_name}"')
    finally:
        conn.close()


@pytest.fixture
def test_config(test_database):
    """Get test configuration for the test database."""
    base_config = get_base_config()
    return HoloConfig(
        host=base_config.host,
        port=base_config.port,
        database=test_database,
        username=base_config.username,
        password=base_config.password,
        write_batch_size=512,
        write_max_interval_ms=5000,
    )


@pytest.fixture
def client(test_config):
    """Create a HoloClient for testing."""
    with HoloClient(test_config) as c:
        yield c


@pytest.fixture
def connection(client):
    """Get a raw connection from the client."""
    return client.sql(lambda conn: conn)


class TestHoloClientBasic:
    """Basic integration tests for HoloClient."""

    def test_get_table_schema(self, client):
        """Test getting table schema."""

        # Create a test table
        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_schema_basic")
                cur.execute("""
                    CREATE TABLE test_schema_basic (
                        id INT NOT NULL,
                        name TEXT,
                        PRIMARY KEY (id)
                    )
                """)

        client.sql(create_table)
        schema = client.get_table_schema("test_schema_basic")
        assert schema.column_count == 2
        assert schema.has_primary_key
        assert "id" in schema.primary_keys
        assert schema.get_column_index("id") == 0
        assert schema.get_column_index("name") == 1

    def test_get_table_schema_qualified_name(self, client):
        """Test getting table schema with qualified name."""

        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
                cur.execute("DROP TABLE IF EXISTS test_schema.qualified_table")
                cur.execute("""
                    CREATE TABLE test_schema.qualified_table (
                        id INT NOT NULL PRIMARY KEY
                    )
                """)

        client.sql(create_table)
        schema = client.get_table_schema("test_schema.qualified_table")
        assert schema.schema_name == "test_schema"
        assert schema.table_name == "qualified_table"


class TestHoloClientPut:
    """Tests for HoloClient put operations."""

    def _create_test_table(self, client, table_name: str, extra_columns: str = ""):
        """Create a test table."""

        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        name TEXT{extra_columns},
                        PRIMARY KEY (id)
                    )
                """)

        client.sql(create_table)

    def test_put_insert_or_replace(self, client, test_config):
        """Test INSERT_OR_REPLACE on conflict action."""
        table_name = "test_put_replace"
        self._create_test_table(client, table_name, ", address TEXT")

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE

        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            # Insert first record
            put = Put(schema)
            put.set_object("id", 0)
            put.set_object("name", "name0")
            put.set_object("address", "address")
            c.put(put)

            # Insert second record
            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("name", "name1")
            c.put(put)

            # Replace first record
            put = Put(schema)
            put.set_object("id", 0)
            put.set_object("name", "name3")
            c.put(put)

            c.flush()

            # Verify results
            get = Get.builder(schema).set_primary_key("id", 0).build()
            record = c.get(get)
            assert record is not None
            assert record.get_object("name") == "name3"
            assert record.get_object("address") is None

            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = c.get(get)
            assert record is not None
            assert record.get_object("name") == "name1"
            assert record.get_object("address") is None

    def test_put_insert_or_ignore(self, client, test_config):
        """Test INSERT_OR_IGNORE on conflict action."""
        table_name = "test_put_ignore"
        self._create_test_table(client, table_name)

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_IGNORE

        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            put = Put(schema)
            put.set_object("id", 0)
            put.set_object("name", "name0")
            c.put(put)

            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("name", "name1")
            c.put(put)

            # This should be ignored
            put = Put(schema)
            put.set_object("id", 0)
            put.set_object("name", "name3")
            c.put(put)

            c.flush()

            get = Get.builder(schema).set_primary_key("id", 0).build()
            record = c.get(get)
            assert record.get_object("name") == "name0"

    def test_put_insert_or_update(self, client, test_config):
        """Test INSERT_OR_UPDATE on conflict action."""
        table_name = "test_put_update"
        self._create_test_table(client, table_name, ", address TEXT")

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_UPDATE

        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            put = Put(schema)
            put.set_object("id", 0)
            put.set_object("name", "name0")
            c.put(put)

            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("name", "name1")
            put.set_object("address", "address2")
            c.put(put)

            # Update second record - only set address
            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("address", "address3")
            c.put(put)

            c.flush()

            get = Get.builder(schema).set_primary_key("id", 0).build()
            record = c.get(get)
            assert record.get_object("name") == "name0"
            assert record.get_object("address") is None

            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = c.get(get)
            assert record.get_object("name") == "name1"
            assert record.get_object("address") == "address3"

    @pytest.mark.parametrize("use_fixed_fe", [False, True])
    def test_put_all_types(self, client, test_config, use_fixed_fe):
        """Test put/insert with all supported data types."""
        table_name = f"test_put_all_types_{'fixed' if use_fixed_fe else 'fe'}"
        skip_cols = set()

        if use_fixed_fe:
            # Verify timestamptz is rejected on FixedFE at schema load time
            _create_all_type_table(client, f"{table_name}_tz")
            test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
            test_config.use_fixed_fe = True
            with HoloClient(test_config) as c:
                with pytest.raises(HoloClientException) as exc_info:
                    c.get_table_schema(f"{table_name}_tz")
                assert exc_info.value.code == ExceptionCode.INVALID_REQUEST
                assert "timestamptz" in str(exc_info.value)

            # Now test all other types without timestamptz
            _create_all_type_table(client, table_name, include_timestamptz=False)
            skip_cols = {"col_timestamptz"}
        else:
            _create_all_type_table(client, table_name)

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
        test_config.use_fixed_fe = use_fixed_fe
        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            # Insert 10 rows with typed values
            for i in range(10):
                put = Put(schema)
                put.set_object("id", i)
                for col_name, val in _make_all_type_values(i).items():
                    if col_name not in skip_cols:
                        put.set_object(col_name, val)
                c.put(put)

            # Insert null row (only PK set)
            put = Put(schema)
            put.set_object("id", 10)
            c.put(put)
            c.flush()

            # Verify all rows via get
            for i in range(10):
                get = Get.builder(schema).set_primary_key("id", i).build()
                record = c.get(get)
                _verify_all_type_record(record, i, skip_cols=skip_cols)

            # Verify null row
            get = Get.builder(schema).set_primary_key("id", 10).build()
            record = c.get(get)
            _verify_all_type_null_record(record, skip_cols=skip_cols)


class TestHoloClientGet:
    """Tests for HoloClient get operations."""

    def _create_and_populate_table(self, client, table_name: str):
        """Create and populate a test table."""

        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        name TEXT,
                        age INT,
                        PRIMARY KEY (id)
                    )
                """)
                cur.execute(f"""
                    INSERT INTO {table_name} (id, name, age) VALUES
                        (1, 'Alice', 30),
                        (2, 'Bob', 25),
                        (3, 'Charlie', 35)
                """)

        client.sql(create_table)

    def test_get_single_record(self, client):
        """Test getting a single record."""
        table_name = "test_get_single"
        self._create_and_populate_table(client, table_name)

        schema = client.get_table_schema(table_name)
        get = Get.builder(schema).set_primary_key("id", 1).build()
        record = client.get(get)

        assert record is not None
        assert record.get_object("id") == 1
        assert record.get_object("name") == "Alice"
        assert record.get_object("age") == 30

    def test_get_nonexistent_record(self, client):
        """Test getting a record that doesn't exist."""
        table_name = "test_get_nonexistent"
        self._create_and_populate_table(client, table_name)

        schema = client.get_table_schema(table_name)
        get = Get.builder(schema).set_primary_key("id", 999).build()
        record = client.get(get)

        assert record is None

    def test_get_selected_columns(self, client):
        """Test getting specific columns."""
        table_name = "test_get_columns"
        self._create_and_populate_table(client, table_name)

        schema = client.get_table_schema(table_name)
        get = (
            Get.builder(schema)
            .set_primary_key("id", 1)
            .with_selected_column("name")
            .build()
        )
        record = client.get(get)

        assert record is not None
        assert record.get_object("name") == "Alice"

    def test_get_multiple_records(self, client):
        """Test getting multiple records at once."""
        table_name = "test_get_multi"
        self._create_and_populate_table(client, table_name)

        schema = client.get_table_schema(table_name)

        get1 = Get.builder(schema).set_primary_key("id", 1).build()
        get2 = Get.builder(schema).set_primary_key("id", 2).build()
        get3 = Get.builder(schema).set_primary_key("id", 999).build()

        results = client.get_many([get1, get2, get3])

        assert results[0] is not None
        assert results[0].get_object("name") == "Alice"
        assert results[1] is not None
        assert results[1].get_object("name") == "Bob"
        assert results[2] is None

    @pytest.mark.parametrize("use_fixed_fe", [False, True])
    def test_get_all_types(self, client, test_config, use_fixed_fe):
        """Test get/select with all supported data types (aligned with Java testALLTypeGet)."""
        table_name = f"test_get_all_types_{'fixed' if use_fixed_fe else 'fe'}"
        skip_cols = set()

        if use_fixed_fe:
            # Verify timestamptz is rejected on FixedFE at schema load time
            _create_all_type_table(client, f"{table_name}_tz")
            test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
            test_config.use_fixed_fe = True
            with HoloClient(test_config) as c:
                with pytest.raises(HoloClientException) as exc_info:
                    c.get_table_schema(f"{table_name}_tz")
                assert exc_info.value.code == ExceptionCode.INVALID_REQUEST
                assert "timestamptz" in str(exc_info.value)

            # Now test all other types without timestamptz
            _create_all_type_table(client, table_name, include_timestamptz=False)
            skip_cols = {"col_timestamptz"}
        else:
            _create_all_type_table(client, table_name)

        # Populate via put, then read back
        test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
        test_config.use_fixed_fe = use_fixed_fe
        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)
            for i in range(10):
                put = Put(schema)
                put.set_object("id", i)
                for col_name, val in _make_all_type_values(i).items():
                    if col_name not in skip_cols:
                        put.set_object(col_name, val)
                c.put(put)
            # null row
            put = Put(schema)
            put.set_object("id", 10)
            c.put(put)
            c.flush()

            # Read back via get on the same client
            for i in range(10):
                get = Get.builder(schema).set_primary_key("id", i).build()
                record = c.get(get)
                _verify_all_type_record(record, i, skip_cols=skip_cols)

            # Verify null row
            get = Get.builder(schema).set_primary_key("id", 10).build()
            record = c.get(get)
            _verify_all_type_null_record(record, skip_cols=skip_cols)


class TestAsyncHoloClientGet:
    """Tests for AsyncHoloClient.get() queue-based batching."""

    def _create_and_populate_table(self, client, table_name: str, rows):
        """Create a table with (id PK, name, age) and insert rows using sync client."""

        def setup(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        name TEXT,
                        age INT,
                        PRIMARY KEY (id)
                    )
                """)
                for r in rows:
                    cur.execute(
                        f"INSERT INTO {table_name} (id, name, age) VALUES (%s, %s, %s)",
                        r,
                    )

        client.sql(setup)

    def test_async_get_single_table(self, client, test_config):
        """Test get() for a single table with queue-based batching."""
        import asyncio

        table_name = "test_async_get_single"
        self._create_and_populate_table(
            client,
            table_name,
            [
                (1, "Alice", 30),
                (2, "Bob", 25),
            ],
        )

        async def _run():
            async with AsyncHoloClient(test_config) as ac:
                schema = await ac.get_table_schema(table_name)

                get1 = Get.builder(schema).set_primary_key("id", 1).build()
                get2 = Get.builder(schema).set_primary_key("id", 2).build()
                get3 = Get.builder(schema).set_primary_key("id", 999).build()

                r1, r2, r3 = await asyncio.gather(
                    ac.get(get1), ac.get(get2), ac.get(get3)
                )

                assert r1 is not None
                assert r1.get_object("name") == "Alice"
                assert r2 is not None
                assert r2.get_object("name") == "Bob"
                assert r3 is None

        asyncio.run(_run())

    def test_async_get_multi_table(self, client, test_config):
        """Test get() across multiple tables in a single batch.

        Gets for different tables are submitted to the same queue.
        The reader should group them by table and execute each group
        as a separate batched query.
        """
        import asyncio

        tables = ["test_async_multi_a", "test_async_multi_b", "test_async_multi_c"]
        expected = {}

        for i, table_name in enumerate(tables):
            rows = [(j, f"{table_name}_name_{j}", 20 + j) for j in range(1, 4)]
            self._create_and_populate_table(client, table_name, rows)
            expected[table_name] = {r[0]: r[1] for r in rows}

        async def _run():
            async with AsyncHoloClient(test_config) as ac:
                schemas = {}
                for t in tables:
                    schemas[t] = await ac.get_table_schema(t)

                gets_info = []
                for pk in [1, 2, 3]:
                    for table_name in tables:
                        get = (
                            Get.builder(schemas[table_name])
                            .set_primary_key("id", pk)
                            .build()
                        )
                        gets_info.append((table_name, pk, get))
                for table_name in tables:
                    get = (
                        Get.builder(schemas[table_name])
                        .set_primary_key("id", 999)
                        .build()
                    )
                    gets_info.append((table_name, 999, get))

                results = await asyncio.gather(*(ac.get(g) for _, _, g in gets_info))

                for (table_name, pk, _), record in zip(gets_info, results):
                    if pk == 999:
                        assert record is None, f"{table_name} pk={pk} should be None"
                    else:
                        assert record is not None, f"{table_name} pk={pk} should exist"
                        assert record.get_object("name") == expected[table_name][pk]

        asyncio.run(_run())

    def test_async_get_multi_table_large_batch(self, client, test_config):
        """Test get() with enough gets to fill multiple batches across tables.

        Submits more gets than batch_size (default 128) to verify that
        multi-table batching works correctly under load.
        """
        import asyncio

        tables = ["test_async_batch_a", "test_async_batch_b"]
        row_count = 100

        for table_name in tables:
            rows = [(j, f"{table_name}_{j}", j) for j in range(row_count)]
            self._create_and_populate_table(client, table_name, rows)

        async def _run():
            async with AsyncHoloClient(test_config) as ac:
                schemas = {}
                for t in tables:
                    schemas[t] = await ac.get_table_schema(t)

                gets_info = []
                for pk in range(row_count):
                    for table_name in tables:
                        get = (
                            Get.builder(schemas[table_name])
                            .set_primary_key("id", pk)
                            .build()
                        )
                        gets_info.append((table_name, pk, get))

                results = await asyncio.gather(*(ac.get(g) for _, _, g in gets_info))

                for (table_name, pk, _), record in zip(gets_info, results):
                    assert record is not None, f"{table_name} pk={pk} missing"
                    assert record.get_object("name") == f"{table_name}_{pk}"
                    assert record.get_object("age") == pk

        asyncio.run(_run())


class TestHoloClientCopy:
    """Tests for COPY-based bulk write operations."""

    def _create_test_table(self, client, table_name: str):
        """Create a test table."""

        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        name TEXT,
                        PRIMARY KEY (id)
                    )
                """)

        client.sql(create_table)

    def test_copy_writer_text_format(self, client):
        """Test COPY writer with text format."""
        table_name = "test_copy_text"
        self._create_test_table(client, table_name)

        schema = client.get_table_schema(table_name)

        with client.copy_writer(
            table_name, mode=CopyMode.STREAM, fmt=CopyFormat.TEXT
        ) as writer:
            for i in range(10):
                put = Put(writer.schema)
                put.set_object("id", i)
                put.set_object("name", f"name_{i}")
                writer.write(put)

        assert writer.count == 10

        # Verify data
        get = Get.builder(schema).set_primary_key("id", 5).build()
        record = client.get(get)
        assert record is not None
        assert record.get_object("name") == "name_5"

    def test_copy_writer_binary_format(self, client):
        """Test COPY writer with binary format."""
        table_name = "test_copy_binary"
        self._create_test_table(client, table_name)

        schema = client.get_table_schema(table_name)

        with client.copy_writer(
            table_name, mode=CopyMode.STREAM, fmt=CopyFormat.BINARY
        ) as writer:
            for i in range(5):
                put = Put(writer.schema)
                put.set_object("id", i)
                put.set_object("name", f"name_{i}")
                writer.write(put)

        assert writer.count == 5

    def test_copy_text_all_types(self, client):
        """Test COPY writer (TEXT format) with all data types (aligned with Java testCopy001)."""
        table_name = "test_copy_text_all_types"
        _create_all_type_table(client, table_name)
        schema = client.get_table_schema(table_name)

        with client.copy_writer(
            table_name, mode=CopyMode.STREAM, fmt=CopyFormat.TEXT
        ) as writer:
            for i in range(10):
                put = Put(writer.schema)
                put.set_object("id", i)
                for col_name, val in _make_all_type_values(i).items():
                    put.set_object(col_name, val)
                writer.write(put)
            # null row
            put = Put(writer.schema)
            put.set_object("id", 10)
            writer.write(put)

        assert writer.count == 11

        # Verify via get
        for i in range(10):
            get = Get.builder(schema).set_primary_key("id", i).build()
            record = client.get(get)
            _verify_all_type_record(record, i)

        get = Get.builder(schema).set_primary_key("id", 10).build()
        record = client.get(get)
        _verify_all_type_null_record(record)

    def test_copy_bulk_load_on_conflict_all_types(self, client):
        """Test COPY writer (BULK_LOAD_ON_CONFLICT, TEXT) with all data types."""
        table_name = "test_copy_blk_oc_all_types"
        _create_all_type_table(client, table_name)
        schema = client.get_table_schema(table_name)

        with client.copy_writer(
            table_name, mode=CopyMode.BULK_LOAD_ON_CONFLICT, fmt=CopyFormat.TEXT
        ) as writer:
            for i in range(10):
                put = Put(writer.schema)
                put.set_object("id", i)
                for col_name, val in _make_all_type_values(i).items():
                    put.set_object(col_name, val)
                writer.write(put)
            # null row
            put = Put(writer.schema)
            put.set_object("id", 10)
            writer.write(put)

        assert writer.count == 11

        # Verify via get
        for i in range(10):
            get = Get.builder(schema).set_primary_key("id", i).build()
            record = client.get(get)
            _verify_all_type_record(record, i)

        get = Get.builder(schema).set_primary_key("id", 10).build()
        record = client.get(get)
        _verify_all_type_null_record(record)

    def test_copy_binary_all_types(self, client):
        """Test COPY writer (BINARY format) with all data types (aligned with Java testCopy001)."""
        table_name = "test_copy_binary_all_types"
        _create_all_type_table(client, table_name)
        schema = client.get_table_schema(table_name)

        with client.copy_writer(
            table_name, mode=CopyMode.STREAM, fmt=CopyFormat.BINARY
        ) as writer:
            for i in range(10):
                put = Put(writer.schema)
                put.set_object("id", i)
                for col_name, val in _make_all_type_values(i).items():
                    put.set_object(col_name, val)
                writer.write(put)
            # null row
            put = Put(writer.schema)
            put.set_object("id", 10)
            writer.write(put)

        assert writer.count == 11

        # Verify via get
        for i in range(10):
            get = Get.builder(schema).set_primary_key("id", i).build()
            record = client.get(get)
            _verify_all_type_record(record, i)

        get = Get.builder(schema).set_primary_key("id", 10).build()
        record = client.get(get)
        _verify_all_type_null_record(record)

    @pytest.mark.parametrize("fmt", [CopyFormat.TEXT, CopyFormat.BINARY])
    def test_copy_fixed_fe(self, client, test_config, fmt):
        """Test COPY writer with FixedFE (STREAM mode only)."""
        table_name = f"test_copy_fixed_fe_{fmt.value}"
        _create_all_type_table(client, table_name, include_timestamptz=False)

        test_config.use_fixed_fe = True
        with HoloClient(test_config) as c:
            with c.copy_writer(table_name, mode=CopyMode.STREAM, fmt=fmt) as writer:
                for i in range(10):
                    put = Put(writer.schema)
                    put.set_object("id", i)
                    for col_name, val in _make_all_type_values(i).items():
                        if col_name != "col_timestamptz":
                            put.set_object(col_name, val)
                    writer.write(put)

            assert writer.count == 10

        # Verify via regular FE get
        schema = client.get_table_schema(table_name)
        for i in range(10):
            get = Get.builder(schema).set_primary_key("id", i).build()
            record = client.get(get)
            _verify_all_type_record(record, i, skip_cols={"col_timestamptz"})

    def test_copy_fixed_fe_rejects_bulk_load(self, client, test_config):
        """Test that FixedFE rejects non-STREAM copy modes."""
        table_name = "test_copy_fixed_fe_reject"
        self._create_test_table(client, table_name)

        test_config.use_fixed_fe = True
        with HoloClient(test_config) as c:
            with pytest.raises(HoloClientException) as exc_info:
                c.copy_writer(table_name, mode=CopyMode.BULK_LOAD)
            assert exc_info.value.code == ExceptionCode.INVALID_REQUEST
            assert "STREAM" in str(exc_info.value)


class TestHoloClientCopyStage:
    """Tests for stage-based COPY write operations (Hologres >= 4.1.0).

    These tests require pyarrow and a Hologres instance that supports
    internal stages (hg_create_internal_stage).
    """

    def _create_test_table(self, client, table_name: str, extra_cols: str = ""):
        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        name TEXT
                        {extra_cols},
                        PRIMARY KEY (id)
                    )
                """)

        client.sql(create_table)

    def _create_stage(self, client, stage_name: str, ttl: int = 7200):
        client.create_stage(stage_name, ttl_seconds=ttl)

    def _drop_stage(self, client, stage_name: str):
        client.drop_stage(stage_name)

    def _count_rows(self, client, table_name: str) -> int:
        def do_count(conn):
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {table_name}")
                return cur.fetchone()[0]

        return client.sql(do_count)

    def test_copy_stage_basic(self, client):
        """Test basic stage copy: write records, verify they land in the table."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_basic"
        stage_name = "test_stage_basic"
        self._create_test_table(client, table_name)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            with client.copy_stage_writer(table_name, stage_name) as writer:
                for i in range(10):
                    put = Put(writer.schema)
                    put.set_object("id", i)
                    put.set_object("name", f"name_{i}")
                    writer.write(put)

            assert writer.count == 10

            # Verify data in table
            schema = client.get_table_schema(table_name)
            get = Get.builder(schema).set_primary_key("id", 5).build()
            record = client.get(get)
            assert record is not None
            assert record.get_object("name") == "name_5"

            assert self._count_rows(client, table_name) == 10
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_large_batch(self, client):
        """Test stage copy with enough data to trigger multiple Arrow batches."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_batch"
        stage_name = "test_stage_batch"
        self._create_test_table(client, table_name)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        num_records = 10000
        try:
            with client.copy_stage_writer(
                table_name, stage_name, max_batch_size=512
            ) as writer:
                for i in range(num_records):
                    put = Put(writer.schema)
                    put.set_object("id", i)
                    put.set_object("name", f"name_{i}")
                    writer.write(put)

            assert writer.count == num_records
            assert self._count_rows(client, table_name) == num_records
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_file_splitting(self, client):
        """Test that large data is split across multiple stage files."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_split"
        stage_name = "test_stage_split"
        self._create_test_table(client, table_name)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            # Use a small file size limit to force multiple files
            with client.copy_stage_writer(
                table_name,
                stage_name,
                file_size_limit=1024,  # 1KB limit to force splits
                max_batch_size=10,
            ) as writer:
                for i in range(200):
                    put = Put(writer.schema)
                    put.set_object("id", i)
                    put.set_object("name", f"name_with_some_padding_{i:04d}")
                    writer.write(put)

            assert writer.count == 200
            assert writer._file_index > 1  # Should have split into multiple files
            assert self._count_rows(client, table_name) == 200
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_on_conflict_ignore(self, client, test_config):
        """Test stage copy with INSERT_OR_IGNORE conflict resolution."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_ignore"
        stage_name = "test_stage_ignore"
        self._create_test_table(client, table_name)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            # Insert initial data
            schema = client.get_table_schema(table_name)
            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("name", "original")
            client.put(put)
            client.flush()

            # Stage copy with conflicting id=1 using IGNORE
            test_config.on_conflict_action = OnConflictAction.INSERT_OR_IGNORE
            with HoloClient(test_config) as c2:
                with c2.copy_stage_writer(table_name, stage_name) as writer:
                    put = Put(writer.schema)
                    put.set_object("id", 1)
                    put.set_object("name", "should_be_ignored")
                    writer.write(put)

                    put = Put(writer.schema)
                    put.set_object("id", 2)
                    put.set_object("name", "new_record")
                    writer.write(put)

            # id=1 should keep original value
            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = client.get(get)
            assert record.get_object("name") == "original"

            # id=2 should be inserted
            get = Get.builder(schema).set_primary_key("id", 2).build()
            record = client.get(get)
            assert record is not None
            assert record.get_object("name") == "new_record"
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_on_conflict_update(self, client, test_config):
        """Test stage copy with INSERT_OR_REPLACE conflict resolution."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_update"
        stage_name = "test_stage_update"
        self._create_test_table(client, table_name)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            # Insert initial data
            schema = client.get_table_schema(table_name)
            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("name", "original")
            client.put(put)
            client.flush()

            # Stage copy with conflicting id=1 using REPLACE
            test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
            with HoloClient(test_config) as c2:
                with c2.copy_stage_writer(table_name, stage_name) as writer:
                    put = Put(writer.schema)
                    put.set_object("id", 1)
                    put.set_object("name", "replaced")
                    writer.write(put)

            # id=1 should be updated
            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = client.get(get)
            assert record.get_object("name") == "replaced"
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_specific_columns(self, client):
        """Test stage copy with a subset of columns."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_cols"
        stage_name = "test_stage_cols"
        self._create_test_table(client, table_name, ", age INT")

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            # Only write id and name, skip age
            with client.copy_stage_writer(
                table_name, stage_name, columns=["id", "name"]
            ) as writer:
                put = Put(writer.schema)
                put.set_object("id", 1)
                put.set_object("name", "Alice")
                writer.write(put)

            schema = client.get_table_schema(table_name)
            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = client.get(get)
            assert record is not None
            assert record.get_object("name") == "Alice"
            assert record.get_object("age") is None
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_no_pk_table(self, client):
        """Test stage copy into a table without primary key."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_nopk"
        stage_name = "test_stage_nopk"

        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT,
                        name TEXT
                    )
                """)

        client.sql(create_table)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            with client.copy_stage_writer(table_name, stage_name) as writer:
                for i in range(5):
                    put = Put(writer.schema)
                    put.set_object("id", i)
                    put.set_object("name", f"name_{i}")
                    writer.write(put)

            assert self._count_rows(client, table_name) == 5
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_multiple_types(self, client):
        """Test stage copy with various data types."""
        pa = pytest.importorskip("pyarrow")
        import datetime
        from decimal import Decimal

        table_name = "test_copy_stage_types"
        stage_name = "test_stage_types"

        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        col_int INT,
                        col_bigint BIGINT,
                        col_float FLOAT4,
                        col_double FLOAT8,
                        col_bool BOOL,
                        col_text TEXT,
                        col_date DATE,
                        col_timestamp TIMESTAMP,
                        PRIMARY KEY (id)
                    )
                """)

        client.sql(create_table)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            schema = client.get_table_schema(table_name)
            with client.copy_stage_writer(table_name, stage_name) as writer:
                put = Put(writer.schema)
                put.set_object("id", 1)
                put.set_object("col_int", 42)
                put.set_object("col_bigint", 9876543210)
                put.set_object("col_float", 3.14)
                put.set_object("col_double", 2.718281828)
                put.set_object("col_bool", True)
                put.set_object("col_text", "hello world")
                put.set_object("col_date", datetime.date(2024, 6, 15))
                put.set_object(
                    "col_timestamp", datetime.datetime(2024, 6, 15, 10, 30, 0)
                )
                writer.write(put)

                # Write a record with nulls
                put2 = Put(writer.schema)
                put2.set_object("id", 2)
                writer.write(put2)

            assert self._count_rows(client, table_name) == 2

            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = client.get(get)
            assert record is not None
            assert record.get_object("col_int") == 42
            assert record.get_object("col_bigint") == 9876543210
            assert record.get_object("col_bool") is True
            assert record.get_object("col_text") == "hello world"
            assert record.get_object("col_date") == datetime.date(2024, 6, 15)

            # Verify null record
            get2 = Get.builder(schema).set_primary_key("id", 2).build()
            record2 = client.get(get2)
            assert record2 is not None
            assert record2.get_object("col_text") is None
            assert record2.get_object("col_bool") is None
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_all_types(self, client):
        """Test stage copy with all supported data types including json, decimal, arrays, bytea."""
        pa = pytest.importorskip("pyarrow")
        import datetime
        import json
        from decimal import Decimal

        table_name = "test_copy_stage_all_types"
        stage_name = "test_stage_all_types"

        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        col_smallint SMALLINT,
                        col_int INT,
                        col_bigint BIGINT,
                        col_float4 FLOAT4,
                        col_float8 FLOAT8,
                        col_bool BOOL,
                        col_text TEXT,
                        col_varchar VARCHAR(100),
                        col_bytea BYTEA,
                        col_date DATE,
                        col_timestamp TIMESTAMP,
                        col_timestamptz TIMESTAMPTZ,
                        col_decimal DECIMAL(18, 6),
                        col_json JSON,
                        col_jsonb JSONB,
                        col_int_array int4[],
                        col_bigint_array int8[],
                        col_float_array float4[],
                        col_text_array text[],
                        PRIMARY KEY (id)
                    )
                """)

        client.sql(create_table)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            schema = client.get_table_schema(table_name)

            with client.copy_stage_writer(table_name, stage_name) as writer:
                # Row 1: all columns set
                put = Put(writer.schema)
                put.set_object("id", 1)
                put.set_object("col_smallint", 123)
                put.set_object("col_int", 456789)
                put.set_object("col_bigint", 9876543210)
                put.set_object("col_float4", 3.14)
                put.set_object("col_float8", 2.718281828459045)
                put.set_object("col_bool", True)
                put.set_object("col_text", "hello world")
                put.set_object("col_varchar", "varchar value")
                put.set_object("col_bytea", b"\x00\x01\x02\xff")
                put.set_object("col_date", datetime.date(2024, 6, 15))
                put.set_object(
                    "col_timestamp", datetime.datetime(2024, 6, 15, 10, 30, 0)
                )
                put.set_object(
                    "col_timestamptz", datetime.datetime(2024, 6, 15, 10, 30, 0)
                )
                put.set_object("col_decimal", Decimal("12345.678901"))
                put.set_object("col_json", json.dumps({"key": "value", "num": 42}))
                put.set_object("col_jsonb", json.dumps({"nested": {"a": [1, 2, 3]}}))
                put.set_object("col_int_array", [1, 2, 3, 4, 5])
                put.set_object("col_bigint_array", [100, 200, 300])
                put.set_object("col_float_array", [1.1, 2.2, 3.3])
                put.set_object("col_text_array", ["hello", "world"])
                writer.write(put)

                # Row 2: all nulls except PK
                put2 = Put(writer.schema)
                put2.set_object("id", 2)
                writer.write(put2)

                # Row 3: edge cases
                put3 = Put(writer.schema)
                put3.set_object("id", 3)
                put3.set_object("col_smallint", -32768)
                put3.set_object("col_int", -2147483648)
                put3.set_object("col_bigint", -9223372036854775808)
                put3.set_object("col_float4", 0.0)
                put3.set_object("col_float8", 0.0)
                put3.set_object("col_bool", False)
                put3.set_object("col_text", "")
                put3.set_object("col_decimal", Decimal("0.000000"))
                put3.set_object("col_json", json.dumps(None))
                put3.set_object("col_jsonb", json.dumps([]))
                put3.set_object("col_int_array", [])
                put3.set_object("col_text_array", [])
                writer.write(put3)

            assert writer.count == 3
            assert self._count_rows(client, table_name) == 3

            # Verify row 1 - all types set
            get1 = Get.builder(schema).set_primary_key("id", 1).build()
            r1 = client.get(get1)
            assert r1 is not None
            assert r1.get_object("col_smallint") == 123
            assert r1.get_object("col_int") == 456789
            assert r1.get_object("col_bigint") == 9876543210
            assert r1.get_object("col_bool") is True
            assert r1.get_object("col_text") == "hello world"
            assert r1.get_object("col_varchar") == "varchar value"
            assert r1.get_object("col_date") == datetime.date(2024, 6, 15)
            assert r1.get_object("col_decimal") == Decimal("12345.678901")
            # json/jsonb come back as parsed dicts
            json_val = r1.get_object("col_json")
            if isinstance(json_val, str):
                json_val = json.loads(json_val)
            assert json_val["key"] == "value"
            assert json_val["num"] == 42
            jsonb_val = r1.get_object("col_jsonb")
            if isinstance(jsonb_val, str):
                jsonb_val = json.loads(jsonb_val)
            assert jsonb_val["nested"]["a"] == [1, 2, 3]
            # bytea
            bytea_val = r1.get_object("col_bytea")
            assert bytes(bytea_val) == b"\x00\x01\x02\xff"
            # arrays
            assert list(r1.get_object("col_int_array")) == [1, 2, 3, 4, 5]
            assert list(r1.get_object("col_bigint_array")) == [100, 200, 300]
            assert list(r1.get_object("col_text_array")) == ["hello", "world"]

            # Verify row 2 - all nulls
            get2 = Get.builder(schema).set_primary_key("id", 2).build()
            r2 = client.get(get2)
            assert r2 is not None
            assert r2.get_object("col_smallint") is None
            assert r2.get_object("col_int") is None
            assert r2.get_object("col_text") is None
            assert r2.get_object("col_json") is None
            assert r2.get_object("col_jsonb") is None
            assert r2.get_object("col_int_array") is None

            # Verify row 3 - edge cases
            get3 = Get.builder(schema).set_primary_key("id", 3).build()
            r3 = client.get(get3)
            assert r3 is not None
            assert r3.get_object("col_smallint") == -32768
            assert r3.get_object("col_int") == -2147483648
            assert r3.get_object("col_bigint") == -9223372036854775808
            assert r3.get_object("col_bool") is False
            assert r3.get_object("col_text") == ""
            assert r3.get_object("col_decimal") == Decimal("0.000000")
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_exception_no_commit(self, client):
        """Test that data is NOT committed if an exception occurs during write."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_exc"
        stage_name = "test_stage_exc"
        self._create_test_table(client, table_name)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            with pytest.raises(ValueError, match="test error"):
                with client.copy_stage_writer(table_name, stage_name) as writer:
                    put = Put(writer.schema)
                    put.set_object("id", 1)
                    put.set_object("name", "should_not_commit")
                    writer.write(put)
                    raise ValueError("test error")

            # Table should be empty since commit was skipped
            assert self._count_rows(client, table_name) == 0
        finally:
            self._drop_stage(client, stage_name)

    def test_copy_stage_manual_flush_and_commit(self, client, test_config):
        """Test manual flush() and commit() without context manager auto-commit."""
        pa = pytest.importorskip("pyarrow")

        table_name = "test_copy_stage_manual"
        stage_name = "test_stage_manual"
        self._create_test_table(client, table_name)

        try:
            self._create_stage(client, stage_name)
        except Exception as e:
            if "hg_create_internal_stage" in str(e) or "does not exist" in str(e):
                pytest.skip("Internal stage not supported on this Hologres version")
            raise

        try:
            from hologres.copy_stage import CopyStageWriter

            schema = client.get_table_schema(table_name)
            writer = CopyStageWriter(
                config=test_config,
                schema=schema,
                stage_name=stage_name,
            )

            for i in range(5):
                put = Put(schema)
                put.set_object("id", i)
                put.set_object("name", f"manual_{i}")
                writer.write(put)

            # Manually flush and commit
            writer.flush()
            assert writer._file_index >= 1  # At least one file written

            writer.commit()
            assert self._count_rows(client, table_name) == 5

            writer._arrow_writer.close()
            if writer._conn is not None:
                writer._conn.close()
        finally:
            self._drop_stage(client, stage_name)


class TestHoloClientScan:
    """Tests for HoloClient.scan() API."""

    def _create_scan_table(self, client, table_name: str):
        """Create a table with composite PK (id, id1) for prefix scan tests."""

        def setup(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        id1 INT NOT NULL,
                        name TEXT,
                        age INT,
                        PRIMARY KEY (id, id1)
                    ) WITH (distribution_key = 'id')
                """)

        client.sql(setup)

    def _insert_data(self, client, table_name: str):
        """Insert test data: 3 prefix groups, each with 5 rows."""
        schema = client.get_table_schema(table_name)
        for prefix in range(3):
            for i in range(5):
                put = Put(schema)
                put.set_object("id", prefix)
                put.set_object("id1", i)
                put.set_object("name", f"name_{prefix}_{i}")
                put.set_object("age", 20 + i)
                client.put(put)
        client.flush()

    def test_scan_equal_filter(self, client):
        """Test scan with equal filter on prefix key."""
        table_name = "test_scan_equal"
        self._create_scan_table(client, table_name)
        self._insert_data(client, table_name)

        schema = client.get_table_schema(table_name)
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 1)
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        records = client.scan(scan)

        assert len(records) == 5
        for r in records:
            assert r.get_object("id") == 1

    def test_scan_with_sort_keys(self, client):
        """Test scan with PRIMARY_KEY sort order."""
        table_name = "test_scan_sort"
        self._create_scan_table(client, table_name)
        self._insert_data(client, table_name)

        schema = client.get_table_schema(table_name)
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 2)
            .set_sort_keys(SortKeys.PRIMARY_KEY)
            .build()
        )
        records = client.scan(scan)

        assert len(records) == 5
        id1_values = [r.get_object("id1") for r in records]
        assert id1_values == [0, 1, 2, 3, 4]

    def test_scan_selected_columns(self, client):
        """Test scan with specific columns selected."""
        table_name = "test_scan_cols"
        self._create_scan_table(client, table_name)
        self._insert_data(client, table_name)

        schema = client.get_table_schema(table_name)
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 0)
            .with_selected_columns(["id", "name"])
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        records = client.scan(scan)

        assert len(records) == 5
        for r in records:
            assert r.get_object("id") == 0
            assert r.get_object("name") is not None
            # Non-selected columns should be None
            assert r.get_object("age") is None

    def test_scan_range_filter(self, client):
        """Test scan with range filter."""
        table_name = "test_scan_range"
        self._create_scan_table(client, table_name)
        self._insert_data(client, table_name)

        schema = client.get_table_schema(table_name)
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 1)
            .add_range_filter(
                "age", start=22, end=24, start_inclusive=True, end_inclusive=False
            )
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        records = client.scan(scan)

        # age 22, 23 (id=1, id1=2,3)
        assert len(records) == 2
        ages = sorted(r.get_object("age") for r in records)
        assert ages == [22, 23]

    def test_scan_no_results(self, client):
        """Test scan that matches no rows."""
        table_name = "test_scan_empty"
        self._create_scan_table(client, table_name)
        self._insert_data(client, table_name)

        schema = client.get_table_schema(table_name)
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 999)
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        records = client.scan(scan)

        assert len(records) == 0

    def test_scan_no_results(self, client):
        """Test scan that matches no rows (was async_scan)."""
        table_name = "test_scan_no_results"
        self._create_scan_table(client, table_name)
        self._insert_data(client, table_name)

        schema = client.get_table_schema(table_name)
        scan = (
            Scan.builder(schema)
            .add_equal_filter("id", 999)
            .set_sort_keys(SortKeys.NONE)
            .build()
        )
        records = client.scan(scan)

        assert len(records) == 0


class TestShardUtil:
    """Integration tests for shard hash correctness.

    Mirrors Java's ShardUtilTest: insert records with typed distribution keys,
    query hg_shard_id from the database, and verify compute_shard matches.
    """

    def _get_shard_count(self, client, schema):
        """Get shard count for a table, matching Java's getShardCount()."""
        sn = schema.schema_name
        tn = schema.table_name

        def query(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT g.property_value "
                    "FROM hologres.hg_table_properties t, hologres.hg_table_group_properties g "
                    "WHERE t.property_key='table_group' AND g.property_key='shard_count' "
                    "AND table_namespace=%s AND table_name=%s "
                    "AND t.property_value = g.tablegroup_name",
                    (sn, tn),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"Table {sn}.{tn} not found in hg_table_properties"
                    )
                return int(row[0])

        return client.sql(query)

    def _run_shard_test(self, client, table_name, col_type, values, dk_cols="id,col1"):
        """Create table with distribution key, insert values, verify hg_shard_id matches."""
        from hologres._shard import compute_shard

        def setup(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(
                    f"CREATE TABLE {table_name} (id text, col1 {col_type}) "
                    f"WITH (distribution_key='{dk_cols}')"
                )

        client.sql(setup)

        schema = client.get_table_schema(table_name, no_cache=True)
        shard_count = self._get_shard_count(client, schema)

        # Insert records and compute expected shards
        expected = {}
        for i, val in enumerate(values):
            record_id = f"row_{i}"
            put = Put(schema)
            put.set_object("id", record_id)
            put.set_object("col1", val)
            client.put(put)
            # Compute expected shard using our Python implementation
            expected[record_id] = compute_shard(put.record, shard_count)
        client.flush()

        # Query actual hg_shard_id from database
        actual = {}

        def read(conn):
            with conn.cursor() as cur:
                cur.execute(f"SELECT hg_shard_id, id FROM {table_name}")
                for row in cur.fetchall():
                    actual[row[1]] = row[0]

        client.sql(read)

        assert len(actual) == len(values), (
            f"Expected {len(values)} rows, got {len(actual)}"
        )
        for record_id, expected_shard in expected.items():
            assert actual[record_id] == expected_shard, (
                f"{record_id}: expected shard {expected_shard}, got {actual[record_id]}"
            )

    def test_shard_int_dk(self, client):
        """Test shard routing with integer distribution key."""
        values = list(range(10))
        self._run_shard_test(client, "test_shard_int", "int", values)

    def test_shard_bigint_dk(self, client):
        """Test shard routing with bigint distribution key."""
        values = [i * 1000000000 for i in range(10)]
        self._run_shard_test(client, "test_shard_bigint", "bigint", values)

    def test_shard_text_dk(self, client):
        """Test shard routing with text distribution key."""
        values = [f"text_value_{i}" for i in range(10)]
        self._run_shard_test(client, "test_shard_text", "text", values)

    def test_shard_timestamp_dk(self, client):
        """Test shard routing with timestamp distribution key (supported since 3.0)."""
        import datetime

        base = datetime.datetime(2024, 1, 1, 0, 0, 0)
        values = [
            base + datetime.timedelta(hours=i, microseconds=i * 123) for i in range(10)
        ]
        self._run_shard_test(client, "test_shard_ts", "timestamp", values)

    def test_shard_date_dk(self, client):
        """Test shard routing with date distribution key."""
        import datetime

        values = [
            datetime.date(2024, 1, 1) + datetime.timedelta(days=i) for i in range(10)
        ]
        self._run_shard_test(client, "test_shard_date", "date", values)

    def test_shard_decimal_dk(self, client):
        """Test shard routing with decimal distribution key (supported since 3.1).

        Mirrors Java's testDecimalDistributionKey with varying precision/scale.
        """
        from decimal import Decimal

        def setup(conn):
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_shard_decimal")
                cur.execute(
                    "CREATE TABLE test_shard_decimal ("
                    "  id text,"
                    "  col1 decimal(38,18),"
                    "  col2 decimal(38,0),"
                    "  col3 decimal(6,2),"
                    "  col4 decimal(6,6)"
                    ") WITH (distribution_key='id,col1,col2,col3,col4')"
                )

        client.sql(setup)

        from hologres._shard import compute_shard

        schema = client.get_table_schema("test_shard_decimal", no_cache=True)
        shard_count = self._get_shard_count(client, schema)

        expected = {}
        total = 10
        for i in range(total):
            record_id = f"dec_{i}"
            put = Put(schema)
            put.set_object("id", record_id)
            if i == 0:
                put.set_object("col1", Decimal("0"))
                put.set_object("col2", Decimal("0"))
                put.set_object("col3", Decimal("0"))
                put.set_object("col4", Decimal("0"))
            elif i == 1:
                put.set_object(
                    "col1", Decimal("12345678901234567890.123456789012345678")
                )
                put.set_object(
                    "col2", Decimal("12345678901234567890123456789012345678")
                )
                put.set_object("col3", Decimal("1234.56"))
                put.set_object("col4", Decimal("0.123456"))
            else:
                # Values with excess precision — tests rounding
                put.set_object(
                    "col1", Decimal("12345678901234567890.12345678901234567899999")
                )
                put.set_object(
                    "col2", Decimal("12345678901234567890123456789012345678.0123456")
                )
                put.set_object("col3", Decimal("1234.567"))
                put.set_object("col4", Decimal("0.123456789"))
            client.put(put)
            expected[record_id] = compute_shard(put.record, shard_count)
        client.flush()

        actual = {}

        def read(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT hg_shard_id, id FROM test_shard_decimal")
                for row in cur.fetchall():
                    actual[row[1]] = row[0]

        client.sql(read)

        assert len(actual) == total
        for record_id, expected_shard in expected.items():
            assert actual[record_id] == expected_shard, (
                f"{record_id}: expected shard {expected_shard}, got {actual[record_id]}"
            )

    def test_shard_uuid_dk(self, client):
        """Test shard routing with UUID distribution key (supported since 3.2)."""
        import uuid

        values = [str(uuid.uuid4()) for _ in range(10)]
        self._run_shard_test(client, "test_shard_uuid", "uuid", values)


class TestRemoveU0000:
    """Tests for remove_u0000_in_text config param."""

    def _create_test_table(self, client, table_name: str):
        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL,
                        col_text TEXT,
                        col_varchar VARCHAR(100),
                        col_char CHAR(10),
                        col_int INT,
                        PRIMARY KEY (id)
                    )
                """)

        client.sql(create_table)

    def test_remove_u0000_enabled(self, client, test_config):
        """Test that \\u0000 is stripped from text columns when enabled (default)."""
        table_name = "test_u0000_enabled"
        self._create_test_table(client, table_name)

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
        test_config.remove_u0000_in_text = True
        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("col_text", "hello\x00world")
            put.set_object("col_varchar", "foo\x00bar")
            put.set_object("col_char", "a\x00b")
            put.set_object("col_int", 42)
            c.put(put)
            c.flush()

            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = c.get(get)
            assert record is not None
            assert record.get_object("col_text") == "helloworld"
            assert record.get_object("col_varchar") == "foobar"
            # char(10) is right-padded
            assert record.get_object("col_char").strip() == "ab"
            assert record.get_object("col_int") == 42

    def test_remove_u0000_multiple_occurrences(self, client, test_config):
        """Test that multiple \\u0000 are all stripped."""
        table_name = "test_u0000_multi"
        self._create_test_table(client, table_name)

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
        test_config.remove_u0000_in_text = True
        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("col_text", "\x00start\x00middle\x00end\x00")
            c.put(put)
            c.flush()

            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = c.get(get)
            assert record is not None
            assert record.get_object("col_text") == "startmiddleend"

    def test_remove_u0000_disabled(self, client, test_config):
        """Test that \\u0000 is NOT stripped when remove_u0000_in_text=False.

        Hologres rejects \\u0000 in text, so we verify it raises an error.
        """
        table_name = "test_u0000_disabled"
        self._create_test_table(client, table_name)

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
        test_config.remove_u0000_in_text = False
        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("col_text", "hello\x00world")
            c.put(put)

            # Hologres does not support \u0000 in text, so flush should fail
            with pytest.raises(Exception):
                c.flush()

    def test_remove_u0000_no_null_chars(self, client, test_config):
        """Test that strings without \\u0000 are unchanged."""
        table_name = "test_u0000_clean"
        self._create_test_table(client, table_name)

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
        test_config.remove_u0000_in_text = True
        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("col_text", "normal text")
            put.set_object("col_varchar", "normal varchar")
            c.put(put)
            c.flush()

            get = Get.builder(schema).set_primary_key("id", 1).build()
            record = c.get(get)
            assert record is not None
            assert record.get_object("col_text") == "normal text"
            assert record.get_object("col_varchar") == "normal varchar"


class TestHoloClientSchemaCache:
    """Tests for schema caching."""

    def test_schema_cache(self, client):
        """Test that schema is cached."""
        table_name = "test_cache_table"

        def create_table(conn):
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id INT NOT NULL PRIMARY KEY
                    )
                """)

        client.sql(create_table)

        schema1 = client.get_table_schema(table_name)
        schema2 = client.get_table_schema(table_name)

        # Should return the same object from cache
        assert schema1 is schema2

        # Force refresh
        schema3 = client.get_table_schema(table_name, no_cache=True)
        # Different object but equal in value
        assert schema1 is not schema3
        assert schema1.table_id == schema3.table_id


class TestHoloClientErrors:
    """Tests for error handling."""

    def test_get_nonexistent_table(self, client):
        """Test that getting schema for non-existent table raises error."""
        with pytest.raises(HoloClientException) as exc_info:
            client.get_table_schema("nonexistent_table_xyz")

        assert exc_info.value.code == ExceptionCode.TABLE_NOT_FOUND

    def test_put_to_closed_client(self, test_config):
        """Test that put to closed client raises error."""
        client = HoloClient(test_config)
        client.close()

        with pytest.raises(HoloClientException) as exc_info:
            schema = TableSchema(TableName.valueOf("test"), [Column("id", "int4", 4)])
            put = Put(schema)
            client.put(put)

        assert exc_info.value.code == ExceptionCode.ALREADY_CLOSE


class TestHoloClientRoaringBitmap:
    """Tests for roaringbitmap extension type (aligned with Java HoloClientTypesTest)."""

    @pytest.fixture(autouse=True)
    def setup_extension(self, client):
        """Ensure roaringbitmap extension is available."""
        client.sql(
            lambda conn: conn.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap")
        )

    def test_roaringbitmap_put_get(self, client, test_config):
        """Test put/get with roaringbitmap column.

        FixedFE does not support roaringbitmap for DML (Java also skips this).
        COPY tests cover FixedFE since COPY always uses regular FE.
        """
        table_name = "test_rb_put_get_fe"
        client.sql(
            lambda conn: conn.execute(f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                rb_col roaringbitmap
            )
        """)
        )

        # Get a known roaringbitmap binary value via SQL (cast to bytea for bytes)
        rb_bytes = client.sql(
            lambda conn: conn.execute(
                "SELECT rb_build(ARRAY[1,2,3,100,200])::bytea"
            ).fetchone()[0]
        )

        test_config.on_conflict_action = OnConflictAction.INSERT_OR_REPLACE
        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            # Put with roaringbitmap bytes
            put = Put(schema)
            put.set_object("id", 1)
            put.set_object("rb_col", rb_bytes)
            c.put(put)
            c.flush()

            # Get back
            get = Get(schema)
            get.set_primary_key("id", 1)
            record = c.get(get)
            assert record is not None

            # Verify via SQL rb_cardinality
            card = c.sql(
                lambda conn: conn.execute(
                    f"SELECT rb_cardinality(rb_col) FROM {table_name} WHERE id = 1"
                ).fetchone()[0]
            )
            assert card == 5

    def test_roaringbitmap_fixed_fe_blocked(self, client, test_config):
        """Test that FixedFE rejects tables with roaringbitmap columns."""
        table_name = "test_rb_fixed_fe_blocked"
        client.sql(
            lambda conn: conn.execute(f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                rb_col roaringbitmap
            )
        """)
        )

        test_config.use_fixed_fe = True
        with HoloClient(test_config) as c:
            with pytest.raises(HoloClientException) as exc_info:
                c.get_table_schema(table_name)
            assert exc_info.value.code == ExceptionCode.INVALID_REQUEST
            assert "roaringbitmap" in str(exc_info.value)

    def test_roaringbitmap_copy_text(self, client, test_config):
        """Test text COPY with roaringbitmap column."""
        table_name = f"test_rb_copy_text"
        client.sql(
            lambda conn: conn.execute(f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                rb_col roaringbitmap
            )
        """)
        )

        # Get roaringbitmap binary value (cast to bytea for bytes)
        rb_bytes = client.sql(
            lambda conn: conn.execute(
                "SELECT rb_build(ARRAY[10,20,30])::bytea"
            ).fetchone()[0]
        )

        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            with c.copy_writer(
                table_name, mode=CopyMode.STREAM, fmt=CopyFormat.TEXT
            ) as writer:
                put = Put(schema)
                put.set_object("id", 1)
                put.set_object("rb_col", rb_bytes)
                writer.write(put)

            # Verify
            card = c.sql(
                lambda conn: conn.execute(
                    f"SELECT rb_cardinality(rb_col) FROM {table_name} WHERE id = 1"
                ).fetchone()[0]
            )
            assert card == 3

    def test_roaringbitmap_copy_binary(self, client, test_config):
        """Test binary COPY with roaringbitmap column (uses bytea passthrough dumper)."""
        table_name = f"test_rb_copy_bin"
        client.sql(
            lambda conn: conn.execute(f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY,
                rb_col roaringbitmap
            )
        """)
        )

        # Get roaringbitmap binary value (cast to bytea for bytes)
        rb_bytes = client.sql(
            lambda conn: conn.execute(
                "SELECT rb_build(ARRAY[5,10,15,20,25])::bytea"
            ).fetchone()[0]
        )

        with HoloClient(test_config) as c:
            schema = c.get_table_schema(table_name)

            with c.copy_writer(
                table_name, mode=CopyMode.STREAM, fmt=CopyFormat.BINARY
            ) as writer:
                put = Put(schema)
                put.set_object("id", 1)
                put.set_object("rb_col", rb_bytes)
                writer.write(put)

            # Verify
            card = c.sql(
                lambda conn: conn.execute(
                    f"SELECT rb_cardinality(rb_col) FROM {table_name} WHERE id = 1"
                ).fetchone()[0]
            )
            assert card == 5
