"""Hologres Python Client.

A Python client for Alibaba Cloud Hologres, providing both synchronous
and asynchronous APIs for reading and writing data.

Basic usage (sync)::

    from hologres import HoloClient, HoloConfig, Put, Get

    config = HoloConfig(
        host="your-hologres-host",
        port=80,
        database="your_db",
        username="your_user",
        password="your_password",
    )
    with HoloClient(config) as client:
        schema = client.get_table_schema("my_table")

        # Write
        put = Put(schema)
        put.set_object("id", 1)
        put.set_object("name", "Alice")
        client.put(put)
        client.flush()

        # Read
        get = Get.builder(schema).set_primary_key("id", 1).build()
        record = client.get(get)
        if record:
            print(record.get_object("name"))

Basic usage (async)::

    from hologres import AsyncHoloClient, HoloConfig, Put

    config = HoloConfig(...)
    async with AsyncHoloClient(config) as client:
        schema = await client.get_table_schema("my_table")
        put = Put(schema)
        put.set_object("id", 1)
        await client.put(put)
        await client.flush()
"""

from .async_client import AsyncHoloClient
from .client import HoloClient
from .column import Column
from .config import HoloConfig
from .copy import AsyncCopyWriter, CopyFormat, CopyMode, CopyWriter
from .copy_stage import AsyncCopyStageWriter, CopyStageWriter
from .exceptions import HoloClientException, HoloClientWithDetailsException
from .get import Get, GetBuilder
from .put import Put
from .record import Record
from .scan import EqualsFilter, RangeFilter, Scan, ScanBuilder, SortKeys
from .table_name import TableName
from .table_schema import TableSchema
from .types import ExceptionCode, MutationType, OnConflictAction, WriteFailStrategy

__all__ = [
    "AsyncCopyStageWriter",
    "AsyncCopyWriter",
    "AsyncHoloClient",
    "Column",
    "CopyFormat",
    "CopyMode",
    "CopyStageWriter",
    "CopyWriter",
    "ExceptionCode",
    "Get",
    "GetBuilder",
    "HoloClient",
    "HoloClientException",
    "HoloClientWithDetailsException",
    "HoloConfig",
    "MutationType",
    "OnConflictAction",
    "Put",
    "RangeFilter",
    "Record",
    "Scan",
    "ScanBuilder",
    "SortKeys",
    "EqualsFilter",
    "TableName",
    "TableSchema",
    "WriteFailStrategy",
]

from ._version import __version__
