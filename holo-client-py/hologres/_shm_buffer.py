"""Shared memory ring buffer for zero-pickle IPC between parent and worker.

Single-producer single-consumer (SPSC) design: parent writes, worker reads.
Each worker gets its own buffer. Messages are variable-length and ordered.

Buffer layout:
  [write_pos: mp.Value('Q')] [read_pos: mp.Value('Q')]
  [SharedMemory: capacity bytes of ring buffer data]

Message format:
  [msg_len: 4B uint] [msg_type: 1B] [payload: variable]

Message types:
  0 = record data
  1 = flush sentinel (payload: 4B ack_id)
  2 = close sentinel (no payload)
"""

from __future__ import annotations

import multiprocessing
import pickle
import struct
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from multiprocessing.shared_memory import SharedMemory
from typing import Any, Dict, List, Optional, Tuple

from .record import Record
from .table_name import TableName
from .table_schema import TableSchema
from .types import MutationType

# Map MutationType to int for binary serialization
_MUTATION_TYPE_TO_INT = {MutationType.INSERT: 0, MutationType.DELETE: 1}
_INT_TO_MUTATION_TYPE = {0: MutationType.INSERT, 1: MutationType.DELETE}

# Message types
MSG_RECORD = 0
MSG_FLUSH = 1
MSG_CLOSE = 2
MSG_GET = 3

# Value type tags for serialization
TAG_NONE = 0
TAG_INT = 1
TAG_FLOAT = 2
TAG_STR = 3
TAG_BOOL = 4
TAG_BYTES = 5
TAG_DATETIME = 6
TAG_DATE = 7
TAG_DATETIME_NAIVE = 8
TAG_PICKLE = 255  # fallback

# Header overhead: msg_len(4) + msg_type(1) = 5 bytes
_MSG_HEADER = 5
# Record header: table_name_len(2) + table_name_utf8 + type(1) + byte_size(4)
#                + n_set(2) + set_indices + n_only_insert(2) + only_insert_indices
#                + n_values(2) + serialized values

_EPOCH_DT = datetime(1970, 1, 1, tzinfo=timezone.utc)
_EPOCH_DATE = date(1970, 1, 1)


def _serialize_value(val: Any) -> bytes:
    """Serialize a single Python value to bytes with a type tag prefix."""
    if val is None:
        return b"\x00"
    if isinstance(val, bool):
        return struct.pack("BB", TAG_BOOL, 1 if val else 0)
    if isinstance(val, int):
        try:
            return struct.pack("<Bq", TAG_INT, val)
        except struct.error:
            # Overflow: fall back to pickle
            pass
    if isinstance(val, float):
        return struct.pack("<Bd", TAG_FLOAT, val)
    if isinstance(val, str):
        encoded = val.encode("utf-8")
        return struct.pack("<BI", TAG_STR, len(encoded)) + encoded
    if isinstance(val, (bytes, bytearray)):
        return struct.pack("<BI", TAG_BYTES, len(val)) + bytes(val)
    if isinstance(val, datetime):
        if val.tzinfo is None:
            delta = val - datetime(1970, 1, 1)
            micros = int(delta.total_seconds() * 1_000_000)
            return struct.pack("<Bq", TAG_DATETIME_NAIVE, micros)
        else:
            delta = val - _EPOCH_DT
            micros = int(delta.total_seconds() * 1_000_000)
            return struct.pack("<Bq", TAG_DATETIME, micros)
    if isinstance(val, date):
        days = (val - _EPOCH_DATE).days
        return struct.pack("<Bi", TAG_DATE, days)
    if isinstance(val, Decimal):
        # Encode as string
        encoded = str(val).encode("utf-8")
        return struct.pack("<BI", TAG_STR, len(encoded)) + encoded
    # Fallback: pickle the value
    pickled = pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)
    return struct.pack("<BI", TAG_PICKLE, len(pickled)) + pickled


_pack_H = struct.Struct("<H").pack
_pack_BI = struct.Struct("<BI").pack
_pack_Bq = struct.Struct("<Bq").pack
_pack_Bd = struct.Struct("<Bd").pack
_pack_Bi = struct.Struct("<Bi").pack
_pack_BB = struct.Struct("BB").pack
_NONE_BYTE = b"\x00"


def _serialize_value_into(buf: bytearray, val: Any) -> None:
    """Serialize a single Python value directly into a bytearray."""
    if val is None:
        buf += _NONE_BYTE
        return
    if isinstance(val, str):
        encoded = val.encode("utf-8")
        buf += _pack_BI(TAG_STR, len(encoded))
        buf += encoded
        return
    if isinstance(val, int):
        if not isinstance(val, bool):
            try:
                buf += _pack_Bq(TAG_INT, val)
                return
            except struct.error:
                pass
        else:
            buf += _pack_BB(TAG_BOOL, 1 if val else 0)
            return
    if isinstance(val, float):
        buf += _pack_Bd(TAG_FLOAT, val)
        return
    if isinstance(val, (bytes, bytearray)):
        buf += _pack_BI(TAG_BYTES, len(val))
        buf += val
        return
    if isinstance(val, datetime):
        if val.tzinfo is None:
            delta = val - datetime(1970, 1, 1)
            micros = int(delta.total_seconds() * 1_000_000)
            buf += _pack_Bq(TAG_DATETIME_NAIVE, micros)
        else:
            delta = val - _EPOCH_DT
            micros = int(delta.total_seconds() * 1_000_000)
            buf += _pack_Bq(TAG_DATETIME, micros)
        return
    if isinstance(val, date):
        days = (val - _EPOCH_DATE).days
        buf += _pack_Bi(TAG_DATE, days)
        return
    if isinstance(val, Decimal):
        encoded = str(val).encode("utf-8")
        buf += _pack_BI(TAG_STR, len(encoded))
        buf += encoded
        return
    pickled = pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)
    buf += _pack_BI(TAG_PICKLE, len(pickled))
    buf += pickled


def _deserialize_value(buf: memoryview, offset: int) -> Tuple[Any, int]:
    """Deserialize a single value from buf at offset. Returns (value, new_offset)."""
    tag = buf[offset]
    offset += 1

    if tag == TAG_NONE:
        return None, offset
    if tag == TAG_BOOL:
        return bool(buf[offset]), offset + 1
    if tag == TAG_INT:
        val = struct.unpack_from("<q", buf, offset)[0]
        return val, offset + 8
    if tag == TAG_FLOAT:
        val = struct.unpack_from("<d", buf, offset)[0]
        return val, offset + 8
    if tag == TAG_STR:
        length = struct.unpack_from("<I", buf, offset)[0]
        offset += 4
        val = bytes(buf[offset : offset + length]).decode("utf-8")
        return val, offset + length
    if tag == TAG_BYTES:
        length = struct.unpack_from("<I", buf, offset)[0]
        offset += 4
        val = bytes(buf[offset : offset + length])
        return val, offset + length
    if tag == TAG_DATETIME:
        micros = struct.unpack_from("<q", buf, offset)[0]
        val = datetime(1970, 1, 1, tzinfo=timezone.utc) + __import__(
            "datetime"
        ).timedelta(microseconds=micros)
        return val, offset + 8
    if tag == TAG_DATETIME_NAIVE:
        micros = struct.unpack_from("<q", buf, offset)[0]
        val = datetime(1970, 1, 1) + __import__("datetime").timedelta(
            microseconds=micros
        )
        return val, offset + 8
    if tag == TAG_DATE:
        days = struct.unpack_from("<i", buf, offset)[0]
        val = date.fromordinal(_EPOCH_DATE.toordinal() + days)
        return val, offset + 4
    if tag == TAG_PICKLE:
        length = struct.unpack_from("<I", buf, offset)[0]
        offset += 4
        val = pickle.loads(bytes(buf[offset : offset + length]))
        return val, offset + length

    raise ValueError(f"Unknown value tag: {tag}")


def serialize_record_msg(
    table_name_key: bytes,
    record: Record,
) -> bytes:
    """Serialize a record into a compact binary message (no pickle for common types).

    Format:
      table_name_len(2B) + table_name_utf8 +
      type(1B) + byte_size(4B) +
      n_set(2B) + [set_col_index(2B)]... +
      n_only_insert(2B) + [only_insert_index(2B)]... +
      n_values(2B) + [serialized_value]...  (only for set columns)
    """
    set_cols = sorted(record._set_columns)
    only_insert = record._only_insert_columns
    n_set = len(set_cols)
    n_only = len(only_insert)

    buf = bytearray(
        len(table_name_key) + 2 + 5 + 2 + n_set * 2 + 2 + n_only * 2 + 2 + n_set * 10
    )
    buf.clear()

    # Table name key
    buf += _pack_H(len(table_name_key))
    buf += table_name_key

    # Type + byte_size
    type_int = _MUTATION_TYPE_TO_INT.get(record.type, 0)
    buf += _pack_BI(type_int, record.byte_size)

    # Set columns as packed indices
    buf += _pack_H(n_set)
    for idx in set_cols:
        buf += _pack_H(idx)

    # Only-insert columns
    only_sorted = sorted(only_insert) if n_only else ()
    buf += _pack_H(n_only)
    for idx in only_sorted:
        buf += _pack_H(idx)

    # Values (only for set columns, in order)
    buf += _pack_H(n_set)
    values = record.values
    for idx in set_cols:
        _serialize_value_into(buf, values[idx])

    return bytes(buf)


def deserialize_record_msg(
    data: memoryview,
    schema_registry: Dict[bytes, TableSchema],
    table_name_registry: Dict[bytes, TableName],
) -> Optional[Record]:
    """Deserialize a record from binary message.

    Returns None if schema not found.
    """
    offset = 0

    # Table name key
    tn_len = struct.unpack_from("<H", data, offset)[0]
    offset += 2
    tn_key = bytes(data[offset : offset + tn_len])
    offset += tn_len

    schema = schema_registry.get(tn_key)
    table_name = table_name_registry.get(tn_key)
    if schema is None or table_name is None:
        return None

    # Type + byte_size
    type_val, byte_size = struct.unpack_from("<BI", data, offset)
    offset += 5

    # Set columns
    n_set = struct.unpack_from("<H", data, offset)[0]
    offset += 2
    set_cols = set()
    for _ in range(n_set):
        idx = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        set_cols.add(idx)

    # Only-insert columns
    n_only = struct.unpack_from("<H", data, offset)[0]
    offset += 2
    only_insert = set()
    for _ in range(n_only):
        idx = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        only_insert.add(idx)

    # Values
    n_values = struct.unpack_from("<H", data, offset)[0]
    offset += 2
    values = [None] * schema.column_count
    set_cols_sorted = sorted(set_cols)
    for i in range(n_values):
        col_idx = set_cols_sorted[i]
        val, offset = _deserialize_value(data, offset)
        values[col_idx] = val

    # Reconstruct Record
    record = Record.__new__(Record)
    record.schema = schema
    record.table_name = table_name
    record.values = values
    record._set_columns = set_cols
    record._only_insert_columns = only_insert
    record.type = _INT_TO_MUTATION_TYPE.get(type_val, MutationType.INSERT)
    record.byte_size = byte_size
    record._futures = []
    return record


def serialize_get_msg(
    request_id: int,
    table_name_key: bytes,
    pk_values: List[Any],
    selected_columns: List[int],
) -> bytes:
    """Serialize a single Get request for multi-process read.

    Format:
      request_id(4B) + tn_key_len(2B) + tn_key +
      n_selected(2B) + [col_idx(2B)]... +
      n_pk(2B) + [serialized_value]...
    """
    parts = []
    parts.append(struct.pack("<I", request_id))
    parts.append(struct.pack("<H", len(table_name_key)))
    parts.append(table_name_key)

    # Selected columns
    parts.append(struct.pack("<H", len(selected_columns)))
    for col_idx in selected_columns:
        parts.append(struct.pack("<H", col_idx))

    # PK values
    parts.append(struct.pack("<H", len(pk_values)))
    for val in pk_values:
        parts.append(_serialize_value(val))

    return b"".join(parts)


def deserialize_get_msg(
    data: memoryview,
    schema_registry: Dict[bytes, TableSchema],
    table_name_registry: Dict[bytes, TableName],
) -> Optional[Tuple[int, TableSchema, TableName, List[int], List[Any]]]:
    """Deserialize a single get message.

    Returns (request_id, schema, table_name, selected_columns, pk_values)
    or None if schema not found.
    """
    offset = 0

    request_id = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    tn_len = struct.unpack_from("<H", data, offset)[0]
    offset += 2
    tn_key = bytes(data[offset : offset + tn_len])
    offset += tn_len

    schema = schema_registry.get(tn_key)
    table_name = table_name_registry.get(tn_key)
    if schema is None or table_name is None:
        return None

    # Selected columns
    n_selected = struct.unpack_from("<H", data, offset)[0]
    offset += 2
    selected_columns = []
    for _ in range(n_selected):
        col_idx = struct.unpack_from("<H", data, offset)[0]
        offset += 2
        selected_columns.append(col_idx)

    # PK values
    n_pk = struct.unpack_from("<H", data, offset)[0]
    offset += 2
    pk_values = []
    for _ in range(n_pk):
        val, offset = _deserialize_value(data, offset)
        pk_values.append(val)

    return request_id, schema, table_name, selected_columns, pk_values


class ShmRingBuffer:
    """Single-producer single-consumer ring buffer over shared memory.

    The parent (producer) calls write_*() methods.
    The worker (consumer) calls read() in a loop.
    """

    def __init__(self, name: str, capacity: int = 8 * 1024 * 1024):
        """Create or attach to a shared memory ring buffer.

        Args:
            name: Unique name for the shared memory segment.
            capacity: Size of the ring buffer in bytes (default 8MB).
        """
        self._name = name
        self._capacity = capacity
        self._shm = SharedMemory(name=name, create=True, size=capacity)
        self._buf = self._shm.buf
        # Positions are absolute (ever-increasing), modded by capacity on access
        self._write_pos = multiprocessing.Value("Q", 0, lock=False)
        self._read_pos = multiprocessing.Value("Q", 0, lock=False)

    @property
    def write_pos(self) -> multiprocessing.Value:
        return self._write_pos

    @property
    def read_pos(self) -> multiprocessing.Value:
        return self._read_pos

    @property
    def shm_name(self) -> str:
        return self._name

    @property
    def capacity(self) -> int:
        return self._capacity

    def _available_write(self) -> int:
        """How many bytes can be written before buffer is full."""
        return self._capacity - (self._write_pos.value - self._read_pos.value)

    def _available_read(self) -> int:
        """How many bytes are available to read."""
        return self._write_pos.value - self._read_pos.value

    def write_message(
        self, msg_type: int, payload: bytes, timeout: float = 5.0
    ) -> bool:
        """Write a message to the ring buffer.

        Blocks until space is available or timeout expires.
        Returns True on success, False on timeout.
        """
        total_len = _MSG_HEADER + len(payload)
        if total_len > self._capacity // 2:
            raise ValueError(f"Message too large: {total_len} > {self._capacity // 2}")

        deadline = time.monotonic() + timeout
        while self._available_write() < total_len:
            if time.monotonic() > deadline:
                return False
            time.sleep(0.0001)  # 100us spin

        wp = self._write_pos.value
        # Write header: [msg_len(4B)][msg_type(1B)]
        header = struct.pack("<IB", len(payload), msg_type)
        self._write_bytes(wp, header)
        wp += _MSG_HEADER
        # Write payload
        self._write_bytes(wp, payload)
        wp += len(payload)
        # Commit: update write_pos (atomic on x86 for aligned 8-byte writes)
        self._write_pos.value = wp
        return True

    def read_message(self, timeout: float = 1.0) -> Optional[Tuple[int, memoryview]]:
        """Read one message from the ring buffer.

        Returns (msg_type, payload_view) or None on timeout.
        The returned memoryview is only valid until the next read call.
        """
        deadline = time.monotonic() + timeout
        while self._available_read() < _MSG_HEADER:
            if time.monotonic() > deadline:
                return None
            time.sleep(0.0001)

        rp = self._read_pos.value

        # Read header
        header_bytes = self._read_bytes(rp, _MSG_HEADER)
        payload_len, msg_type = struct.unpack("<IB", header_bytes)
        rp += _MSG_HEADER

        # Wait for full payload
        while self._available_read() < _MSG_HEADER + payload_len:
            if time.monotonic() > deadline:
                return None
            time.sleep(0.0001)

        # Read payload
        payload = self._read_bytes(rp, payload_len)
        rp += payload_len

        # Commit: update read_pos
        self._read_pos.value = rp
        return msg_type, memoryview(payload)

    def _write_bytes(self, pos: int, data: bytes) -> None:
        """Write bytes at absolute position, wrapping around the ring buffer."""
        start = pos % self._capacity
        end = start + len(data)
        if end <= self._capacity:
            self._buf[start:end] = data
        else:
            # Wrap around
            first = self._capacity - start
            self._buf[start : self._capacity] = data[:first]
            self._buf[0 : len(data) - first] = data[first:]

    def _read_bytes(self, pos: int, length: int) -> bytes:
        """Read bytes from absolute position, wrapping around the ring buffer."""
        start = pos % self._capacity
        end = start + length
        if end <= self._capacity:
            return bytes(self._buf[start:end])
        else:
            first = self._capacity - start
            return bytes(self._buf[start : self._capacity]) + bytes(
                self._buf[0 : length - first]
            )

    def close(self) -> None:
        """Close and unlink the shared memory segment."""
        try:
            self._shm.close()
        except Exception:
            pass
        try:
            self._shm.unlink()
        except Exception:
            pass
