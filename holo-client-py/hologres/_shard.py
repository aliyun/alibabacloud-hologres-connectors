"""Distribution key hashing and shard routing.

Matches Java's ShardUtil (MurmurHash3 with seed 104729, range 65536).
"""

from __future__ import annotations

import datetime
import struct
import uuid as _uuid_mod
from decimal import Decimal
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from .column import Column
    from .record import Record

from .column import (
    ARRAY,
    DATE,
    DECIMAL,
    NUMERIC,
    OTHER,
    TIME,
    TIMESTAMP,
    TIMESTAMP_WITH_TIMEZONE,
    VARCHAR,
)

SEED = 104729
RANGE_END = 65536

# Java epoch for dates: 1970-01-01
_EPOCH_DATE = datetime.date(1970, 1, 1)
# Microseconds per second/millisecond
_MICROS_PER_SEC = 1_000_000
_MICROS_PER_MS = 1_000
_NANOS_PER_MICRO = 1_000


def _murmur3_32(data: bytes, seed: int = SEED) -> int:
    """MurmurHash3 32-bit, matching Guava's Hashing.murmur3_32(seed)."""
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    mask = 0xFFFFFFFF

    h1 = seed & mask
    length = len(data)
    n_blocks = length // 4

    for i in range(n_blocks):
        k1 = struct.unpack_from("<I", data, i * 4)[0]
        k1 = (k1 * c1) & mask
        k1 = ((k1 << 15) | (k1 >> 17)) & mask
        k1 = (k1 * c2) & mask
        h1 ^= k1
        h1 = ((h1 << 13) | (h1 >> 19)) & mask
        h1 = (h1 * 5 + 0xE6546B64) & mask

    tail_idx = n_blocks * 4
    k1 = 0
    tail_size = length & 3
    if tail_size >= 3:
        k1 ^= data[tail_idx + 2] << 16
    if tail_size >= 2:
        k1 ^= data[tail_idx + 1] << 8
    if tail_size >= 1:
        k1 ^= data[tail_idx]
        k1 = (k1 * c1) & mask
        k1 = ((k1 << 15) | (k1 >> 17)) & mask
        k1 = (k1 * c2) & mask
        h1 ^= k1

    h1 ^= length
    # fmix32
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & mask
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & mask
    h1 ^= h1 >> 16

    # Convert to signed 32-bit int (matching Java's int)
    if h1 >= 0x80000000:
        h1 -= 0x100000000
    return h1


# Precompute NULL hash: hash of empty string, same as Java's NULL_HASH_CODE
_NULL_HASH = _murmur3_32(b"")
NULL_HASH_CODE = (
    _NULL_HASH % RANGE_END
    if _NULL_HASH >= 0
    else ((_NULL_HASH % RANGE_END) + RANGE_END) % RANGE_END
)


def _get_storage_value(obj, column: Column):
    """Convert a value to its storage representation for hashing.

    Matches Java's ShardUtil.getStorageValue(Record, int).
    """
    if obj is None:
        return None

    col_type = column.type
    type_name = column.type_name.lower().strip()

    if col_type == TIMESTAMP or col_type == TIMESTAMP_WITH_TIMEZONE:
        if type_name == "timestamp":
            # timestamp (without tz): microseconds since epoch, UTC
            return _timestamp_to_microseconds(obj, utc=True)
        else:
            # timestamptz: milliseconds since epoch, local timezone
            return _timestamp_to_microseconds(obj, utc=False) // _MICROS_PER_MS

    elif col_type == DATE:
        return _date_to_epoch_day(obj)

    elif col_type in (NUMERIC, DECIMAL):
        return _decimal_to_le_bytes(obj, column.scale)

    elif col_type == TIME:
        if type_name == "time":
            # time (without tz): microseconds since midnight
            return _time_to_microseconds(obj)
        # timetz falls through to default

    # UUID: convert to 16-byte big-endian binary (MSB + LSB)
    # Matches Java's ShardUtil case Types.OTHER for uuid columns.
    # Python maps uuid to VARCHAR, Java maps to OTHER — check type_name.
    if type_name == "uuid":
        return _uuid_to_bytes(obj)

    return obj


def _timestamp_to_microseconds(obj, utc: bool) -> int:
    """Convert timestamp to microseconds since epoch.

    Matches Java's TimestampUtil.timestampToMicroSecond().
    For "timestamp" type: uses UTC (no timezone shift).
    For "timestamptz" type: uses system local timezone.
    """
    if isinstance(obj, datetime.datetime):
        if utc:
            # Treat as UTC regardless of tzinfo
            if obj.tzinfo is not None:
                epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                delta = obj - epoch
            else:
                epoch = datetime.datetime(1970, 1, 1)
                delta = obj - epoch
            total_seconds = int(delta.total_seconds())
            micros = delta.microseconds
        else:
            # Use local timezone
            if obj.tzinfo is None:
                obj = obj.astimezone()  # attach local tz
            epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            delta = obj - epoch
            total_seconds = int(delta.total_seconds())
            micros = delta.microseconds
        return total_seconds * _MICROS_PER_SEC + micros
    elif isinstance(obj, (int, float)):
        # Treat as milliseconds since epoch (matching Java's Number branch)
        ms = int(obj)
        return ms * _MICROS_PER_MS
    elif isinstance(obj, str):
        dt = datetime.datetime.fromisoformat(obj)
        return _timestamp_to_microseconds(dt, utc)
    else:
        # Fall through to default string hash
        return str(obj)


def _date_to_epoch_day(obj) -> int:
    """Convert date to days since 1970-01-01.

    Matches Java's LocalDate.toEpochDay().
    """
    if isinstance(obj, datetime.datetime):
        obj = obj.date()
    if isinstance(obj, datetime.date):
        return (obj - _EPOCH_DATE).days
    elif isinstance(obj, str):
        return (datetime.date.fromisoformat(obj) - _EPOCH_DATE).days
    elif isinstance(obj, (int, float)):
        # Treat as milliseconds since epoch (matching Java's Number branch)
        d = datetime.date.fromtimestamp(int(obj) / 1000)
        return (d - _EPOCH_DATE).days
    return str(obj)


def _decimal_to_le_bytes(obj, scale: int) -> bytes:
    """Convert decimal to 16-byte little-endian unscaled value.

    Matches Java's BigDecimal.setScale(scale, HALF_UP).unscaledValue()
    reversed into little-endian 16-byte array.
    """
    if not isinstance(obj, Decimal):
        obj = Decimal(str(obj))

    # Rescale to column's declared scale (matching Java's HALF_UP)
    # Use sufficient precision for decimal(38, 18) — Java's BigDecimal is unlimited
    import decimal

    with decimal.localcontext() as ctx:
        ctx.prec = 50
        ctx.rounding = decimal.ROUND_HALF_UP
        rescaled = obj.quantize(Decimal(10) ** -scale)

    # Get unscaled integer value
    sign, digits, exp = rescaled.as_tuple()
    unscaled = int("".join(str(d) for d in digits)) if digits else 0
    if sign:
        unscaled = -unscaled

    # Convert to big-endian bytes (matching Java's BigInteger.toByteArray())
    if unscaled == 0:
        big_endian = b"\x00"
    else:
        # Java's toByteArray() returns signed two's complement big-endian
        if unscaled > 0:
            byte_len = (unscaled.bit_length() + 8) // 8  # +1 for sign bit
            big_endian = unscaled.to_bytes(byte_len, "big", signed=True)
        else:
            byte_len = (unscaled.bit_length() + 9) // 8  # +1 for sign bit
            big_endian = unscaled.to_bytes(byte_len, "big", signed=True)

    if len(big_endian) > 16:
        raise OverflowError(f"{obj} is too large to store as decimal")

    # Reverse to little-endian, then zero-pad to 16 bytes
    le = big_endian[::-1]
    result = bytearray(16)
    result[: len(le)] = le
    return bytes(result)


def _time_to_microseconds(obj) -> int:
    """Convert time to microseconds since midnight.

    Matches Java's LocalTime.toNanoOfDay() / 1000.
    """
    if isinstance(obj, datetime.time):
        return (
            obj.hour * 3600 + obj.minute * 60 + obj.second
        ) * _MICROS_PER_SEC + obj.microsecond
    elif isinstance(obj, datetime.timedelta):
        total_seconds = int(obj.total_seconds())
        micros = obj.microseconds
        return total_seconds * _MICROS_PER_SEC + micros
    elif isinstance(obj, str):
        t = datetime.time.fromisoformat(obj)
        return _time_to_microseconds(t)
    return str(obj)


def _uuid_to_bytes(obj) -> bytes:
    """Convert UUID to 16-byte big-endian binary (MSB then LSB).

    Matches Java's ShardUtil: UUID.getMostSignificantBits() (8 bytes)
    followed by UUID.getLeastSignificantBits() (8 bytes), big-endian.
    The result is hashed as byte[] via murmur3.
    """
    if isinstance(obj, _uuid_mod.UUID):
        return obj.bytes  # uuid.UUID.bytes is already 16-byte big-endian
    # String input: parse first
    return _uuid_mod.UUID(str(obj)).bytes


def _hash_object(obj) -> int:
    """Hash a single value, matching Java's ShardUtil.hash(Object)."""
    if obj is None:
        return NULL_HASH_CODE
    if isinstance(obj, bool):
        return _hash_object(1 if obj else 0)
    if isinstance(obj, (bytes, bytearray)):
        return _murmur3_32(bytes(obj))
    if isinstance(obj, (list, tuple)):
        # Array hash: hash * 31 + childHash, matching Java's array branch
        h = 0
        for child in obj:
            h = ((h * 31) & 0xFFFFFFFF) + (0 if child is None else _hash_object(child))
            # Keep as signed 32-bit
            h &= 0xFFFFFFFF
        if h >= 0x80000000:
            h -= 0x100000000
        return h
    # Default: string representation, UTF-8 encoded
    return _murmur3_32(str(obj).encode("utf-8"))


def hash_record(record: Record, dk_indices: List[int]) -> int:
    """Hash a record's distribution key columns, matching Java's ShardUtil.hash(Record, int[]).

    Returns raw hash (signed 32-bit int). Use with unsigned remainder for shard routing.
    """
    if not dk_indices:
        return hash(id(record))

    schema = record.schema
    h = 0
    first = True
    for idx in dk_indices:
        column = schema.get_column(idx)
        # Skip generated columns (matching Java)
        if column.is_generated_column:
            continue
        obj = record.values[idx]
        obj = _get_storage_value(obj, column)
        if first:
            h = _hash_object(obj)
            first = False
        else:
            h ^= _hash_object(obj)
    return h


def split_range(num_shards: int) -> List[int]:
    """Build shard boundary starts, matching Java's ShardUtil.split().

    The first ``RANGE_END % num_shards`` shards each get one extra slot.
    Returns a list of length *num_shards* with each shard's start offset.
    """
    base = RANGE_END // num_shards
    remain = RANGE_END % num_shards
    starts: List[int] = []
    pos = 0
    for i in range(num_shards):
        starts.append(pos)
        pos += base + (1 if i < remain else 0)
    return starts


def compute_shard(record: Record, num_shards: int) -> int:
    """Compute the shard index for a record based on its distribution key."""
    if num_shards <= 1:
        return 0

    schema = record.schema
    dk_indices = schema.distribution_key_index
    if not dk_indices:
        dk_indices = schema.pk_index
    if not dk_indices:
        return hash(id(record)) % num_shards

    raw_hash = hash_record(record, dk_indices)
    unsigned = raw_hash & 0xFFFFFFFF
    slot = unsigned % RANGE_END

    base = RANGE_END // num_shards
    remain = RANGE_END % num_shards
    threshold = remain * (base + 1)
    if slot < threshold:
        return slot // (base + 1)
    return remain + (slot - threshold) // base
