from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from .types import OnConflictAction, WriteFailStrategy
from ._version import __version__

logger = logging.getLogger("hologres.config")


@dataclass
class HoloConfig:
    """Configuration for HoloClient.

    Required fields: host, port, database, username, password.
    """

    # Connection
    host: str = None
    port: int = None
    database: str = None
    username: str = None
    password: str = None
    app_name: str = f"holo-client-py-{__version__}"

    # Write settings
    write_batch_size: int = 512
    write_batch_byte_size: int = 2 * 1024 * 1024  # 2MB
    write_batch_total_byte_size: int = 20 * 1024 * 1024  # 20MB
    write_max_interval_ms: int = 10_000  # 10 seconds
    write_parallelism: int = 4
    on_conflict_action: OnConflictAction = OnConflictAction.INSERT_OR_REPLACE
    enable_deduplication: bool = True
    enable_generate_binlog: bool = True
    remove_u0000_in_text: bool = True
    write_fail_strategy: WriteFailStrategy = WriteFailStrategy.TRY_ONE_BY_ONE

    # Read settings
    read_batch_size: int = 128
    read_batch_queue_size: int = 256
    read_parallelism: int = 4
    read_timeout_ms: int = 0  # 0 = no timeout
    read_retry_count: int = 1  # 1 = no retry

    # Fixed FE
    use_fixed_fe: bool = False

    # Shared memory settings
    shm_size: int = 8 * 1024 * 1024  # 8MB per worker

    # Retry / connection settings
    retry_count: int = 3
    retry_sleep_init_ms: int = 1_000
    retry_sleep_step_ms: int = 10_000
    connection_max_idle_ms: int = 60_000  # 1 minute
    meta_cache_ttl_ms: int = 60_000  # 1 minute

    def validate(self) -> None:
        """Validate the config, raising ValueError on invalid settings."""
        if not self.host:
            raise ValueError("host is required")
        if not self.database:
            raise ValueError("database is required")
        if not self.username:
            raise ValueError("username is required")
        if not self.password:
            raise ValueError("password is required")
        if self.write_batch_size <= 0:
            raise ValueError("write_batch_size must be > 0")
        if self.write_batch_byte_size <= 0:
            raise ValueError("write_batch_byte_size must be > 0")
        if self.write_max_interval_ms <= 0:
            raise ValueError("write_max_interval_ms must be > 0")
        if self.write_parallelism < 1:
            raise ValueError("write_parallelism must be >= 1")
        if self.read_parallelism < 1:
            raise ValueError("read_parallelism must be >= 1")
        if self.shm_size < 1024 * 1024:
            raise ValueError("shm_size must be >= 1MB")

    @property
    def conninfo(self) -> str:
        """PostgreSQL connection string (regular FE)."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.username} password={self.password} "
            f"application_name={self.app_name}"
        )

    @property
    def fixed_fe_conninfo(self) -> str:
        """PostgreSQL connection string routed to Fixed FE."""
        return f"{self.conninfo} options=type=fixed"
