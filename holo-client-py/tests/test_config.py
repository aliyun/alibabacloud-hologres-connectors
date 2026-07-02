"""Unit tests for HoloConfig."""

import pytest

from hologres import HoloConfig, OnConflictAction


class TestHoloConfig:
    """Tests for HoloConfig validation and properties."""

    def test_config_requires_host(self):
        """Test that host is required."""
        config = HoloConfig(
            port=80,
            database="test_db",
            username="user",
            password="pass",
        )
        with pytest.raises(ValueError, match="host is required"):
            config.validate()

    def test_config_requires_database(self):
        """Test that database is required."""
        config = HoloConfig(
            host="localhost",
            port=80,
            username="user",
            password="pass",
        )
        with pytest.raises(ValueError, match="database is required"):
            config.validate()

    def test_config_requires_username(self):
        """Test that username is required."""
        config = HoloConfig(
            host="localhost",
            port=80,
            database="test_db",
            password="pass",
        )
        with pytest.raises(ValueError, match="username is required"):
            config.validate()

    def test_config_requires_password(self):
        """Test that password is required."""
        config = HoloConfig(
            host="localhost",
            port=80,
            database="test_db",
            username="user",
        )
        with pytest.raises(ValueError, match="password is required"):
            config.validate()

    def test_config_validates_write_batch_size(self):
        """Test that write_batch_size must be positive."""
        config = HoloConfig(
            host="localhost",
            port=80,
            database="test_db",
            username="user",
            password="pass",
            write_batch_size=0,
        )
        with pytest.raises(ValueError, match="write_batch_size must be > 0"):
            config.validate()

    def test_config_validates_write_batch_byte_size(self):
        """Test that write_batch_byte_size must be positive."""
        config = HoloConfig(
            host="localhost",
            port=80,
            database="test_db",
            username="user",
            password="pass",
            write_batch_byte_size=-1,
        )
        with pytest.raises(ValueError, match="write_batch_byte_size must be > 0"):
            config.validate()

    def test_config_validates_write_max_interval_ms(self):
        """Test that write_max_interval_ms must be positive."""
        config = HoloConfig(
            host="localhost",
            port=80,
            database="test_db",
            username="user",
            password="pass",
            write_max_interval_ms=0,
        )
        with pytest.raises(ValueError, match="write_max_interval_ms must be > 0"):
            config.validate()

    def test_config_default_values(self):
        """Test default configuration values."""
        config = HoloConfig(
            host="localhost",
            port=80,
            database="test_db",
            username="user",
            password="pass",
        )
        config.validate()

        assert config.write_batch_size == 512
        assert config.write_batch_byte_size == 2 * 1024 * 1024
        assert config.write_batch_total_byte_size == 20 * 1024 * 1024
        assert config.write_max_interval_ms == 10_000
        assert config.on_conflict_action == OnConflictAction.INSERT_OR_REPLACE
        assert config.enable_deduplication is True
        assert config.retry_count == 3
        assert config.app_name == "holo-client-py-0.1.0"

    def test_config_conninfo(self):
        """Test connection string generation."""
        config = HoloConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
            app_name="my_app",
        )
        conninfo = config.conninfo

        assert "host=localhost" in conninfo
        assert "port=5432" in conninfo
        assert "dbname=test_db" in conninfo
        assert "user=test_user" in conninfo
        assert "password=test_pass" in conninfo
        assert "application_name=my_app" in conninfo

    def test_config_custom_values(self):
        """Test custom configuration values."""
        config = HoloConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="user",
            password="pass",
            write_batch_size=1024,
            write_batch_byte_size=4 * 1024 * 1024,
            write_max_interval_ms=5000,
            on_conflict_action=OnConflictAction.INSERT_OR_UPDATE,
            enable_deduplication=False,
            retry_count=5,
        )
        config.validate()

        assert config.write_batch_size == 1024
        assert config.write_batch_byte_size == 4 * 1024 * 1024
        assert config.write_max_interval_ms == 5000
        assert config.on_conflict_action == OnConflictAction.INSERT_OR_UPDATE
        assert config.enable_deduplication is False
        assert config.retry_count == 5
