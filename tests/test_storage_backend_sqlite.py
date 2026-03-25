"""
Tests for SQLiteStorage backend.

Verifies roundtrip storage, TTL expiration, concurrent writes,
auto-creation of DB directory, custom path via env var, cleanup,
and graceful shutdown.
"""

import os
import threading
import time

import pytest

from utils.storage_backend import SQLiteStorage


@pytest.fixture
def storage(tmp_path):
    """Create a SQLiteStorage instance using a temp directory."""
    db_path = str(tmp_path / "test.db")
    s = SQLiteStorage(db_path=db_path)
    yield s
    s.shutdown()


class TestSQLiteStorage:
    def test_setex_and_get_roundtrip(self, storage):
        """Values survive a set/get roundtrip."""
        storage.setex("key1", 3600, "value1")
        assert storage.get("key1") == "value1"

    def test_get_returns_none_for_missing_key(self, storage):
        """Missing keys return None."""
        assert storage.get("nonexistent") is None

    def test_ttl_expiration(self, storage):
        """Values expire after their TTL."""
        storage.setex("ephemeral", 1, "gone_soon")
        assert storage.get("ephemeral") == "gone_soon"
        time.sleep(1.1)
        assert storage.get("ephemeral") is None

    def test_overwrite_existing_key(self, storage):
        """Overwriting a key replaces the value."""
        storage.setex("key", 3600, "old")
        storage.setex("key", 3600, "new")
        assert storage.get("key") == "new"

    def test_concurrent_writes_from_multiple_threads(self, storage):
        """10 threads writing simultaneously should not corrupt data."""
        errors = []

        def writer(i):
            try:
                storage.setex(f"thread_{i}", 3600, f"value_{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        for i in range(10):
            assert storage.get(f"thread_{i}") == f"value_{i}"

    def test_db_directory_auto_creation(self, tmp_path):
        """DB directory is created automatically if it doesn't exist."""
        nested = tmp_path / "a" / "b" / "c"
        db_path = str(nested / "test.db")
        s = SQLiteStorage(db_path=db_path)
        try:
            s.setex("k", 3600, "v")
            assert s.get("k") == "v"
            assert nested.exists()
        finally:
            s.shutdown()

    def test_custom_db_path_via_env_var(self, tmp_path, monkeypatch):
        """PAL_DB_PATH env var overrides the default path."""
        custom_path = str(tmp_path / "custom.db")
        monkeypatch.setenv("PAL_DB_PATH", custom_path)
        s = SQLiteStorage()
        try:
            s.setex("env_key", 3600, "env_val")
            assert s.get("env_key") == "env_val"
            assert os.path.exists(custom_path)
        finally:
            s.shutdown()

    def test_cleanup_removes_expired_entries(self, storage):
        """Manual cleanup removes expired entries."""
        storage.setex("alive", 3600, "keep")
        storage.setex("dead", 1, "remove")
        time.sleep(1.1)

        storage._cleanup_expired()

        assert storage.get("alive") == "keep"
        assert storage.get("dead") is None

        # Verify the row is actually gone from the DB
        conn = storage._get_connection()
        cursor = conn.execute(
            "SELECT COUNT(*) FROM conversation_store WHERE key='dead'"
        )
        assert cursor.fetchone()[0] == 0

    def test_shutdown_closes_connection(self, tmp_path):
        """After shutdown, the thread-local connection is closed."""
        db_path = str(tmp_path / "shutdown_test.db")
        s = SQLiteStorage(db_path=db_path)
        s.setex("k", 3600, "v")
        s.shutdown()

        # The thread-local conn should be None after shutdown
        assert not hasattr(s._local, "conn") or s._local.conn is None

    def test_set_with_ttl_alias(self, storage):
        """set_with_ttl is an alias for setex."""
        storage.set_with_ttl("alias_key", 3600, "alias_val")
        assert storage.get("alias_key") == "alias_val"
