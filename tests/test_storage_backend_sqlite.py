"""
Tests for SQLiteStorage backend.

Verifies normalized schema (threads, turns, turn_files), migration from
legacy KV table, CRUD operations, query methods, and thread lifecycle.
"""

import json
import os
import sqlite3
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


def _make_thread_ctx(thread_id="t-1234", tool_name="consensus", turns=None, **kwargs):
    """Helper to build a ThreadContext-shaped dict."""
    ctx = {
        "thread_id": thread_id,
        "parent_thread_id": kwargs.get("parent_thread_id"),
        "tool_name": tool_name,
        "initial_context": kwargs.get("initial_context", {"step": "Evaluate X"}),
        "status": kwargs.get("status", "active"),
        "created_at": kwargs.get("created_at", "2026-03-25T10:00:00+00:00"),
        "last_updated_at": kwargs.get("last_updated_at", "2026-03-25T10:00:00+00:00"),
        "turns": turns or [],
    }
    return ctx


def _make_turn(role="assistant", content="response", turn_index=0, **kwargs):
    """Helper to build a turn dict."""
    return {
        "role": role,
        "content": content,
        "timestamp": kwargs.get("timestamp", "2026-03-25T10:01:00+00:00"),
        "files": kwargs.get("files"),
        "images": kwargs.get("images"),
        "tool_name": kwargs.get("tool_name", "consensus"),
        "model_provider": kwargs.get("model_provider"),
        "model_name": kwargs.get("model_name"),
        "model_metadata": kwargs.get("model_metadata"),
    }


class TestSQLiteStorageSchema:
    """Tests for schema creation and migration."""

    def test_fresh_install_creates_normalized_tables(self, storage):
        """Fresh install creates threads, turns, turn_files, schema_version tables."""
        conn = storage._get_connection()
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "threads" in tables
        assert "turns" in tables
        assert "turn_files" in tables
        assert "schema_version" in tables

    def test_schema_version_is_current(self, storage):
        """Schema version should be 2 after init."""
        assert storage._get_schema_version() == 2

    def test_migration_from_legacy_kv_table(self, tmp_path):
        """Migrate data from old conversation_store to normalized tables."""
        db_path = str(tmp_path / "legacy.db")

        # Create legacy schema and insert data
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE conversation_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                expires_at REAL NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
        """)
        ctx = _make_thread_ctx(
            thread_id="legacy-uuid",
            turns=[
                _make_turn(
                    role="user",
                    content="question",
                    model_provider="google",
                    model_name="gemini-2.5-flash",
                    files=["/src/main.py"],
                ),
                _make_turn(
                    role="assistant",
                    content="answer",
                    model_provider="openai",
                    model_name="gpt-4o",
                ),
            ],
        )
        now = time.time()
        conn.execute(
            "INSERT INTO conversation_store (key, value, expires_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (f"thread:{ctx['thread_id']}", json.dumps(ctx), now + 86400, now, now),
        )
        conn.commit()
        conn.close()

        # Open with SQLiteStorage — should auto-migrate
        s = SQLiteStorage(db_path=db_path)
        try:
            # Old table should be renamed to backup
            c = s._get_connection()
            tables = {row[0] for row in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            assert "conversation_store_backup" in tables
            assert "conversation_store" not in tables

            # Data should be in normalized tables
            loaded = s.load_thread("legacy-uuid")
            assert loaded is not None
            data = json.loads(loaded)
            assert data["thread_id"] == "legacy-uuid"
            assert len(data["turns"]) == 2
            assert data["turns"][0]["model_name"] == "gemini-2.5-flash"
            assert data["turns"][1]["model_name"] == "gpt-4o"

            # File references should be in turn_files
            cursor = c.execute("SELECT file_path FROM turn_files")
            files = [row[0] for row in cursor.fetchall()]
            assert "/src/main.py" in files
        finally:
            s.shutdown()

    def test_migration_handles_malformed_json(self, tmp_path):
        """Malformed rows are skipped during migration, not crash."""
        db_path = str(tmp_path / "bad.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE conversation_store (
                key TEXT PRIMARY KEY, value TEXT NOT NULL,
                expires_at REAL NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL
            )
        """)
        now = time.time()
        # One good row, one bad row
        good = _make_thread_ctx(thread_id="good-uuid")
        conn.execute(
            "INSERT INTO conversation_store VALUES (?, ?, ?, ?, ?)",
            ("thread:good-uuid", json.dumps(good), now + 86400, now, now),
        )
        conn.execute(
            "INSERT INTO conversation_store VALUES (?, ?, ?, ?, ?)",
            ("thread:bad-uuid", "not-json{{{", now + 86400, now, now),
        )
        conn.commit()
        conn.close()

        s = SQLiteStorage(db_path=db_path)
        try:
            assert s.load_thread("good-uuid") is not None
            assert s.load_thread("bad-uuid") is None
        finally:
            s.shutdown()

    def test_migration_is_idempotent(self, tmp_path):
        """Running migration twice doesn't duplicate data."""
        db_path = str(tmp_path / "idem.db")
        s1 = SQLiteStorage(db_path=db_path)
        s1.save_thread(_make_thread_ctx(thread_id="idem-1"))
        s1.shutdown()

        # Re-open — should not crash or duplicate
        s2 = SQLiteStorage(db_path=db_path)
        try:
            assert s2.load_thread("idem-1") is not None
            assert s2._get_schema_version() == 2
        finally:
            s2.shutdown()


class TestSQLiteStorageCRUD:
    """Tests for normalized CRUD operations."""

    def test_save_and_load_thread_roundtrip(self, storage):
        """Threads survive a save/load roundtrip."""
        ctx = _make_thread_ctx(thread_id="rt-1", tool_name="consensus")
        storage.save_thread(ctx)

        loaded = storage.load_thread("rt-1")
        assert loaded is not None
        data = json.loads(loaded)
        assert data["thread_id"] == "rt-1"
        assert data["tool_name"] == "consensus"
        assert data["turns"] == []

    def test_load_returns_none_for_missing_thread(self, storage):
        """Loading a nonexistent thread returns None."""
        assert storage.load_thread("nonexistent") is None

    def test_append_turn_inserts_single_row(self, storage):
        """append_turn adds exactly one row to turns table."""
        storage.save_thread(_make_thread_ctx(thread_id="at-1"))
        turn = _make_turn(
            model_provider="google",
            model_name="gemini-2.5-flash",
            files=["/a.py", "/b.py"],
            images=["/img.png"],
        )
        storage.append_turn("at-1", turn, 0)

        # Verify turn count
        assert storage.get_turn_count("at-1") == 1

        # Verify turn data via load
        data = json.loads(storage.load_thread("at-1"))
        assert len(data["turns"]) == 1
        assert data["turns"][0]["model_name"] == "gemini-2.5-flash"
        assert data["turns"][0]["files"] == ["/a.py", "/b.py"]
        assert data["turns"][0]["images"] == ["/img.png"]

    def test_append_multiple_turns(self, storage):
        """Multiple turns are ordered by turn_index."""
        storage.save_thread(_make_thread_ctx(thread_id="mt-1"))
        for i in range(5):
            storage.append_turn("mt-1", _make_turn(content=f"turn-{i}"), i)

        data = json.loads(storage.load_thread("mt-1"))
        assert len(data["turns"]) == 5
        assert data["turns"][0]["content"] == "turn-0"
        assert data["turns"][4]["content"] == "turn-4"

    def test_update_thread_timestamp(self, storage):
        """update_thread_timestamp changes last_updated_at and sets status active."""
        ctx = _make_thread_ctx(thread_id="ts-1", status="inactive")
        storage.save_thread(ctx)
        storage.update_thread_timestamp("ts-1", "2026-03-26T00:00:00+00:00")

        data = json.loads(storage.load_thread("ts-1"))
        assert data["last_updated_at"] == "2026-03-26T00:00:00+00:00"

        # Status should be set to active
        conn = storage._get_connection()
        status = conn.execute("SELECT status FROM threads WHERE thread_id = ?", ("ts-1",)).fetchone()[0]
        assert status == "active"

    def test_overwrite_thread(self, storage):
        """Saving a thread with same ID updates it."""
        storage.save_thread(_make_thread_ctx(thread_id="ow-1", tool_name="old"))
        storage.save_thread(
            _make_thread_ctx(thread_id="ow-1", tool_name="new", last_updated_at="2026-04-01T00:00:00+00:00")
        )
        data = json.loads(storage.load_thread("ow-1"))
        assert data["last_updated_at"] == "2026-04-01T00:00:00+00:00"

    def test_concurrent_writes(self, storage):
        """Multiple threads writing simultaneously don't corrupt data."""
        errors = []

        def writer(i):
            try:
                ctx = _make_thread_ctx(thread_id=f"cw-{i}")
                storage.save_thread(ctx)
                storage.append_turn(f"cw-{i}", _make_turn(content=f"v{i}"), 0)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        for i in range(10):
            data = json.loads(storage.load_thread(f"cw-{i}"))
            assert data["turns"][0]["content"] == f"v{i}"

    def test_thread_not_deleted_over_time(self, storage):
        """Threads are never deleted — data persists forever."""
        storage.save_thread(_make_thread_ctx(thread_id="forever-1"))
        storage.append_turn("forever-1", _make_turn(content="hello"), 0)

        # Simulate what _mark_inactive_threads does
        storage._mark_inactive_threads()

        # Thread should still be loadable
        data = json.loads(storage.load_thread("forever-1"))
        assert data["turns"][0]["content"] == "hello"

    def test_legacy_kv_interface_for_thread_keys(self, storage):
        """Legacy setex/get interface works for thread: keys."""
        ctx = _make_thread_ctx(thread_id="kv-1")
        ctx["turns"] = [_make_turn(content="via-kv")]
        storage.setex("thread:kv-1", 3600, json.dumps(ctx))

        result = storage.get("thread:kv-1")
        assert result is not None
        data = json.loads(result)
        assert data["thread_id"] == "kv-1"
        assert data["turns"][0]["content"] == "via-kv"


class TestSQLiteStorageQueries:
    """Tests for query methods (list, search, find)."""

    @pytest.fixture(autouse=True)
    def setup_data(self, storage):
        """Populate storage with test data for query tests."""
        self.storage = storage
        # Thread 1: consensus with google model
        storage.save_thread(
            _make_thread_ctx(
                thread_id="q-1",
                tool_name="consensus",
                initial_context={"step": "Evaluate Python vs Rust"},
                created_at="2026-03-20T10:00:00+00:00",
                last_updated_at="2026-03-20T10:05:00+00:00",
            )
        )
        storage.append_turn(
            "q-1",
            _make_turn(
                content="Python is great for CLI",
                model_name="gemini-2.5-flash",
                model_provider="google",
                files=["/src/main.py"],
            ),
            0,
        )
        storage.append_turn(
            "q-1",
            _make_turn(
                content="Rust compiles to single binary",
                model_name="gpt-4o",
                model_provider="openai",
            ),
            1,
        )

        # Thread 2: consensus with openai model
        storage.save_thread(
            _make_thread_ctx(
                thread_id="q-2",
                tool_name="consensus",
                initial_context={"step": "Review auth middleware"},
                created_at="2026-03-21T10:00:00+00:00",
                last_updated_at="2026-03-21T10:05:00+00:00",
            )
        )
        storage.append_turn(
            "q-2",
            _make_turn(
                content="Auth looks good",
                model_name="gpt-4o",
                model_provider="openai",
                files=["/auth/middleware.py"],
            ),
            0,
        )

    def test_list_threads_all(self):
        """list_threads returns all threads."""
        results, total = self.storage.list_threads()
        assert total == 2
        assert len(results) == 2

    def test_list_threads_by_tool(self):
        """Filter threads by tool_name."""
        results, total = self.storage.list_threads(tool_name="consensus")
        assert total == 2
        results, total = self.storage.list_threads(tool_name="nonexistent")
        assert total == 0

    def test_list_threads_by_model(self):
        """Filter threads by model used in turns."""
        results, _ = self.storage.list_threads(model_name="gemini-2.5-flash")
        assert len(results) == 1
        assert results[0]["thread_id"] == "q-1"

    def test_list_threads_by_date(self):
        """Filter threads by date range."""
        results, _ = self.storage.list_threads(after="2026-03-21T00:00:00+00:00")
        assert len(results) == 1
        assert results[0]["thread_id"] == "q-2"

    def test_list_threads_pagination(self):
        """Pagination with limit/offset."""
        results, total = self.storage.list_threads(limit=1, offset=0)
        assert total == 2
        assert len(results) == 1
        first_id = results[0]["thread_id"]

        results, _ = self.storage.list_threads(limit=1, offset=1)
        assert len(results) == 1
        assert results[0]["thread_id"] != first_id

    def test_list_threads_returns_models_used(self):
        """Thread summaries include models_used list."""
        results, _ = self.storage.list_threads()
        q1 = next(r for r in results if r["thread_id"] == "q-1")
        assert "gemini-2.5-flash" in q1["models_used"]
        assert "gpt-4o" in q1["models_used"]

    def test_search_turns_content(self):
        """Search finds turns matching content."""
        results = self.storage.search_turns("single binary")
        assert len(results) >= 1
        assert any(r["model_name"] == "gpt-4o" for r in results)

    def test_search_turns_with_model_filter(self):
        """Search with model filter narrows results."""
        results = self.storage.search_turns("great", model_name="gemini-2.5-flash")
        assert len(results) >= 1
        results = self.storage.search_turns("great", model_name="gpt-4o")
        assert len(results) == 0

    def test_find_threads_by_file(self):
        """Find threads referencing a specific file."""
        results = self.storage.find_threads_by_file("/src/main.py")
        assert len(results) == 1
        assert results[0]["thread_id"] == "q-1"

        results = self.storage.find_threads_by_file("/auth/middleware.py")
        assert len(results) == 1
        assert results[0]["thread_id"] == "q-2"

        results = self.storage.find_threads_by_file("/nonexistent.py")
        assert len(results) == 0


class TestSQLiteStorageLifecycle:
    """Tests for DB lifecycle: auto-creation, custom path, shutdown."""

    def test_db_directory_auto_creation(self, tmp_path):
        """DB directory is created automatically if it doesn't exist."""
        nested = tmp_path / "a" / "b" / "c"
        db_path = str(nested / "test.db")
        s = SQLiteStorage(db_path=db_path)
        try:
            s.save_thread(_make_thread_ctx(thread_id="auto-1"))
            assert s.load_thread("auto-1") is not None
            assert nested.exists()
        finally:
            s.shutdown()

    def test_custom_db_path_via_env_var(self, tmp_path, monkeypatch):
        """PAL_DB_PATH env var overrides the default path."""
        custom_path = str(tmp_path / "custom.db")
        monkeypatch.setenv("PAL_DB_PATH", custom_path)
        s = SQLiteStorage()
        try:
            s.save_thread(_make_thread_ctx(thread_id="env-1"))
            assert s.load_thread("env-1") is not None
            assert os.path.exists(custom_path)
        finally:
            s.shutdown()

    def test_shutdown_closes_connection(self, tmp_path):
        """After shutdown, the thread-local connection is cleared."""
        db_path = str(tmp_path / "shutdown_test.db")
        s = SQLiteStorage(db_path=db_path)
        s.save_thread(_make_thread_ctx(thread_id="sd-1"))
        s.shutdown()
        assert not hasattr(s._local, "conn") or s._local.conn is None
