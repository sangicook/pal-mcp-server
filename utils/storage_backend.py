"""
Storage backends for conversation threads

This module provides thread-safe storage backends for conversation contexts:

1. InMemoryStorage - Ephemeral, process-specific storage (data lost on restart)
2. SQLiteStorage - Persistent storage using SQLite with WAL mode (survives restarts)

The default backend is SQLite. Override via PAL_STORAGE_BACKEND env var ("memory" or "sqlite").

Key Features:
- Thread-safe operations using locks
- Normalized schema (threads, turns, turn_files) for rich queries
- Auto-migration from legacy KV schema
- No TTL-based deletion — data is kept forever
- Background thread marks inactive threads (informational only)
- Singleton pattern for consistent state within a single process
- SQLite WAL mode for concurrent read access
"""

import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Optional

from utils.env import get_env

logger = logging.getLogger(__name__)

# Current schema version
SCHEMA_VERSION = 2

# Thread status constants
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"


class InMemoryStorage:
    """Thread-safe in-memory storage for conversation threads"""

    def __init__(self):
        self._store: dict[str, tuple[str, float]] = {}
        self._lock = threading.Lock()
        # Match Redis behavior: cleanup interval based on conversation timeout
        # Run cleanup at 1/10th of timeout interval (e.g., 18 mins for 3 hour timeout)
        timeout_hours = int(get_env("CONVERSATION_TIMEOUT_HOURS", "3") or "3")
        self._cleanup_interval = (timeout_hours * 3600) // 10
        self._cleanup_interval = max(300, self._cleanup_interval)  # Minimum 5 minutes
        self._shutdown = False

        # Start background cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_worker, daemon=True)
        self._cleanup_thread.start()

        logger.info(
            f"In-memory storage initialized with {timeout_hours}h timeout, cleanup every {self._cleanup_interval//60}m"
        )

    def set_with_ttl(self, key: str, ttl_seconds: int, value: str) -> None:
        """Store value with expiration time"""
        with self._lock:
            expires_at = time.time() + ttl_seconds
            self._store[key] = (value, expires_at)
            logger.debug(f"Stored key {key} with TTL {ttl_seconds}s")

    def get(self, key: str) -> Optional[str]:
        """Retrieve value if not expired"""
        with self._lock:
            if key in self._store:
                value, expires_at = self._store[key]
                if time.time() < expires_at:
                    logger.debug(f"Retrieved key {key}")
                    return value
                else:
                    # Clean up expired entry
                    del self._store[key]
                    logger.debug(f"Key {key} expired and removed")
        return None

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        """Redis-compatible setex method"""
        self.set_with_ttl(key, ttl_seconds, value)

    def _cleanup_worker(self):
        """Background thread that periodically cleans up expired entries"""
        while not self._shutdown:
            time.sleep(self._cleanup_interval)
            self._cleanup_expired()

    def _cleanup_expired(self):
        """Remove all expired entries"""
        with self._lock:
            current_time = time.time()
            expired_keys = [k for k, (_, exp) in self._store.items() if exp < current_time]
            for key in expired_keys:
                del self._store[key]

            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired conversation threads")

    def shutdown(self):
        """Graceful shutdown of background thread"""
        self._shutdown = True
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=1)


class SQLiteStorage:
    """Persistent SQLite storage for conversation threads.

    Uses a normalized schema (threads, turns, turn_files) for rich queries.
    WAL journal mode for concurrent read access. Data is kept forever —
    no TTL-based deletion.
    """

    def __init__(self, db_path=None):
        if db_path is None:
            db_path = get_env("PAL_DB_PATH") or os.path.join(os.path.expanduser("~"), ".pal-mcp", "conversations.db")
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._connections: list[sqlite3.Connection] = []
        self._conn_lock = threading.Lock()
        self._shutdown_flag = False
        self._has_fts = False

        self._run_migrations()

        self._cleanup_thread = threading.Thread(target=self._inactive_worker, daemon=True)
        self._cleanup_thread.start()

        logger.info(f"SQLite storage initialized at {db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get a thread-local SQLite connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self._db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
            with self._conn_lock:
                self._connections.append(conn)
        return self._local.conn

    def _get_schema_version(self) -> int:
        """Read current schema version. Returns 0 if no version table."""
        conn = self._get_connection()
        try:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'")
            if cursor.fetchone() is None:
                return 0
            cursor = conn.execute("SELECT MAX(version) FROM schema_version")
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else 0
        except Exception:
            return 0

    def _set_schema_version(self, conn: sqlite3.Connection, version: int, description: str) -> None:
        """Record a schema version."""
        conn.execute(
            "INSERT INTO schema_version (version, applied_at, description) VALUES (?, ?, ?)",
            (version, time.time(), description),
        )

    def _run_migrations(self) -> None:
        """Check schema version and apply pending migrations."""
        current = self._get_schema_version()

        if current >= SCHEMA_VERSION:
            # Check FTS availability
            self._has_fts = self._check_fts_exists()
            return

        conn = self._get_connection()

        if current < 1:
            self._migrate_to_v1(conn)
        if current < 2:
            self._migrate_to_v2(conn)

        self._has_fts = self._check_fts_exists()

    def _migrate_to_v1(self, conn: sqlite3.Connection) -> None:
        """Create normalized tables and migrate data from legacy KV table if present."""
        logger.info("Running migration v0 → v1: Creating normalized tables")

        # Create schema_version table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at REAL NOT NULL,
                description TEXT
            )
        """)

        # Create normalized tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS threads (
                thread_id TEXT PRIMARY KEY,
                parent_thread_id TEXT,
                tool_name TEXT NOT NULL,
                initial_context TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                last_updated_at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_threads_tool_name ON threads(tool_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_threads_created_at ON threads(created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_threads_last_updated ON threads(last_updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_threads_status ON threads(status)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL REFERENCES threads(thread_id),
                turn_index INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                tool_name TEXT,
                model_provider TEXT,
                model_name TEXT,
                model_metadata TEXT,
                timestamp TEXT NOT NULL,
                UNIQUE(thread_id, turn_index)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_turns_thread_id ON turns(thread_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_turns_model_name ON turns(model_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_turns_model_provider ON turns(model_provider)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_turns_timestamp ON turns(timestamp)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS turn_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                turn_id INTEGER NOT NULL REFERENCES turns(id),
                file_path TEXT NOT NULL,
                file_type TEXT NOT NULL DEFAULT 'file'
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_turn_files_turn_id ON turn_files(turn_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_turn_files_file_path ON turn_files(file_path)")

        # Migrate data from legacy conversation_store if it exists
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversation_store'")
        if cursor.fetchone() is not None:
            self._migrate_legacy_data(conn)

        self._set_schema_version(conn, 1, "Normalized tables (threads, turns, turn_files)")
        conn.commit()
        logger.info("Migration v0 → v1 complete")

    def _migrate_legacy_data(self, conn: sqlite3.Connection) -> None:
        """Transform rows from conversation_store into normalized tables."""
        cursor = conn.execute("SELECT key, value FROM conversation_store")
        migrated = 0
        skipped = 0

        for row in cursor.fetchall():
            key, value = row
            if not key.startswith("thread:"):
                skipped += 1
                continue
            try:
                ctx = json.loads(value)
                self._insert_thread_from_dict(conn, ctx)
                migrated += 1
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                logger.warning(f"Skipping malformed row {key}: {e}")
                skipped += 1

        # Rename old table to backup
        conn.execute("ALTER TABLE conversation_store RENAME TO conversation_store_backup")
        logger.info(f"Migrated {migrated} threads from legacy table ({skipped} skipped). Old table backed up.")

    def _insert_thread_from_dict(self, conn: sqlite3.Connection, ctx: dict) -> None:
        """Insert a ThreadContext dict into normalized tables."""
        thread_id = ctx["thread_id"]
        conn.execute(
            """INSERT OR IGNORE INTO threads
               (thread_id, parent_thread_id, tool_name, initial_context, status, created_at, last_updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                thread_id,
                ctx.get("parent_thread_id"),
                ctx.get("tool_name", "unknown"),
                json.dumps(ctx.get("initial_context", {}), ensure_ascii=False),
                STATUS_INACTIVE,
                ctx.get("created_at", ""),
                ctx.get("last_updated_at", ""),
            ),
        )
        for i, turn in enumerate(ctx.get("turns", [])):
            metadata = turn.get("model_metadata")
            metadata_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
            cursor = conn.execute(
                """INSERT OR IGNORE INTO turns
                   (thread_id, turn_index, role, content, tool_name, model_provider, model_name, model_metadata, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    thread_id,
                    i,
                    turn.get("role", "user"),
                    turn.get("content", ""),
                    turn.get("tool_name"),
                    turn.get("model_provider"),
                    turn.get("model_name"),
                    metadata_json,
                    turn.get("timestamp", ""),
                ),
            )
            turn_id = cursor.lastrowid
            if turn_id:
                for fp in turn.get("files") or []:
                    conn.execute(
                        "INSERT INTO turn_files (turn_id, file_path, file_type) VALUES (?, ?, 'file')",
                        (turn_id, fp),
                    )
                for ip in turn.get("images") or []:
                    conn.execute(
                        "INSERT INTO turn_files (turn_id, file_path, file_type) VALUES (?, ?, 'image')",
                        (turn_id, ip),
                    )

    def _migrate_to_v2(self, conn: sqlite3.Connection) -> None:
        """Create FTS5 virtual table for content search (optional)."""
        logger.info("Running migration v1 → v2: Creating FTS5 index")
        try:
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS turns_fts USING fts5(
                    content,
                    content=turns,
                    content_rowid=id
                )
            """)
            # Triggers to keep FTS in sync
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS turns_fts_ai AFTER INSERT ON turns BEGIN
                    INSERT INTO turns_fts(rowid, content) VALUES (new.id, new.content);
                END
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS turns_fts_ad AFTER DELETE ON turns BEGIN
                    INSERT INTO turns_fts(turns_fts, rowid, content) VALUES('delete', old.id, old.content);
                END
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS turns_fts_au AFTER UPDATE ON turns BEGIN
                    INSERT INTO turns_fts(turns_fts, rowid, content) VALUES('delete', old.id, old.content);
                    INSERT INTO turns_fts(rowid, content) VALUES (new.id, new.content);
                END
            """)
            # Populate FTS from existing turns
            conn.execute("INSERT INTO turns_fts(rowid, content) SELECT id, content FROM turns")
            logger.info("FTS5 index created successfully")
        except Exception as e:
            logger.warning(f"FTS5 not available, content search will use LIKE fallback: {e}")

        # Ensure schema_version table exists before writing
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at REAL NOT NULL,
                description TEXT
            )
        """)
        self._set_schema_version(conn, 2, "FTS5 content search index")
        conn.commit()

    def _check_fts_exists(self) -> bool:
        """Check if FTS5 virtual table exists."""
        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='turns_fts'")
            return cursor.fetchone() is not None
        except Exception:
            return False

    def save_thread(self, ctx: dict) -> None:
        """Insert or update a thread row."""
        with self._write_lock:
            conn = self._get_connection()
            conn.execute(
                """INSERT INTO threads
                   (thread_id, parent_thread_id, tool_name, initial_context, status, created_at, last_updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(thread_id) DO UPDATE SET
                       initial_context=excluded.initial_context,
                       status=excluded.status,
                       last_updated_at=excluded.last_updated_at""",
                (
                    ctx["thread_id"],
                    ctx.get("parent_thread_id"),
                    ctx.get("tool_name", "unknown"),
                    json.dumps(ctx.get("initial_context", {}), ensure_ascii=False),
                    ctx.get("status", STATUS_ACTIVE),
                    ctx.get("created_at", ""),
                    ctx.get("last_updated_at", ""),
                ),
            )
            conn.commit()
        logger.debug(f"Saved thread {ctx['thread_id']}")

    def load_thread(self, thread_id: str) -> Optional[str]:
        """Load a thread and all its turns, return JSON matching ThreadContext shape.

        Uses 3 queries total (thread + turns + all files) instead of N+1.
        """
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT thread_id, parent_thread_id, tool_name, initial_context, status, created_at, last_updated_at "
            "FROM threads WHERE thread_id = ?",
            (thread_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None

        ctx = {
            "thread_id": row[0],
            "parent_thread_id": row[1],
            "created_at": row[5],
            "last_updated_at": row[6],
            "tool_name": row[2],
            "turns": [],
            "initial_context": json.loads(row[3]) if row[3] else {},
        }

        turn_rows = conn.execute(
            "SELECT id, turn_index, role, content, tool_name, model_provider, model_name, model_metadata, timestamp "
            "FROM turns WHERE thread_id = ? ORDER BY turn_index",
            (thread_id,),
        ).fetchall()

        # Batch-load all files for all turns in one query
        turn_ids = [trow[0] for trow in turn_rows]
        files_by_turn: dict[int, tuple[list[str], list[str]]] = {}
        if turn_ids:
            placeholders = ",".join("?" * len(turn_ids))
            file_rows = conn.execute(
                f"SELECT turn_id, file_path, file_type FROM turn_files WHERE turn_id IN ({placeholders})",
                turn_ids,
            ).fetchall()
            for frow in file_rows:
                tid = frow[0]
                if tid not in files_by_turn:
                    files_by_turn[tid] = ([], [])
                if frow[2] == "image":
                    files_by_turn[tid][1].append(frow[1])
                else:
                    files_by_turn[tid][0].append(frow[1])

        for trow in turn_rows:
            turn_id = trow[0]
            metadata = json.loads(trow[7]) if trow[7] else None
            files, images = files_by_turn.get(turn_id, ([], []))
            ctx["turns"].append(
                {
                    "role": trow[2],
                    "content": trow[3],
                    "timestamp": trow[8],
                    "files": files if files else None,
                    "images": images if images else None,
                    "tool_name": trow[4],
                    "model_provider": trow[5],
                    "model_name": trow[6],
                    "model_metadata": metadata,
                }
            )

        return json.dumps(ctx, ensure_ascii=False)

    def append_turn(self, thread_id: str, turn: dict, turn_index: int) -> int:
        """Insert a single turn row and its file references. Returns the turn row id."""
        metadata = turn.get("model_metadata")
        metadata_json = json.dumps(metadata, ensure_ascii=False) if metadata else None

        with self._write_lock:
            conn = self._get_connection()
            cursor = conn.execute(
                """INSERT INTO turns
                   (thread_id, turn_index, role, content, tool_name, model_provider, model_name, model_metadata, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    thread_id,
                    turn_index,
                    turn.get("role", "user"),
                    turn.get("content", ""),
                    turn.get("tool_name"),
                    turn.get("model_provider"),
                    turn.get("model_name"),
                    metadata_json,
                    turn.get("timestamp", ""),
                ),
            )
            turn_id = cursor.lastrowid

            # Insert file references
            for fp in turn.get("files") or []:
                conn.execute(
                    "INSERT INTO turn_files (turn_id, file_path, file_type) VALUES (?, ?, 'file')",
                    (turn_id, fp),
                )
            for ip in turn.get("images") or []:
                conn.execute(
                    "INSERT INTO turn_files (turn_id, file_path, file_type) VALUES (?, ?, 'image')",
                    (turn_id, ip),
                )

            conn.commit()

        logger.debug(f"Appended turn {turn_index} to thread {thread_id}")
        return turn_id

    def update_thread_timestamp(self, thread_id: str, timestamp: str) -> None:
        """Update a thread's last_updated_at and set status to active."""
        with self._write_lock:
            conn = self._get_connection()
            conn.execute(
                f"UPDATE threads SET last_updated_at = ?, status = '{STATUS_ACTIVE}' WHERE thread_id = ?",
                (timestamp, thread_id),
            )
            conn.commit()

    def get_turn_count(self, thread_id: str) -> int:
        """Get the number of turns in a thread."""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE thread_id = ?",
            (thread_id,),
        )
        row = cursor.fetchone()
        return row[0] if row else 0

    def list_threads(
        self,
        tool_name: Optional[str] = None,
        model_name: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        """Query threads with optional filters. Returns (summaries, total_count)."""
        conn = self._get_connection()

        where_clauses = []
        params: list[Any] = []

        if tool_name:
            where_clauses.append("t.tool_name = ?")
            params.append(tool_name)
        if after:
            where_clauses.append("t.created_at >= ?")
            params.append(after)
        if before:
            where_clauses.append("t.created_at <= ?")
            params.append(before)
        if model_name:
            where_clauses.append("t.thread_id IN (SELECT DISTINCT thread_id FROM turns WHERE model_name = ?)")
            params.append(model_name)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Get total count
        count_cursor = conn.execute(f"SELECT COUNT(*) FROM threads t WHERE {where_sql}", params)
        total_count = count_cursor.fetchone()[0]

        # Get page of results
        query_params = params + [limit, offset]
        cursor = conn.execute(
            f"""SELECT t.thread_id, t.tool_name, t.initial_context, t.status,
                       t.created_at, t.last_updated_at,
                       (SELECT COUNT(*) FROM turns WHERE thread_id = t.thread_id) as turn_count
                FROM threads t
                WHERE {where_sql}
                ORDER BY t.last_updated_at DESC
                LIMIT ? OFFSET ?""",
            query_params,
        )

        rows = cursor.fetchall()
        thread_ids = [row[0] for row in rows]

        # Batch-load models_used for all threads in one query
        models_by_thread: dict[str, list[str]] = {}
        if thread_ids:
            placeholders = ",".join("?" * len(thread_ids))
            model_rows = conn.execute(
                f"SELECT thread_id, model_name FROM turns WHERE thread_id IN ({placeholders}) "
                f"AND model_name IS NOT NULL GROUP BY thread_id, model_name",
                thread_ids,
            ).fetchall()
            for mrow in model_rows:
                models_by_thread.setdefault(mrow[0], []).append(mrow[1])

        results = []
        for row in rows:
            initial_ctx = json.loads(row[2]) if row[2] else {}
            preview = initial_ctx.get("step", "")
            if len(preview) > 200:
                preview = preview[:200] + "..."

            results.append(
                {
                    "thread_id": row[0],
                    "tool_name": row[1],
                    "preview": preview,
                    "models_used": models_by_thread.get(row[0], []),
                    "turn_count": row[6],
                    "created_at": row[4],
                    "last_updated_at": row[5],
                    "status": row[3],
                }
            )

        return results, total_count

    def search_turns(
        self,
        query: str,
        model_name: Optional[str] = None,
        model_provider: Optional[str] = None,
        tool_name: Optional[str] = None,
        limit: int = 20,
    ) -> list[dict]:
        """Search turn content using FTS5 or LIKE fallback."""
        conn = self._get_connection()
        use_fts = self._has_fts

        if use_fts:
            where_clauses = ["turns_fts MATCH ?"]
            params: list[Any] = [query]
            from_clause = "FROM turns_fts JOIN turns tu ON turns_fts.rowid = tu.id"
            order_clause = "ORDER BY rank"
        else:
            where_clauses = ["tu.content LIKE ?"]
            params = [f"%{query}%"]
            from_clause = "FROM turns tu"
            order_clause = "ORDER BY tu.timestamp DESC"

        if model_name:
            where_clauses.append("tu.model_name = ?")
            params.append(model_name)
        if model_provider:
            where_clauses.append("tu.model_provider = ?")
            params.append(model_provider)
        if tool_name:
            where_clauses.append("t.tool_name = ?")
            params.append(tool_name)

        where_sql = " AND ".join(where_clauses)
        params.append(limit)

        cursor = conn.execute(
            f"""SELECT tu.thread_id, tu.turn_index, tu.role, tu.model_name,
                       tu.model_provider, tu.timestamp, tu.content,
                       t.tool_name, t.initial_context
                {from_clause}
                JOIN threads t ON tu.thread_id = t.thread_id
                WHERE {where_sql}
                {order_clause}
                LIMIT ?""",
            params,
        )
        return self._format_search_results(cursor)

    def _format_search_results(self, cursor) -> list[dict]:
        """Format search cursor results into dicts."""
        results = []
        for row in cursor.fetchall():
            initial_ctx = json.loads(row[8]) if row[8] else {}
            content = row[6]
            snippet = content[:300] + "..." if len(content) > 300 else content
            results.append(
                {
                    "thread_id": row[0],
                    "turn_index": row[1],
                    "role": row[2],
                    "model_name": row[3],
                    "model_provider": row[4],
                    "timestamp": row[5],
                    "snippet": snippet,
                    "thread_tool_name": row[7],
                    "thread_preview": initial_ctx.get("step", "")[:200],
                }
            )
        return results

    def find_threads_by_file(self, file_path: str) -> list[dict]:
        """Find threads that reference a specific file."""
        conn = self._get_connection()
        cursor = conn.execute(
            """SELECT DISTINCT t.thread_id, t.tool_name, t.initial_context,
                      t.status, t.created_at, t.last_updated_at
               FROM turn_files tf
               JOIN turns tu ON tf.turn_id = tu.id
               JOIN threads t ON tu.thread_id = t.thread_id
               WHERE tf.file_path = ?
               ORDER BY t.last_updated_at DESC""",
            (file_path,),
        )
        results = []
        for row in cursor.fetchall():
            initial_ctx = json.loads(row[2]) if row[2] else {}
            results.append(
                {
                    "thread_id": row[0],
                    "tool_name": row[1],
                    "preview": initial_ctx.get("step", "")[:200],
                    "status": row[3],
                    "created_at": row[4],
                    "last_updated_at": row[5],
                }
            )
        return results

    def get(self, key: str) -> Optional[str]:
        """Retrieve value — routes through normalized load_thread for thread keys."""
        if key.startswith("thread:"):
            thread_id = key[len("thread:") :]
            return self.load_thread(thread_id)
        return None

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        """Store value — routes through normalized save for thread keys."""
        if key.startswith("thread:"):
            try:
                ctx = json.loads(value)
                self.save_thread(ctx)
                turns = ctx.get("turns", [])
                if turns:
                    # Batch-check existing turn indices in one query
                    conn = self._get_connection()
                    existing = {
                        row[0]
                        for row in conn.execute(
                            "SELECT turn_index FROM turns WHERE thread_id = ?",
                            (ctx["thread_id"],),
                        ).fetchall()
                    }
                    for i, turn in enumerate(turns):
                        if i not in existing:
                            self.append_turn(ctx["thread_id"], turn, i)
                return
            except (json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to parse thread JSON for normalized storage: {e}")
                return

    def set_with_ttl(self, key: str, ttl_seconds: int, value: str) -> None:
        """Alias for setex."""
        self.setex(key, ttl_seconds, value)

    def _inactive_worker(self):
        """Background thread that marks threads as inactive after idle period."""
        while not self._shutdown_flag:
            time.sleep(1800)  # 30 minutes
            self._mark_inactive_threads()

    def _mark_inactive_threads(self):
        """Mark threads inactive if not updated in the last 3 hours. Informational only."""
        try:
            timeout_hours = int(get_env("CONVERSATION_TIMEOUT_HOURS", "3") or "3")
            from datetime import datetime, timedelta, timezone

            cutoff = (datetime.now(timezone.utc) - timedelta(hours=timeout_hours)).isoformat()
            with self._write_lock:
                conn = self._get_connection()
                cursor = conn.execute(
                    f"UPDATE threads SET status = '{STATUS_INACTIVE}' WHERE status = '{STATUS_ACTIVE}' AND last_updated_at < ?",
                    (cutoff,),
                )
                marked = cursor.rowcount
                conn.commit()
            if marked:
                logger.debug(f"Marked {marked} threads as inactive")
        except Exception as e:
            logger.warning(f"Inactive marking error: {e}")

    def shutdown(self):
        """Graceful shutdown of background thread and all connections."""
        self._shutdown_flag = True
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=1)
        # Close all thread-local connections
        with self._conn_lock:
            for conn in self._connections:
                try:
                    conn.close()
                except Exception:
                    pass
            self._connections.clear()
        if hasattr(self._local, "conn"):
            self._local.conn = None


# Global singleton instance
_storage_instance = None
_storage_lock = threading.Lock()


def get_storage_backend():
    """Get the global storage instance (singleton pattern).

    Defaults to SQLite. Set PAL_STORAGE_BACKEND=memory for in-memory storage.
    """
    global _storage_instance
    if _storage_instance is None:
        with _storage_lock:
            if _storage_instance is None:
                backend = (get_env("PAL_STORAGE_BACKEND", "sqlite") or "sqlite").lower()
                if backend == "memory":
                    _storage_instance = InMemoryStorage()
                    logger.info("Initialized in-memory conversation storage")
                else:
                    if backend != "sqlite":
                        logger.warning(f"Unknown PAL_STORAGE_BACKEND '{backend}', defaulting to sqlite")
                    _storage_instance = SQLiteStorage()
                    logger.info("Initialized SQLite conversation storage")
    return _storage_instance
