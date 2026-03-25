"""
Storage backends for conversation threads

This module provides thread-safe storage backends for conversation contexts:

1. InMemoryStorage - Ephemeral, process-specific storage (data lost on restart)
2. SQLiteStorage - Persistent storage using SQLite with WAL mode (survives restarts)

The default backend is SQLite. Override via PAL_STORAGE_BACKEND env var ("memory" or "sqlite").

Key Features:
- Thread-safe operations using locks
- TTL support with automatic expiration
- Background cleanup thread for memory management
- Singleton pattern for consistent state within a single process
- SQLite WAL mode for concurrent read access
"""

import logging
import os
import sqlite3
import threading
import time
from typing import Optional

from utils.env import get_env

logger = logging.getLogger(__name__)


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

    Uses WAL journal mode for concurrent read access and busy_timeout
    to handle write contention gracefully.
    """

    def __init__(self, db_path=None):
        if db_path is None:
            db_path = get_env("PAL_DB_PATH") or os.path.join(
                os.path.expanduser("~"), ".pal-mcp", "conversations.db"
            )
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._shutdown_flag = False
        self._get_count = 0

        # Initialize the database schema
        conn = self._get_connection()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation_store (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                expires_at REAL NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_expires_at ON conversation_store(expires_at)"
        )
        conn.commit()

        # Start background cleanup thread (every 30 min)
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_worker, daemon=True
        )
        self._cleanup_thread.start()

        logger.info(f"SQLite storage initialized at {db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get a thread-local SQLite connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self._db_path, check_same_thread=False
            )
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA busy_timeout=5000")
        return self._local.conn

    def get(self, key: str) -> Optional[str]:
        """Retrieve value if not expired."""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT value FROM conversation_store WHERE key=? AND expires_at > ?",
            (key, time.time()),
        )
        row = cursor.fetchone()

        # Lazy cleanup: every 100th call
        self._get_count += 1
        if self._get_count % 100 == 0:
            self._cleanup_expired()

        if row:
            logger.debug(f"Retrieved key {key}")
            return row[0]
        return None

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        """Store value with expiration time."""
        now = time.time()
        with self._write_lock:
            conn = self._get_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO conversation_store
                    (key, value, expires_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, value, now + ttl_seconds, now, now),
            )
            conn.commit()
        logger.debug(f"Stored key {key} with TTL {ttl_seconds}s")

    def set_with_ttl(self, key: str, ttl_seconds: int, value: str) -> None:
        """Alias for setex."""
        self.setex(key, ttl_seconds, value)

    def _cleanup_worker(self):
        """Background thread that periodically cleans up expired entries."""
        while not self._shutdown_flag:
            time.sleep(1800)  # 30 minutes
            self._cleanup_expired()

    def _cleanup_expired(self):
        """Remove all expired entries."""
        try:
            with self._write_lock:
                conn = self._get_connection()
                cursor = conn.execute(
                    "DELETE FROM conversation_store WHERE expires_at < ?",
                    (time.time(),),
                )
                deleted = cursor.rowcount
                conn.commit()
            if deleted:
                logger.debug(f"Cleaned up {deleted} expired conversation threads")
        except Exception as e:
            logger.warning(f"Cleanup error: {e}")

    def shutdown(self):
        """Graceful shutdown of background thread and connections."""
        self._shutdown_flag = True
        if self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=1)
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
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
                backend = (
                    get_env("PAL_STORAGE_BACKEND", "sqlite") or "sqlite"
                ).lower()
                if backend == "memory":
                    _storage_instance = InMemoryStorage()
                    logger.info("Initialized in-memory conversation storage")
                else:
                    _storage_instance = SQLiteStorage()
                    logger.info("Initialized SQLite conversation storage")
    return _storage_instance
