"""Database connection module."""
from typing import Dict, Optional

import psycopg2
import psycopg2.pool


class _PooledCursorContext:
    """Context manager that borrows a connection from the pool, yields a cursor, returns it on exit."""

    def __init__(self, pool: psycopg2.pool.ThreadedConnectionPool):
        self._pool = pool
        self._conn = None
        self._cur = None

    def __enter__(self):
        self._conn = self._pool.getconn()
        self._cur = self._conn.cursor()
        return self._cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cur.close()
        if exc_type:
            try:
                self._conn.rollback()
            except Exception:
                pass
        self._pool.putconn(self._conn)
        return False


class DBConnection:
    def __init__(self, config: Optional[Dict[str, str]] = None,
                 min_conn: int = 1, max_conn: int = 10):
        if config is None:
            from src.config import DB_CONFIG
            config = DB_CONFIG
        self._config = dict(config)
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            min_conn, max_conn, **self._config
        )

    @property
    def config(self) -> Dict[str, str]:
        return dict(self._config)

    def cursor(self) -> _PooledCursorContext:
        return _PooledCursorContext(self._pool)

    def close(self):
        if self._pool:
            self._pool.closeall()
            self._pool = None

    def test(self) -> None:
        """Raise on connection failure."""
        conn = psycopg2.connect(**self._config)
        conn.close()
