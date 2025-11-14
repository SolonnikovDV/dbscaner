"""Database connection module."""
import psycopg2
from psycopg2.extensions import connection
from src.config import DB_CONFIG


class DBConnection:
    def __init__(self):
        self._conn = None
        self._connect()
        
    def _connect(self):
        if not self._conn or self._conn.closed:
            self._conn = psycopg2.connect(**DB_CONFIG)
        
    def cursor(self):
        self._connect()
        return self._conn.cursor()
        
    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
