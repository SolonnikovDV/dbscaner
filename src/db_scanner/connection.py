"""Database connection handling module."""
from typing import Dict, Optional
import psycopg2
from psycopg2.extensions import connection
import configparser


class DatabaseConnection:
    """Handles database connections with context manager support."""
    
    def __init__(self, config_path: str):
        """Initialize connection parameters from config file.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config = self._load_config(config_path)
        self._conn: Optional[connection] = None

    @staticmethod
    def _load_config(config_path: str) -> Dict[str, str]:
        """Load database configuration from file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Dictionary with database connection parameters
        """
        parser = configparser.ConfigParser()
        parser.read(config_path)
        
        return {
            'dbname': parser['database']['dbname'],
            'user': parser['database']['user'],
            'password': parser['database']['password'],
            'host': parser['database']['host'],
            'port': parser['database']['port']
        }

    def connect(self) -> connection:
        """Establish database connection.
        
        Returns:
            Active database connection
        """
        if not self._conn or self._conn.closed:
            self._conn = psycopg2.connect(**self.config)
        return self._conn

    def close(self):
        """Close the database connection if it exists."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> connection:
        """Context manager entry.
        
        Returns:
            Active database connection
        """
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()