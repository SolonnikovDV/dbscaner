"""
Конфигурация подключения к базе данных и другие настройки
"""

# Database configuration for PostgreSQL (Test Environment)
# IMPORTANT: Replace these values with your actual test database credentials
DB_CONFIG = {
    'host': 'your_host',          # Database host
    'port': '5432',               # Database port
    'database': 'your_db',        # Database name
    'user': 'your_user',          # Database user
    'password': 'your_pass'       # Database password
}

# Schema for test objects
TEST_SCHEMA = 'test_graph'

# SQL scripts location
SQL_SCRIPTS_DIR = 'sql/test_objects'