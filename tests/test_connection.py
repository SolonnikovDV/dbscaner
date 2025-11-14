"""Test database connection"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from src.config_test import DB_CONFIG

def test_connection():
    """Test database connection with current configuration"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print("Successfully connected to PostgreSQL:")
            print(version)

            # Проверим наличие схемы test_graph
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1
                    FROM information_schema.schemata
                    WHERE schema_name = 'test_graph'
                );
            """)
            schema_exists = cur.fetchone()[0]
            print("\nSchema 'test_graph' exists:", schema_exists)

            if schema_exists:
                # Проверим существующие объекты в схеме
                cur.execute("""
                    SELECT
                        table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = 'test_graph'
                    UNION ALL
                    SELECT
                        routine_name, routine_type
                    FROM information_schema.routines
                    WHERE routine_schema = 'test_graph';
                """)
                print("\nExisting objects in test_graph schema:")
                for obj in cur.fetchall():
                    print(f"- {obj[0]} ({obj[1]})")
        conn.close()
    except Exception as e:
        print("Error connecting to database:")
        print(str(e))

if __name__ == "__main__":
    test_connection()