"""Database object scanner module."""
from typing import List, Dict, Set, Optional
import re
from psycopg2.extensions import connection

from .models import DBObject, Relationship, ObjectType


class DBScanner:
    """Scanner for database objects and their relationships."""

    def __init__(self, conn: connection):
        """Initialize scanner with database connection.
        
        Args:
            conn: Active database connection
        """
        self.conn = conn

    def get_object_definition(self, obj: DBObject) -> str:
        """Get the SQL definition of a database object.
        
        Args:
            obj: Database object to get definition for
            
        Returns:
            SQL definition of the object
        """
        with self.conn.cursor() as cur:
            try:
                if obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                    if obj.obj_type == ObjectType.VIEW:
                        cur.execute("""
                            SELECT view_definition
                            FROM information_schema.views
                            WHERE table_schema = %s
                                AND table_name = %s
                        """, (obj.schema, obj.name))
                    else:
                        cur.execute("""
                            SELECT pg_get_viewdef(c.oid)
                            FROM pg_class c
                            JOIN pg_namespace n ON c.relnamespace = n.oid
                            WHERE n.nspname = %s AND c.relname = %s
                        """, (obj.schema, obj.name))
                elif obj.obj_type == ObjectType.FUNCTION:
                    cur.execute("""
                        SELECT pg_get_functiondef(p.oid)
                        FROM pg_proc p
                        JOIN pg_namespace n ON p.pronamespace = n.oid
                        WHERE n.nspname = %s AND p.proname = %s
                    """, (obj.schema, obj.name))
                elif obj.obj_type == ObjectType.TABLE:
                    cur.execute("""
                        SELECT pg_get_tabledef(c.oid)
                        FROM pg_class c
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        WHERE n.nspname = %s AND c.relname = %s
                    """, (obj.schema, obj.name))
                result = cur.fetchone()
                if result is None:
                    return f"-- Определение не найдено для объекта {obj.schema}.{obj.name} типа {obj.obj_type}"
                return result[0]
            except Exception as e:
                return f"-- Ошибка при получении определения: {str(e)}\n-- Для объекта: {obj.schema}.{obj.name}"

    def find_related_objects(self, obj: DBObject) -> List[Relationship]:
        """Find objects related to the given database object."""
        relationships = []
        with self.conn.cursor() as cur:
            # Получаем OID исходного объекта
            if obj.obj_type == ObjectType.FUNCTION:
                cur.execute("""
                    SELECT p.oid
                    FROM pg_proc p
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE n.nspname = %s AND p.proname = %s
                """, (obj.schema, obj.name))
            else:
                cur.execute("""
                    SELECT c.oid
                    FROM pg_class c
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = %s AND c.relname = %s
                """, (obj.schema, obj.name))
            
            result = cur.fetchone()
            if not result:
                return relationships
            
            source_oid = result[0]
            
            # Поиск зависимостей без рекурсии для начала
            query = """
            WITH deps AS (
                -- Зависимости для представлений (используемые объекты)
                SELECT DISTINCT
                    n.nspname as dep_schema,
                    c.relname as dep_name,
                    CASE c.relkind
                        WHEN 'r' THEN 'TABLE'
                        WHEN 'v' THEN 'VIEW'
                        WHEN 'm' THEN 'MATERIALIZED_VIEW'
                        ELSE c.relkind::text
                    END as dep_type,
                    1 as depth,
                    'depends_on' as rel_type
                FROM pg_rewrite rw
                JOIN pg_depend d ON d.objid = rw.oid
                JOIN pg_class src ON rw.ev_class = src.oid
                JOIN pg_class c ON d.refobjid = c.oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE src.oid = %s
                
                UNION
                
                -- Зависимости функций (используемые объекты)
                SELECT DISTINCT
                    n.nspname as dep_schema,
                    c.relname as dep_name,
                    CASE c.relkind
                        WHEN 'r' THEN 'TABLE'
                        WHEN 'v' THEN 'VIEW'
                        WHEN 'm' THEN 'MATERIALIZED_VIEW'
                        ELSE c.relkind::text
                    END as dep_type,
                    1 as depth,
                    'depends_on' as rel_type
                FROM pg_depend d
                JOIN pg_class c ON d.refobjid = c.oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE d.objid = %s
                AND d.deptype = 'n'
                
                UNION
                
                -- Обратные зависимости (кто использует данный объект)
                SELECT DISTINCT
                    n.nspname as dep_schema,
                    CASE
                        WHEN p.proname IS NOT NULL THEN p.proname
                        ELSE c.relname
                    END as dep_name,
                    CASE
                        WHEN p.proname IS NOT NULL THEN 'FUNCTION'
                        WHEN c.relkind = 'r' THEN 'TABLE'
                        WHEN c.relkind = 'v' THEN 'VIEW'
                        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
                        ELSE c.relkind::text
                    END as dep_type,
                    1 as depth,
                    'used_by' as rel_type
                FROM pg_depend d
                LEFT JOIN pg_class c ON d.objid = c.oid
                LEFT JOIN pg_proc p ON d.objid = p.oid
                JOIN pg_namespace n ON COALESCE(c.relnamespace, p.pronamespace) = n.oid
                WHERE d.refobjid = %s
                AND d.deptype = 'n'
            )
                -- Объекты, от которых зависит исходный объект
                SELECT DISTINCT
                    n.nspname as dep_schema,
                    CASE
                        WHEN p.proname IS NOT NULL THEN p.proname
                        ELSE c.relname
                    END as dep_name,
                    CASE
                        WHEN p.proname IS NOT NULL THEN 'FUNCTION'
                        WHEN c.relkind = 'r' THEN 'TABLE'
                        WHEN c.relkind = 'v' THEN 'VIEW'
                        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
                        ELSE c.relkind::text
                    END as dep_type,
                    1 as depth,
                    'depends_on' as rel_type
                FROM pg_depend d
                LEFT JOIN pg_class c ON d.refobjid = c.oid
                LEFT JOIN pg_proc p ON d.refobjid = p.oid
                JOIN pg_namespace n ON COALESCE(c.relnamespace, p.pronamespace) = n.oid
                WHERE d.objid = %s
                AND d.deptype = 'n'
                
                UNION ALL
                
                -- Объекты, которые зависят от исходного объекта
                SELECT DISTINCT
                    n.nspname as dep_schema,
                    CASE
                        WHEN p.proname IS NOT NULL THEN p.proname
                        ELSE c.relname
                    END as dep_name,
                    CASE
                        WHEN p.proname IS NOT NULL THEN 'FUNCTION'
                        WHEN c.relkind = 'r' THEN 'TABLE'
                        WHEN c.relkind = 'v' THEN 'VIEW'
                        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
                        ELSE c.relkind::text
                    END as dep_type,
                    1 as depth,
                    'used_by' as rel_type
                FROM pg_depend d
                LEFT JOIN pg_class c ON d.objid = c.oid
                LEFT JOIN pg_proc p ON d.objid = p.oid
                JOIN pg_namespace n ON COALESCE(c.relnamespace, p.pronamespace) = n.oid
                WHERE d.refobjid = %s
                AND d.deptype = 'n'
            )
            SELECT DISTINCT 
                dep_schema,
                dep_name,
                dep_type,
                rel_type,
                depth
            FROM deps
            WHERE dep_schema IS NOT NULL
            AND dep_name IS NOT NULL
            """
            
            cur.execute(query, (source_oid, source_oid, source_oid))
            
            for row in cur.fetchall():
                dep_schema, dep_name, dep_type, rel_type, depth = row
                
                try:
                    target = DBObject(
                        schema=dep_schema,
                        name=dep_name,
                        obj_type=ObjectType[dep_type],
                        definition=""
                    )
                    
                    relationships.append(
                        Relationship(
                            source=obj,
                            target=target,
                            relationship_type=rel_type,
                            depth=depth
                        )
                    )
                except KeyError:
                    print(f"Пропуск объекта {dep_schema}.{dep_name} с неизвестным типом: {dep_type}")
                    continue
                    
        return relationships

    def _find_table_references(self, sql: str) -> Set[str]:
        """Find table references in SQL code."""
        pattern = r'(?:FROM|JOIN|UPDATE|INSERT INTO|DELETE FROM)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)'
        matches = re.finditer(pattern, sql, re.IGNORECASE)
        return {match.group(1) for match in matches}

    def _get_object_by_name(self, full_name: str, obj_type: ObjectType) -> Optional[DBObject]:
        """Get database object by its name and type."""
        parts = full_name.split('.')
        if len(parts) == 2:
            schema, name = parts
        else:
            schema, name = 'public', parts[0]

        with self.conn.cursor() as cur:
            if obj_type == ObjectType.TABLE:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_name = %s
                    )
                """, (schema, name))
                exists = cur.fetchone()[0]
                
                if exists:
                    return DBObject(
                        name=name,
                        schema=schema,
                        obj_type=obj_type,
                        definition=self.get_object_definition(DBObject(
                            name=name,
                            schema=schema,
                            obj_type=obj_type,
                            definition=""
                        ))
                    )
        return None