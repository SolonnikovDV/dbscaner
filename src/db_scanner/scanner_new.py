"""Database object scanner module."""
from typing import List, Dict, Set, Optional
import re
from psycopg2.extensions import connection

from .models import DBObject, Relationship, ObjectType


class DBScanner:
    """Scanner for database objects and their relationships."""

    def __init__(self, conn: connection):
        """Initialize scanner with database connection."""
        self.conn = conn

    def get_object_definition(self, obj: DBObject) -> str:
        """Get the SQL definition of a database object."""
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
                        SELECT 
                            'CREATE TABLE ' || n.nspname || '.' || c.relname || ' (' ||
                            string_agg(
                                a.attname || ' ' || 
                                pg_catalog.format_type(a.atttypid, a.atttypmod) ||
                                CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END,
                                ', '
                            ) || ' );'
                        FROM pg_class c
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        JOIN pg_attribute a ON c.oid = a.attrelid
                        WHERE n.nspname = %s 
                            AND c.relname = %s 
                            AND a.attnum > 0
                            AND NOT a.attisdropped
                        GROUP BY n.nspname, c.relname
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
        try:
            with self.conn.cursor() as cur:
                # Зависимости (прямые и косвенные) для объектов
                if obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                    # Рекурсивный поиск зависимостей для представлений
                    cur.execute("""
                        WITH RECURSIVE dependency_chain AS (
                            -- Базовый случай: прямые зависимости
                            SELECT DISTINCT
                                n.nspname,
                                c.relname,
                                CASE c.relkind
                                    WHEN 'r' THEN 'TABLE'
                                    WHEN 'v' THEN 'VIEW'
                                    WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                    ELSE c.relkind::text
                                END as obj_type,
                                'depends_on' as rel_type,
                                1 as depth,
                                ARRAY[c.oid] as dependency_path
                            FROM pg_rewrite rw
                            JOIN pg_class src ON rw.ev_class = src.oid
                            JOIN pg_depend d ON d.objid = rw.oid
                            JOIN pg_class c ON d.refobjid = c.oid
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            JOIN pg_namespace srcn ON src.relnamespace = srcn.oid
                            WHERE srcn.nspname = %s
                            AND src.relname = %s
                            AND d.deptype = 'n'
                            AND c.relkind IN ('r', 'v', 'm')

                            UNION ALL

                            -- Рекурсивный случай: зависимости зависимостей
                            SELECT DISTINCT
                                n2.nspname,
                                c2.relname,
                                CASE c2.relkind
                                    WHEN 'r' THEN 'TABLE'
                                    WHEN 'v' THEN 'VIEW'
                                    WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                    ELSE c2.relkind::text
                                END,
                                'depends_on',
                                dc.depth + 1,
                                dc.dependency_path || c2.oid
                            FROM dependency_chain dc
                            JOIN pg_rewrite rw2 ON rw2.ev_class = (
                                SELECT oid FROM pg_class 
                                WHERE relname = dc.relname 
                                AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = dc.nspname)
                            )
                            JOIN pg_depend d2 ON d2.objid = rw2.oid
                            JOIN pg_class c2 ON d2.refobjid = c2.oid
                            JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
                            WHERE d2.deptype = 'n'
                            AND c2.relkind IN ('r', 'v', 'm')
                            AND NOT c2.oid = ANY(dc.dependency_path)  -- Предотвращение циклов
                            AND dc.depth < 5  -- Ограничение глубины поиска
                        )
                        SELECT 
                            nspname,
                            relname,
                            obj_type,
                            rel_type,
                            depth
                        FROM dependency_chain
                        ORDER BY depth;
                    """, (obj.schema, obj.name))
                    deps = cur.fetchall()
                    for row in deps:
                        dep_schema, dep_name, dep_type, rel_type = row
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
                                    depth=1
                                )
                            )
                        except KeyError:
                            print(f"Пропуск объекта {dep_schema}.{dep_name} с неизвестным типом: {dep_type}")

                elif obj.obj_type == ObjectType.FUNCTION:
                    # Для функций анализируем тело функции
                    cur.execute("""
                        WITH func_objects AS (
                            SELECT DISTINCT
                                regexp_matches(
                                    pg_get_functiondef(p.oid),
                                    'FROM\\s+([a-zA-Z_][a-zA-Z0-9_]*\\.([a-zA-Z_][a-zA-Z0-9_]*))',
                                    'g'
                                ) as refs
                            FROM pg_proc p
                            JOIN pg_namespace n ON p.pronamespace = n.oid
                            WHERE n.nspname = %s AND p.proname = %s
                        )
                        SELECT DISTINCT
                            split_part(refs[1], '.', 1) as schema_name,
                            split_part(refs[1], '.', 2) as object_name,
                            CASE c.relkind
                                WHEN 'r' THEN 'TABLE'
                                WHEN 'v' THEN 'VIEW'
                                WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                ELSE c.relkind::text
                            END,
                            'depends_on'
                        FROM func_objects
                        JOIN pg_class c ON c.relname = split_part(refs[1], '.', 2)
                        JOIN pg_namespace n ON n.nspname = split_part(refs[1], '.', 1)
                            AND c.relnamespace = n.oid
                        WHERE c.relkind IN ('r', 'v', 'm')
                    """, (obj.schema, obj.name))
                    deps = cur.fetchall()
                    for row in deps:
                        dep_schema, dep_name, dep_type, rel_type = row
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
                                    depth=1
                                )
                            )
                        except KeyError:
                            print(f"Пропуск объекта {dep_schema}.{dep_name} с неизвестным типом: {dep_type}")

                # Поиск обратных зависимостей для всех типов объектов
                # Представления, использующие объект
                cur.execute("""
                    WITH RECURSIVE view_deps AS (
                        SELECT DISTINCT
                            n.nspname as dep_schema,
                            c.relname as dep_name,
                            c.relkind,
                            'used_by' as rel_type
                        FROM pg_class base
                        JOIN pg_namespace basen ON base.relnamespace = basen.oid
                        JOIN pg_depend d ON d.refobjid = base.oid
                        JOIN pg_rewrite rw ON rw.oid = d.objid
                        JOIN pg_class c ON rw.ev_class = c.oid
                        JOIN pg_namespace n ON c.relnamespace = n.oid
                        WHERE basen.nspname = %s
                        AND base.relname = %s
                        AND c.relkind IN ('v', 'm')
                    )
                    SELECT
                        dep_schema,
                        dep_name,
                        CASE relkind
                            WHEN 'r' THEN 'TABLE'
                            WHEN 'v' THEN 'VIEW'
                            WHEN 'm' THEN 'MATERIALIZED_VIEW'
                            ELSE relkind::text
                        END,
                        rel_type
                    FROM view_deps
                """, (obj.schema, obj.name))
                deps = cur.fetchall()
                for row in deps:
                    dep_schema, dep_name, dep_type, rel_type = row
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
                                depth=1
                            )
                        )
                    except KeyError:
                        print(f"Пропуск объекта {dep_schema}.{dep_name} с неизвестным типом: {dep_type}")

                # Функции, использующие объект
                cur.execute("""
                    SELECT DISTINCT
                        n.nspname,
                        p.proname,
                        'FUNCTION',
                        'used_by'
                    FROM pg_class base
                    JOIN pg_namespace basen ON base.relnamespace = basen.oid
                    JOIN pg_depend d ON d.refobjid = base.oid
                    JOIN pg_proc p ON d.objid = p.oid
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE basen.nspname = %s
                    AND base.relname = %s
                    AND d.deptype = 'n'
                """, (obj.schema, obj.name))
                deps = cur.fetchall()
                for row in deps:
                    dep_schema, dep_name, dep_type, rel_type = row
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
                                depth=1
                            )
                        )
                    except KeyError:
                        print(f"Пропуск объекта {dep_schema}.{dep_name} с неизвестным типом: {dep_type}")

        except Exception as e:
            print(f"Ошибка при поиске зависимостей: {e}")

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