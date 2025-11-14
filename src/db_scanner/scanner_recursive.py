"""
Database Object Dependency Graph Scanner - Recursive Scanner Module
Copyright (c) 2025 Dmitry Solonnikov
Licensed under MIT License - see LICENSE file for details
"""
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

        # Нисходящие связи: объекты, от которых зависит текущий объект
        relationships.extend(self._find_downstream_dependencies(obj))

        # Восходящие связи: объекты, которые зависят от текущего объекта
        relationships.extend(self._find_upstream_dependencies(obj))

        # Удаляем дубликаты связей и самореференции (объект не может зависеть сам от себя)
        unique_relationships = []
        seen = set()

        for rel in relationships:
            # Пропускаем самореференции
            if rel.source.schema == rel.target.schema and rel.source.name == rel.target.name:
                continue

            key = (rel.source.schema, rel.source.name, rel.target.schema, rel.target.name, rel.relationship_type)
            if key not in seen:
                unique_relationships.append(rel)
                seen.add(key)

        return unique_relationships

    def _find_downstream_dependencies(self, obj: DBObject) -> List[Relationship]:
        """Найти объекты, от которых зависит текущий объект (нисходящие связи)."""
        relationships = []

        if obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW, ObjectType.FUNCTION):
            with self.conn.cursor() as cur:
                try:
                    if obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                        # Для обычных представлений используем pg_rewrite
                        if obj.obj_type == ObjectType.VIEW:
                            cur.execute("""
                                SELECT DISTINCT
                                    n.nspname as dep_schema,
                                    c.relname as dep_name,
                                    CASE c.relkind
                                        WHEN 'r' THEN 'TABLE'
                                        WHEN 'v' THEN 'VIEW'
                                        WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                    END as obj_type,
                                    'depends_on' as rel_type,
                                    1 as depth
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
                            """, (obj.schema, obj.name))

                            view_deps = cur.fetchall()
                            for row in view_deps:
                                try:
                                    dep_schema, dep_name, dep_type, rel_type, depth = row
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
                                except (ValueError, KeyError) as e:
                                    print(f"Ошибка при обработке view зависимости: {e}")

                        # Для материализованных представлений анализируем определение
                        elif obj.obj_type == ObjectType.MATERIALIZED_VIEW:
                            try:
                                cur.execute("SELECT pg_get_viewdef(c.oid, true) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s AND c.relname = %s", (obj.schema, obj.name))
                                mat_view_def_result = cur.fetchone()
                                if mat_view_def_result:
                                    mat_view_sql = mat_view_def_result[0]
                                    # Ищем все ссылки на таблицы и представления в SQL коде
                                    table_refs = self._extract_table_references(mat_view_sql, obj.schema)

                                    for table_ref in table_refs:
                                        table_parts = table_ref.split('.')
                                        if len(table_parts) == 2:
                                            dep_schema, dep_name = table_parts
                                        else:
                                            dep_schema, dep_name = obj.schema, table_parts[0]

                                        # Проверяем тип объекта
                                        cur.execute("""
                                            SELECT CASE c.relkind
                                                WHEN 'r' THEN 'TABLE'
                                                WHEN 'v' THEN 'VIEW'
                                                WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                            END
                                            FROM pg_class c
                                            JOIN pg_namespace n ON c.relnamespace = n.oid
                                            WHERE n.nspname = %s AND c.relname = %s
                                            AND c.relkind IN ('r', 'v', 'm')
                                        """, (dep_schema, dep_name))

                                        type_result = cur.fetchone()
                                        if type_result and type_result[0]:
                                            obj_type_str = type_result[0]
                                            try:
                                                target = DBObject(
                                                    schema=dep_schema,
                                                    name=dep_name,
                                                    obj_type=ObjectType[obj_type_str],
                                                    definition=""
                                                )
                                                relationships.append(
                                                    Relationship(
                                                        source=obj,
                                                        target=target,
                                                        relationship_type='depends_on',
                                                        depth=1
                                                    )
                                                )
                                            except KeyError:
                                                print(f"Неизвестный тип объекта: {obj_type_str}")

                            except Exception as e:
                                print(f"Ошибка при анализе SQL материализованного представления: {e}")

                    elif obj.obj_type == ObjectType.FUNCTION:
                        # Анализируем SQL код функции для поиска всех ссылок на объекты
                        try:
                            cur.execute("SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = %s AND p.proname = %s", (obj.schema, obj.name))
                            func_def_result = cur.fetchone()
                            if func_def_result:
                                func_sql = func_def_result[0]
                                # Ищем все ссылки на объекты в SQL коде
                                table_refs = self._extract_table_references(func_sql, obj.schema)

                                for table_ref in table_refs:
                                    table_parts = table_ref.split('.')
                                    if len(table_parts) == 2:
                                        dep_schema, dep_name = table_parts
                                    else:
                                        dep_schema, dep_name = obj.schema, table_parts[0]

                                    # Проверяем тип объекта (включая представления)
                                    cur.execute("""
                                        SELECT CASE c.relkind
                                            WHEN 'r' THEN 'TABLE'
                                            WHEN 'v' THEN 'VIEW'
                                            WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                        END
                                        FROM pg_class c
                                        JOIN pg_namespace n ON c.relnamespace = n.oid
                                        WHERE n.nspname = %s AND c.relname = %s
                                        AND c.relkind IN ('r', 'v', 'm')
                                    """, (dep_schema, dep_name))

                                    type_result = cur.fetchone()
                                    if type_result and type_result[0]:
                                        obj_type_str = type_result[0]
                                        try:
                                            target = DBObject(
                                                schema=dep_schema,
                                                name=dep_name,
                                                obj_type=ObjectType[obj_type_str],
                                                definition=""
                                            )
                                            relationships.append(
                                                Relationship(
                                                    source=obj,
                                                    target=target,
                                                    relationship_type='depends_on',
                                                    depth=1
                                                )
                                            )
                                        except KeyError:
                                            print(f"Неизвестный тип объекта: {obj_type_str}")

                        except Exception as e:
                            print(f"Ошибка при анализе SQL функции: {e}")

                except Exception as e:
                    print(f"Ошибка при поиске нисходящих зависимостей: {e}")

        return relationships

    def _extract_table_references(self, sql: str, default_schema: str) -> set:
        """Извлечь все ссылки на таблицы из SQL кода."""
        import re

        # Паттерны для поиска ссылок на таблицы
        patterns = [
            r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
            r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
            r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
            r'\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
            r'\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
            r'\bSELECT\s+.*?\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        ]

        tables = set()
        for pattern in patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE | re.MULTILINE | re.DOTALL)
            for match in matches:
                table_ref = match.group(1).strip()
                if table_ref:
                    # Добавляем schema если отсутствует
                    if '.' not in table_ref:
                        table_ref = f"{default_schema}.{table_ref}"
                    tables.add(table_ref.lower())

        return tables

    def _find_upstream_dependencies(self, obj: DBObject) -> List[Relationship]:
        """Найти объекты, которые зависят от текущего объекта (восходящие связи)."""
        relationships = []

        try:
            with self.conn.cursor() as cur:
                # Для всех типов объектов: найти объекты, которые на них ссылаются
                if obj.obj_type in (ObjectType.TABLE, ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW, ObjectType.FUNCTION):
                    # Найти представления, которые зависят от этого объекта
                    try:
                        cur.execute("""
                            SELECT DISTINCT
                                n.nspname as dep_schema,
                                c.relname as dep_name,
                                CASE c.relkind
                                    WHEN 'v' THEN 'VIEW'
                                    WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                END as obj_type,
                                'used_by' as rel_type,
                                1 as depth
                            FROM pg_rewrite rw
                            JOIN pg_class src ON rw.ev_class = src.oid
                            JOIN pg_depend d ON d.objid = rw.oid
                            JOIN pg_class c ON d.refobjid = c.oid
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            JOIN pg_namespace srcn ON src.relnamespace = srcn.oid
                            WHERE srcn.nspname = %s
                            AND src.relname = %s
                            AND d.deptype = 'n'
                            AND c.relkind IN ('v', 'm')
                        """, (obj.schema, obj.name))

                        view_deps = cur.fetchall()
                        for row in view_deps:
                            if len(row) == 5:
                                dep_schema, dep_name, dep_type, rel_type, depth = row
                                if dep_schema and dep_name and dep_type and not (dep_schema == obj.schema and dep_name == obj.name):
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

                    except Exception as e:
                        print(f"Ошибка при поиске view зависимостей: {e}")

                    # Найти функции, которые используют этот объект (анализируя их определения)
                    if obj.obj_type in (ObjectType.TABLE, ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                        try:
                            # Найдем все функции в схеме
                            cur.execute("""
                                SELECT n.nspname, p.proname, pg_get_functiondef(p.oid)
                                FROM pg_proc p
                                JOIN pg_namespace n ON p.pronamespace = n.oid
                                WHERE n.nspname = %s
                            """, (obj.schema,))

                            all_funcs = cur.fetchall()
                            for func_schema, func_name, func_def in all_funcs:
                                if func_def and f'{obj.schema}.{obj.name}' in func_def and not (func_schema == obj.schema and func_name == obj.name):
                                    target = DBObject(
                                        schema=func_schema,
                                        name=func_name,
                                        obj_type=ObjectType.FUNCTION,
                                        definition=""
                                    )
                                    relationships.append(
                                        Relationship(
                                            source=obj,
                                            target=target,
                                            relationship_type='used_by',
                                            depth=1
                                        )
                                    )

                        except Exception as e:
                            print(f"Ошибка при поиске func зависимостей: {e}")

                    # Найти триггеры, которые используют этот объект
                    if obj.obj_type == ObjectType.TABLE:
                        try:
                            cur.execute("""
                                SELECT DISTINCT
                                    n.nspname as trigger_schema,
                                    t.tgname as trigger_name,
                                    'TRIGGER' as obj_type,
                                    'used_by' as rel_type,
                                    1 as depth
                                FROM pg_trigger t
                                JOIN pg_class c ON t.tgrelid = c.oid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = %s
                                AND c.relname = %s
                                AND NOT t.tgisinternal
                            """, (obj.schema, obj.name))

                            trigger_deps = cur.fetchall()
                            for row in trigger_deps:
                                trigger_schema, trigger_name, obj_type, rel_type, depth = row
                                try:
                                    target = DBObject(
                                        schema=trigger_schema,
                                        name=trigger_name,
                                        obj_type=ObjectType[obj_type],
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
                                    print(f"Пропуск триггера {trigger_schema}.{trigger_name} с неизвестным типом: {obj_type}")

                        except Exception as e:
                            print(f"Ошибка при поиске trigger зависимостей: {e}")

                    # Найти таблицы и представления, которые зависят от этого объекта
                    if obj.obj_type in (ObjectType.TABLE, ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                        # Найти материализованные представления, которые используют этот объект
                        try:
                            cur.execute("""
                                SELECT n.nspname, c.relname, pg_get_viewdef(c.oid, true), 'MATERIALIZED_VIEW' as obj_type
                                FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = %s
                                AND c.relkind = 'm'
                            """, (obj.schema,))

                            all_mat_views = cur.fetchall()
                            for view_schema, view_name, view_def, view_type in all_mat_views:
                                if view_def and f'{obj.schema}.{obj.name}' in view_def and not (view_schema == obj.schema and view_name == obj.name):
                                    try:
                                        target = DBObject(
                                            schema=view_schema,
                                            name=view_name,
                                            obj_type=ObjectType[view_type],
                                            definition=""
                                        )
                                        relationships.append(
                                            Relationship(
                                                source=obj,
                                                target=target,
                                                relationship_type='used_by',
                                                depth=1
                                            )
                                        )
                                    except KeyError:
                                        print(f"Пропуск материализованного представления {view_schema}.{view_name} с неизвестным типом: {view_type}")

                        except Exception as e:
                            print(f"Ошибка при поиске зависимостей материализованных представлений: {e}")

                        # Найти обычные представления, которые используют этот объект
                        try:
                            cur.execute("""
                                SELECT n.nspname, c.relname, pg_get_viewdef(c.oid, true), 'VIEW' as obj_type
                                FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = %s
                                AND c.relkind = 'v'
                            """, (obj.schema,))

                            all_views = cur.fetchall()
                            for view_schema, view_name, view_def, view_type in all_views:
                                if view_def and f'{obj.schema}.{obj.name}' in view_def and not (view_schema == obj.schema and view_name == obj.name):
                                    try:
                                        target = DBObject(
                                            schema=view_schema,
                                            name=view_name,
                                            obj_type=ObjectType[view_type],
                                            definition=""
                                        )
                                        relationships.append(
                                            Relationship(
                                                source=obj,
                                                target=target,
                                                relationship_type='used_by',
                                                depth=1
                                            )
                                        )
                                    except KeyError:
                                        print(f"Пропуск представления {view_schema}.{view_name} с неизвестным типом: {view_type}")

                        except Exception as e:
                            print(f"Ошибка при поиске зависимостей представлений: {e}")

        except Exception as e:
            print(f"Ошибка при поиске восходящих зависимостей: {e}")

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

    def scan_object(self, schema: str, obj_name: str) -> List[Relationship]:
        """Scan object and its relationships."""
        # Определяем тип объекта
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    CASE 
                        WHEN c.relkind = 'r' THEN 'TABLE'
                        WHEN c.relkind = 'v' THEN 'VIEW'
                        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
                        ELSE c.relkind::text
                    END
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                UNION ALL
                SELECT 'FUNCTION'
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = %s AND p.proname = %s
            """, (schema, obj_name, schema, obj_name))
            
            result = cur.fetchone()
            if not result:
                return []
                
            obj_type = ObjectType[result[0]]
            
        # Создаем объект
        obj = DBObject(
            schema=schema,
            name=obj_name,
            obj_type=obj_type,
            definition=''
        )
        
        # Получаем определение
        obj.definition = self.get_object_definition(obj)
        
        # Получаем зависимости
        return self.find_related_objects(obj)