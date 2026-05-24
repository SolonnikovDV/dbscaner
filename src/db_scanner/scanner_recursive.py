"""
Database Object Dependency Graph Scanner - Recursive Scanner Module
Copyright (c) 2025 Dmitry Solonnikov
Licensed under MIT License - see LICENSE file for details
"""
from typing import List, Dict, Set, Optional
import re
import networkx as nx
from psycopg2.extensions import connection

from .models import DBObject, Relationship, ObjectType


SYSTEM_SCHEMAS = frozenset({
    'pg_catalog', 'information_schema', 'pg_toast',
    'pg_temp_1', 'pg_toast_temp_1',
})

def _is_system_object(schema: str) -> bool:
    """Return True for PostgreSQL/Greenplum internal schemas."""
    return (schema in SYSTEM_SCHEMAS
            or schema.startswith('pg_')
            or schema.startswith('gp_'))


SERVICE_PATTERNS = [
    # Logging framework functions (infrastructure, not business logic)
    r'^srv_create_log$',
    r'^srv_add_log_entry$',
    r'^srv_save_log$',
    r'^srv_add_log_error$',
    r'^srv_add_log_to_buffer$',
    r'^srv_flush_log$',
    r'^tp_log_instance$',
    r'^tp_log',
    r'^srv_.*log',
    r'_log_instance$',
]

def _is_service_object(name: str) -> bool:
    """Return True for infrastructure/service objects (logging, monitoring)."""
    return any(re.match(p, name.lower()) for p in SERVICE_PATTERNS)


NOISE_PATTERNS = [
    r'.*_log$',
    r'.*_logs$',
    r'.*_audit$',
    r'^audit_.*',
    r'^log_.*',
    r'^logging_.*',
    r'.*_history$',
    r'.*_archive$',
    r'^arch_.*',
    r'.*_tmp$',
    r'.*_temp$',
]


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
                        try:
                            cur.execute(
                                "SELECT pg_get_functiondef(p.oid) "
                                "FROM pg_proc p "
                                "JOIN pg_namespace n ON p.pronamespace = n.oid "
                                "WHERE n.nspname = %s AND p.proname = %s "
                                "LIMIT 1",
                                (obj.schema, obj.name)
                            )
                            func_def_result = cur.fetchone()
                            print(f"[SCAN] {obj.schema}.{obj.name}: "
                                  f"func_def={'found len=' + str(len(func_def_result[0])) if func_def_result and func_def_result[0] else 'NONE'}")
                            if func_def_result and func_def_result[0]:
                                func_sql = func_def_result[0]
                                table_refs = self._extract_table_references(func_sql, obj.schema)
                                print(f"[SCAN]   extracted {len(table_refs)} refs: {sorted(table_refs)[:8]}")

                                for table_ref in table_refs:
                                    table_parts = table_ref.split('.')
                                    if len(table_parts) == 2:
                                        dep_schema, dep_name = table_parts
                                    else:
                                        dep_schema, dep_name = obj.schema, table_parts[0]

                                    # Check pg_class first (TABLE/VIEW/MAT_VIEW)
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
                                    type_row = cur.fetchone()
                                    obj_type_str = type_row[0] if type_row else None

                                    # If not found as table/view, check pg_proc (FUNCTION)
                                    if not obj_type_str:
                                        cur.execute("""
                                            SELECT 'FUNCTION'
                                            FROM pg_proc p
                                            JOIN pg_namespace n ON p.pronamespace = n.oid
                                            WHERE n.nspname = %s AND p.proname = %s
                                            LIMIT 1
                                        """, (dep_schema, dep_name))
                                        func_row = cur.fetchone()
                                        obj_type_str = func_row[0] if func_row else None

                                    if obj_type_str:
                                        print(f"[SCAN]   + dep: {dep_schema}.{dep_name} ({obj_type_str})")
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
                                            print(f"[SCAN]   KEYERROR: {obj_type_str}")
                                    else:
                                        print(f"[SCAN]   ? not found in DB: {dep_schema}.{dep_name}")

                        except Exception as e:
                            import traceback
                            print(f"[SCAN] EXCEPTION in {obj.schema}.{obj.name}: {e}")
                            traceback.print_exc()

                except Exception as e:
                    print(f"Ошибка при поиске нисходящих зависимостей: {e}")

        return relationships

    def _extract_table_references(self, sql: str, default_schema: str) -> set:
        """Извлечь все ссылки на таблицы из SQL кода."""
        import re

        # Identifier: optional schema prefix + name
        _ID = r'([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)'

        # Phase 1 — keyword-context patterns (for tables/views in DML)
        patterns = [
            rf'\bFROM\s+{_ID}',
            rf'\bJOIN\s+{_ID}',
            rf'\bINNER\s+JOIN\s+{_ID}',
            rf'\bLEFT\s+(?:OUTER\s+)?JOIN\s+{_ID}',
            rf'\bRIGHT\s+(?:OUTER\s+)?JOIN\s+{_ID}',
            rf'\bFULL\s+(?:OUTER\s+)?JOIN\s+{_ID}',
            rf'\bCROSS\s+JOIN\s+{_ID}',
            rf'\bUPDATE\s+(?:ONLY\s+)?{_ID}',
            rf'\bINSERT\s+INTO\s+{_ID}',
            rf'\bINSERT\s+OVERWRITE\s+(?:INTO\s+)?{_ID}',
            rf'\bDELETE\s+FROM\s+{_ID}',
            rf'\bTRUNCATE\s+(?:TABLE\s+)?{_ID}',
            rf'\bLOCK\s+TABLE\s+{_ID}',
            rf'\bPERFORM\s+{_ID}',
        ]

        tables = set()

        # Phase 1 — keyword-context patterns
        for pattern in patterns:
            for match in re.finditer(pattern, sql, re.IGNORECASE | re.MULTILINE | re.DOTALL):
                ref = match.group(1).strip()
                if ref:
                    if '.' not in ref:
                        ref = f"{default_schema}.{ref}"
                    tables.add(ref.lower())

        # Phase 2 — catch-all: every qualified schema.name in the text.
        # This mirrors what highlightDDL does in the browser: finds function calls
        # in assignments (:= schema.func()), conditionals (IF schema.func()),
        # WHERE clauses, and any other context not covered by Phase 1.
        # False positives (e.g. record.field) are filtered by the DB existence check
        # in the calling code — only real objects survive.
        for match in re.finditer(r'\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b',
                                  sql, re.IGNORECASE):
            schema_part = match.group(1).lower()
            name_part   = match.group(2).lower()
            # Skip obviously non-schema identifiers:
            # SQL special words that can appear before a dot (e.g. "NEW.col", "OLD.col")
            if schema_part in ('new', 'old', 'excluded', 'current_date',
                               'current_timestamp', 'pg_catalog',
                               'information_schema', 'pg_toast'):
                continue
            tables.add(f"{schema_part}.{name_part}")

        return tables

    def _find_upstream_dependencies(self, obj: DBObject) -> List[Relationship]:
        """Найти объекты, которые ИСПОЛЬЗУЮТ текущий объект (восходящие связи).

        Corrected query: find views/functions that reference OUR object,
        not what our object depends on (that was the original bug).
        """
        relationships = []

        try:
            with self.conn.cursor() as cur:
                if obj.obj_type in (ObjectType.TABLE, ObjectType.VIEW,
                                    ObjectType.MATERIALIZED_VIEW, ObjectType.FUNCTION):

                    # ── Views/Mat-views that reference our object via pg_rewrite ──────────
                    # src = the VIEW that has the rewrite rule (the consumer)
                    # c   = what src depends on = OUR OBJECT (the referenced/depended-upon)
                    try:
                        cur.execute("""
                            SELECT DISTINCT
                                srcn.nspname  AS dep_schema,
                                src.relname   AS dep_name,
                                CASE src.relkind
                                    WHEN 'v' THEN 'VIEW'
                                    WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                END           AS obj_type,
                                'used_by'     AS rel_type,
                                1             AS depth
                            FROM pg_rewrite rw
                            JOIN pg_class src ON rw.ev_class = src.oid
                            JOIN pg_depend d   ON d.objid = rw.oid
                            JOIN pg_class c    ON d.refobjid = c.oid
                            JOIN pg_namespace n    ON n.oid = c.relnamespace
                            JOIN pg_namespace srcn ON src.relnamespace = srcn.oid
                            WHERE n.nspname  = %s
                              AND c.relname  = %s
                              AND d.deptype  = 'n'
                              AND src.relkind IN ('v', 'm')
                              AND NOT (srcn.nspname = %s AND src.relname = %s)
                        """, (obj.schema, obj.name, obj.schema, obj.name))

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

                    # ── Functions that reference our table/view (text scan for PL/pgSQL) ──
                    if obj.obj_type in (ObjectType.TABLE, ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                        try:
                            cur.execute("""
                                SELECT n.nspname, p.proname, pg_get_functiondef(p.oid)
                                FROM pg_proc p
                                JOIN pg_namespace n ON p.pronamespace = n.oid
                                WHERE n.nspname = %s
                                  AND NOT EXISTS (
                                      SELECT 1 FROM pg_depend d JOIN pg_extension e ON d.refobjid = e.oid
                                      WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'
                                  )
                            """, (obj.schema,))
                            fqn = f'{obj.schema}.{obj.name}'.lower()
                            for func_schema, func_name, func_def in cur.fetchall():
                                if (func_def and fqn in func_def.lower()
                                        and not (func_schema == obj.schema and func_name == obj.name)):
                                    target = DBObject(
                                        schema=func_schema, name=func_name,
                                        obj_type=ObjectType.FUNCTION, definition=""
                                    )
                                    relationships.append(Relationship(
                                        source=obj, target=target,
                                        relationship_type='used_by', depth=1
                                    ))
                        except Exception as e:
                            print(f"Ошибка при поиске func зависимостей: {e}")

                    # ── FUNCTION: who calls THIS function (via pg_depend + text scan) ────
                    if obj.obj_type == ObjectType.FUNCTION:
                        # pg_depend: views/mat-views that depend on this function
                        try:
                            cur.execute("""
                                SELECT DISTINCT
                                    srcn.nspname, src.relname,
                                    CASE src.relkind
                                        WHEN 'v' THEN 'VIEW'
                                        WHEN 'm' THEN 'MATERIALIZED_VIEW'
                                    END,
                                    'used_by', 1
                                FROM pg_depend d
                                JOIN pg_proc p   ON d.refobjid = p.oid
                                JOIN pg_namespace pn  ON p.pronamespace = pn.oid
                                JOIN pg_class src ON d.objid = src.oid
                                JOIN pg_namespace srcn ON src.relnamespace = srcn.oid
                                WHERE pn.nspname = %s AND p.proname = %s
                                  AND d.deptype IN ('n', 'a')
                                  AND src.relkind IN ('v', 'm')
                                  AND NOT (srcn.nspname = %s AND src.relname = %s)
                            """, (obj.schema, obj.name, obj.schema, obj.name))
                            for row in cur.fetchall():
                                dep_schema, dep_name, dep_type, rel_type, depth = row
                                if dep_schema and dep_name and dep_type:
                                    try:
                                        target = DBObject(
                                            schema=dep_schema, name=dep_name,
                                            obj_type=ObjectType[dep_type], definition=""
                                        )
                                        relationships.append(Relationship(
                                            source=obj, target=target,
                                            relationship_type=rel_type, depth=depth
                                        ))
                                    except KeyError:
                                        pass
                        except Exception as e:
                            print(f"Ошибка при поиске view→func зависимостей: {e}")

                        # Text scan: other functions in schema that call this function
                        try:
                            cur.execute("""
                                SELECT n.nspname, p.proname, pg_get_functiondef(p.oid)
                                FROM pg_proc p
                                JOIN pg_namespace n ON p.pronamespace = n.oid
                                WHERE n.nspname = %s
                                  AND p.proname != %s
                                  AND NOT EXISTS (
                                      SELECT 1 FROM pg_depend d JOIN pg_extension e ON d.refobjid = e.oid
                                      WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'
                                  )
                            """, (obj.schema, obj.name))
                            for caller_schema, caller_name, caller_def in cur.fetchall():
                                if caller_def and obj.name.lower() in caller_def.lower():
                                    target = DBObject(
                                        schema=caller_schema, name=caller_name,
                                        obj_type=ObjectType.FUNCTION, definition=""
                                    )
                                    relationships.append(Relationship(
                                        source=obj, target=target,
                                        relationship_type='used_by', depth=1
                                    ))
                        except Exception as e:
                            print(f"Ошибка при поиске func→func зависимостей: {e}")

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
        with self.conn.cursor() as cur:
            # Two separate queries — Greenplum UNION ALL can reorder operands
            cur.execute("""
                SELECT 1 FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = %s AND p.proname = %s LIMIT 1
            """, (schema, obj_name))
            if cur.fetchone() is not None:
                result_type = 'FUNCTION'
            else:
                cur.execute("""
                    SELECT CASE
                        WHEN c.relkind = 'r' THEN 'TABLE'
                        WHEN c.relkind = 'v' THEN 'VIEW'
                        WHEN c.relkind = 'm' THEN 'MATERIALIZED_VIEW'
                    END
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                      AND c.relkind IN ('r', 'v', 'm')
                    LIMIT 1
                """, (schema, obj_name))
                row = cur.fetchone()
                if not row:
                    return []
                result_type = row[0]

            obj_type = ObjectType[result_type]

        obj = DBObject(
            schema=schema,
            name=obj_name,
            obj_type=obj_type,
            definition=''
        )
        obj.definition = self.get_object_definition(obj)
        return self.find_related_objects(obj)

    def is_noise_object(self, name: str) -> bool:
        """Return True if object name matches a known noise pattern (logging, audit, etc.)."""
        return any(re.match(p, name.lower()) for p in NOISE_PATTERNS)

    def deep_scan_upstream_functions(self, obj: DBObject,
                                     exclude_self: tuple) -> List[Relationship]:
        """
        Cross-schema full-text scan: find ALL functions (any schema) that reference
        schema.object_name in their body.  Uses prosrc for performance.
        Expensive — call only when deep_scan=True.
        """
        fqn = f'{obj.schema}.{obj.name}'.lower()
        relationships = []
        try:
            with self.conn.cursor() as cur:
                # Use prosrc only (avoids errors on aggregate/window functions
                # where pg_get_functiondef raises "is an aggregate function")
                cur.execute("""
                    SELECT n.nspname, p.proname
                    FROM pg_proc p
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE n.nspname NOT LIKE 'pg_%%'
                      AND n.nspname NOT LIKE 'gp_%%'
                      AND n.nspname != 'information_schema'
                      AND p.prosrc ILIKE %s
                      AND p.proisagg = false
                      AND NOT (n.nspname = %s AND p.proname = %s)
                      AND NOT EXISTS (
                          SELECT 1 FROM pg_depend d
                          JOIN pg_extension e ON d.refobjid = e.oid
                          WHERE d.classid = 'pg_proc'::regclass
                            AND d.objid = p.oid AND d.deptype = 'e'
                      )
                """, (f'%{fqn}%', exclude_self[0], exclude_self[1]))
                for func_schema, func_name in cur.fetchall():
                    target = DBObject(
                        schema=func_schema, name=func_name,
                        obj_type=ObjectType.FUNCTION, definition=""
                    )
                    relationships.append(Relationship(
                        source=obj, target=target,
                        relationship_type='used_by', depth=1
                    ))
        except Exception as e:
            print(f"Deep scan error for {fqn}: {e}")
        return relationships

    def build_dependency_graph(
        self,
        central_obj: DBObject,
        max_depth: int = 10,
        exclude_noise: bool = False,
        direction: str = 'both',
        deep_scan: bool = False,
    ) -> dict:
        """
        Build a full dependency graph starting from central_obj.

        direction:
            'both'       — upstream + downstream (default)
            'downstream' — only objects that central_obj depends on
            'upstream'   — only objects that use central_obj

        Returns dict with keys: nodes, links, cycles, noise_filtered.
        """
        nodes: List[Dict] = []
        links: List[Dict] = []
        node_ids: Set[str] = set()
        processed_objects: Set[tuple] = set()
        noise_filtered: List[str] = []
        central_node_id = f"{central_obj.schema}.{central_obj.name}"

        def _is_noise(name: str) -> bool:
            return exclude_noise and self.is_noise_object(name)

        def _add_node(obj: DBObject, is_svc: bool = False) -> None:
            if _is_system_object(obj.schema):
                return
            node_id = f"{obj.schema}.{obj.name}"
            if node_id not in node_ids:
                nodes.append({
                    "id":        node_id,
                    "name":      obj.name,
                    "schema":    obj.schema,
                    "type":      obj.obj_type.name,
                    "isCentral": node_id == central_node_id,
                    "is_service": is_svc,
                })
                node_ids.add(node_id)

        def _traverse(obj: DBObject, current_depth: int) -> None:
            obj_key = (obj.schema, obj.name)
            if current_depth >= max_depth or obj_key in processed_objects:
                return
            processed_objects.add(obj_key)

            obj_deps = self.find_related_objects(obj)

            for dep in obj_deps:
                if direction == 'downstream' and dep.relationship_type != 'depends_on':
                    continue
                if direction == 'upstream' and dep.relationship_type != 'used_by':
                    continue

                # Skip system schema objects
                if _is_system_object(dep.source.schema) or _is_system_object(dep.target.schema):
                    continue

                src_svc = _is_service_object(dep.source.name)
                tgt_svc = _is_service_object(dep.target.name)

                # When noise filter ON: skip noise AND service objects
                for side_name, side_schema, is_svc in [
                    (dep.source.name, dep.source.schema, src_svc),
                    (dep.target.name, dep.target.schema, tgt_svc),
                ]:
                    if _is_noise(side_name) or (exclude_noise and is_svc):
                        fqn = f"{side_schema}.{side_name}"
                        if fqn not in noise_filtered:
                            noise_filtered.append(fqn)

                # Skip entire dep if either side is noise or (service + filter on)
                if _is_noise(dep.source.name) or _is_noise(dep.target.name):
                    continue
                if exclude_noise and (src_svc or tgt_svc):
                    continue

                _add_node(dep.source, is_svc=src_svc)
                _add_node(dep.target, is_svc=tgt_svc)

                if dep.relationship_type == 'depends_on':
                    source_id = f"{dep.source.schema}.{dep.source.name}"
                    target_id = f"{dep.target.schema}.{dep.target.name}"
                else:
                    source_id = f"{dep.target.schema}.{dep.target.name}"
                    target_id = f"{dep.source.schema}.{dep.source.name}"

                link = {
                    "source": source_id,
                    "target": target_id,
                    "type": dep.relationship_type,
                    "depth": dep.depth,
                }
                if link not in links:
                    links.append(link)

                if (dep.target.schema, dep.target.name) not in processed_objects:
                    _traverse(dep.target, current_depth + 1)

        _add_node(central_obj)
        _traverse(central_obj, 0)

        # Deep scan: cross-schema function upstream search (expensive, on-demand)
        if deep_scan and direction in ('both', 'upstream'):
            extra_rels = self.deep_scan_upstream_functions(
                central_obj, (central_obj.schema, central_obj.name)
            )
            for dep in extra_rels:
                tgt_svc = _is_service_object(dep.target.name)
                if _is_noise(dep.target.name):
                    continue
                if exclude_noise and tgt_svc:
                    continue
                _add_node(dep.target, is_svc=tgt_svc)
                key = (dep.target.schema, dep.target.name)
                if key not in processed_objects:
                    _traverse(dep.target, 1)
                tgt_id = f"{dep.target.schema}.{dep.target.name}"
                link = {
                    "source": tgt_id,
                    "target": central_node_id,
                    "type": "depends_on",
                    "depth": 1,
                    "deep_scan": True,
                }
                if link not in links:
                    links.append(link)

        # Detect cycles in the built graph
        G = nx.DiGraph()
        for link in links:
            G.add_edge(link["source"], link["target"])

        cycles: List[List[str]] = []
        try:
            for cycle in nx.simple_cycles(G):
                cycles.append(cycle)
        except Exception:
            pass

        return {
            "nodes": nodes,
            "links": links,
            "cycles": cycles,
            "noise_filtered": noise_filtered,
        }