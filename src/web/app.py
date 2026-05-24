"""
Database Object Dependency Graph Scanner - Web Application
Copyright (c) 2025 Dmitry Solonnikov
Licensed under MIT License - see LICENSE file for details
"""

from flask import Flask, render_template, jsonify, request
from src.db_scanner.scanner_recursive import DBScanner
from src.db_scanner.column_lineage import ColumnLineageScanner
from src.db_scanner.alias_tracker import AliasTracker
from src.db.connection import DBConnection
from src.instance_store import InstanceStore

app = Flask(__name__)

instance_store = InstanceStore()


class _AppServices:
    """Holds DB connection and scanners; supports hot-swap on instance change."""

    def __init__(self):
        self.connection = None
        self.scanner = None
        self.col_scanner = None
        self.alias_tracker = None
        self.reconnect(instance_store.get_active_config())

    def reconnect(self, config):
        if self.connection:
            self.connection.close()
        self.connection = DBConnection(config)
        self.scanner = DBScanner(self.connection)
        self.col_scanner = ColumnLineageScanner(self.connection)
        self.alias_tracker = AliasTracker(self.connection)


services = _AppServices()


@app.errorhandler(Exception)
def handle_exception(e):
    """Return JSON instead of HTML for all unhandled exceptions."""
    import traceback
    app.logger.error(traceback.format_exc())
    return jsonify({"error": str(e), "nodes": [], "links": [], "cycles": [], "noise_filtered": []}), 500

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/about')
def get_about():
    """Application metadata, features, license and author info."""
    import os
    root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    license_text = ""
    license_path = os.path.join(root, "LICENSE")
    if os.path.isfile(license_path):
        with open(license_path, "r", encoding="utf-8") as f:
            license_text = f.read()

    return jsonify({
        "name": "DB Graph Explorer",
        "tagline": "PostgreSQL / Greenplum dependency graph & data lineage tool",
        "description": (
            "Инструмент для анализа и визуализации графа зависимостей и data lineage "
            "объектов PostgreSQL / Greenplum. Исследуйте связи между таблицами, "
            "представлениями и функциями, отслеживайте потоки данных на уровне колонок, "
            "симулируйте impact при изменениях схемы."
        ),
        "features": [
            "Object-level dependency graph (upstream / downstream / both)",
            "Column-level lineage via sqllineage",
            "Attribute alias evolution tracking",
            "Impact analysis (breaking / warning) for column drops",
            "Pipeline view — full data flow from source to sink",
            "Deep Scan — cross-schema function body text search",
            "Cycle detection and noise filtering",
            "Multi-instance database connection management",
            "Interactive DDL viewer with clickable schema.object references",
            "Global search across objects and columns",
        ],
        "author": "Dmitry Solonnikov",
        "copyright": "Copyright © 2025 Dmitry Solonnikov",
        "license_name": "MIT License",
        "license_text": license_text,
        "license_url": "https://opensource.org/licenses/MIT",
        "telegram": "https://t.me/Dmitry_as_SoloD",
        "telegram_handle": "@Dmitry_as_SoloD",
    })


# ── Instance management ──────────────────────────────────────────────────────

@app.route('/api/instances')
def list_instances():
    active_id = instance_store.get_active_id()
    return jsonify({
        "active_id": active_id,
        "instances": [
            {**inst.to_public(), "is_active": inst.id == active_id}
            for inst in instance_store.list_instances()
        ],
    })


@app.route('/api/instances/active')
def get_active_instance():
    inst = instance_store.get_active()
    return jsonify({**inst.to_public(), "is_active": True})


@app.route('/api/instances', methods=['POST'])
def create_instance():
    data = request.get_json(force=True) or {}
    required = ("name", "host", "port", "database", "user")
    missing = [k for k in required if not str(data.get(k, "")).strip()]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400
    inst = instance_store.add(
        name=data["name"],
        host=data["host"],
        port=str(data["port"]),
        database=data["database"],
        user=data["user"],
        password=data.get("password", ""),
    )
    if data.get("set_active"):
        instance_store.set_active(inst.id)
        services.reconnect(inst.to_config())
    return jsonify({**inst.to_public(), "is_active": inst.id == instance_store.get_active_id()}), 201


@app.route('/api/instances/<instance_id>', methods=['PUT'])
def update_instance(instance_id):
    data = request.get_json(force=True) or {}
    try:
        inst = instance_store.update(instance_id, **data)
    except KeyError:
        return jsonify({"error": "Instance not found"}), 404
    if instance_id == instance_store.get_active_id():
        services.reconnect(inst.to_config())
    return jsonify({**inst.to_public(), "is_active": instance_id == instance_store.get_active_id()})


@app.route('/api/instances/<instance_id>', methods=['DELETE'])
def delete_instance(instance_id):
    try:
        was_active = instance_id == instance_store.get_active_id()
        instance_store.delete(instance_id)
        if was_active:
            services.reconnect(instance_store.get_active_config())
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except KeyError:
        return jsonify({"error": "Instance not found"}), 404
    return jsonify({"ok": True, "active_id": instance_store.get_active_id()})


@app.route('/api/instances/<instance_id>/activate', methods=['POST'])
def activate_instance(instance_id):
    try:
        inst = instance_store.set_active(instance_id)
        services.reconnect(inst.to_config())
    except KeyError:
        return jsonify({"error": "Instance not found"}), 404
    return jsonify({**inst.to_public(), "is_active": True})


@app.route('/api/instances/test', methods=['POST'])
def test_instance_connection():
    data = request.get_json(force=True) or {}
    config = {
        "host": data.get("host", ""),
        "port": str(data.get("port", "5432")),
        "database": data.get("database", ""),
        "user": data.get("user", ""),
        "password": data.get("password", ""),
    }
    missing = [k for k in ("host", "database", "user") if not config[k]]
    if missing:
        return jsonify({"ok": False, "error": f"Missing: {', '.join(missing)}"}), 400
    try:
        DBConnection(config).test()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route('/schemas')
def get_schemas():
    """Получение списка схем"""
    with services.connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT nspname 
            FROM pg_namespace 
            WHERE nspname NOT LIKE 'pg_%' 
              AND nspname NOT LIKE 'gp_%'
            AND nspname != 'information_schema'
              AND nspname != 'catalog_history'
              AND nspname != 'madlib'
            ORDER BY nspname
        """)
        schemas = [row[0] for row in cur.fetchall()]
    return jsonify(schemas)

@app.route('/objects/<schema>')
def get_objects(schema):
    """Получение списка объектов в схеме (исключая объекты расширений и системные)."""
    with services.connection.cursor() as cur:
        cur.execute("""
            SELECT 
                c.relname AS name,
                CASE c.relkind
                    WHEN 'r' THEN 'TABLE'
                    WHEN 'v' THEN 'VIEW'
                    WHEN 'm' THEN 'MATERIALIZED_VIEW'
                    WHEN 'f' THEN 'FOREIGN_TABLE'
                    WHEN 'p' THEN 'PARTITIONED_TABLE'
                END AS type
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
            AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
              -- exclude objects owned by extensions
              AND NOT EXISTS (
                  SELECT 1 FROM pg_depend d
                  JOIN pg_extension e ON d.refobjid = e.oid
                  WHERE d.classid = 'pg_class'::regclass
                    AND d.objid = c.oid
                    AND d.deptype = 'e'
              )

            UNION

            SELECT 
                p.proname AS name,
                'FUNCTION' AS type
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s
              -- exclude functions owned by extensions
              AND NOT EXISTS (
                  SELECT 1 FROM pg_depend d
                  JOIN pg_extension e ON d.refobjid = e.oid
                  WHERE d.classid = 'pg_proc'::regclass
                    AND d.objid = p.oid
                    AND d.deptype = 'e'
              )

            ORDER BY name
        """, (schema, schema))
        objects = [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
    return jsonify(objects)

@app.route('/graph/<schema>/<object_name>')
def get_graph(schema, object_name):
    """Получение данных для графа зависимостей."""
    from src.db_scanner.models import DBObject, ObjectType

    direction    = request.args.get('direction', 'both')
    exclude_noise = request.args.get('exclude_noise', 'false').lower() == 'true'
    max_depth    = min(int(request.args.get('max_depth', 10)), 20)
    deep_scan    = request.args.get('deep_scan', 'false').lower() == 'true'

    RELKIND_MAP = {
        'r': 'TABLE', 'v': 'VIEW', 'm': 'MATERIALIZED_VIEW',
        'p': 'TABLE', 'f': 'TABLE',
    }

    with services.connection.cursor() as cur:
        # Two separate queries — Greenplum's UNION ALL optimizer can reorder operands,
        # so we must use explicit sequential checks to guarantee pg_proc is tried first.
        # (RETURNS TABLE functions create a pg_class entry with relkind='f'/'c' that
        #  a single UNION ALL would pick up before pg_proc.)

        # 1. Check pg_proc (functions/procedures)
        cur.execute("""
            SELECT 1
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s AND p.proname = %s
            LIMIT 1
        """, (schema, object_name))
        is_func = cur.fetchone() is not None

        if is_func:
            obj_type = ObjectType.FUNCTION
            print(f"[GET_GRAPH] {schema}.{object_name} → FUNCTION (pg_proc)")
        else:
            # 2. Not a function — look for table/view in pg_class
            cur.execute("""
                SELECT c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                  AND c.relkind IN ('r', 'v', 'm', 'p')
                LIMIT 1
            """, (schema, object_name))
            row = cur.fetchone()
            if not row:
                print(f"[GET_GRAPH] {schema}.{object_name} → NOT FOUND")
                return jsonify({"nodes": [], "links": [], "cycles": [], "noise_filtered": []})
            raw = row[0]
            type_str = RELKIND_MAP.get(raw, 'TABLE')
            obj_type = ObjectType[type_str]
            print(f"[GET_GRAPH] {schema}.{object_name} raw={raw!r} → {obj_type}")

    central_obj = DBObject(
        schema=schema,
        name=object_name,
        obj_type=obj_type,
        definition=''
    )

    graph_data = services.scanner.build_dependency_graph(
        central_obj,
        max_depth=max_depth,
        exclude_noise=exclude_noise,
        direction=direction,
        deep_scan=deep_scan,
    )
    return jsonify(graph_data)

def _resolve_obj_type(schema, object_name, cur):
    """Return ObjectType — two separate queries to avoid Greenplum UNION ALL reordering."""
    from src.db_scanner.models import ObjectType
    RELKIND_MAP = {'r': 'TABLE', 'v': 'VIEW', 'm': 'MATERIALIZED_VIEW', 'p': 'TABLE', 'f': 'TABLE'}

    # Check pg_proc first (explicit sequential, not UNION ALL)
    cur.execute("""
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s AND p.proname = %s LIMIT 1
    """, (schema, object_name))
    if cur.fetchone() is not None:
        return ObjectType.FUNCTION

    # Fall back to pg_class
    cur.execute("""
        SELECT c.relkind FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
          AND c.relkind IN ('r', 'v', 'm', 'p') LIMIT 1
    """, (schema, object_name))
    row = cur.fetchone()
    if not row:
        return None
    return ObjectType[RELKIND_MAP.get(row[0], 'TABLE')]


@app.route('/graph/<schema>/<object_name>/columns')
def get_column_lineage(schema, object_name):
    """Column-level lineage for the given object."""
    from src.db_scanner.models import DBObject

    with services.connection.cursor() as cur:
        obj_type = _resolve_obj_type(schema, object_name, cur)
        if obj_type is None:
            return jsonify({"error": "Object not found", "columns": [], "lineage": []})

    obj = DBObject(schema=schema, name=object_name, obj_type=obj_type, definition='')
    return jsonify(services.col_scanner.get_column_lineage(obj))


@app.route('/lineage/attribute/<schema>/<object_name>/<column>')
def get_attribute_lineage(schema, object_name, column):
    """Trace a column through the chain of objects (alias history)."""
    direction  = request.args.get('direction', 'both')
    max_depth  = min(int(request.args.get('max_depth', 15)), 30)
    services.alias_tracker.max_depth = max_depth
    try:
        return jsonify(services.alias_tracker.trace(schema, object_name, column, direction=direction))
    except Exception as exc:
        return jsonify({"error": str(exc), "path": [], "renames": [], "total_hops": 0})


@app.route('/ddl/<schema>/<object_name>')
def get_ddl(schema, object_name):
    """Получение DDL объекта."""
    with services.connection.cursor() as cur:
        # VIEW / MATERIALIZED VIEW
        cur.execute("""
            SELECT pg_get_viewdef(c.oid, true)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            AND c.relkind IN ('v', 'm')
        """, (schema, object_name))
        row = cur.fetchone()
        if row:
            return row[0] or "-- empty view definition"

        # FUNCTION
        cur.execute("""
            SELECT pg_get_functiondef(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s AND p.proname = %s
        """, (schema, object_name))
        row = cur.fetchone()
        if row:
            return row[0]

        # TABLE — reconstruct CREATE TABLE from catalog
        cur.execute("""
            SELECT
                'CREATE TABLE ' || n.nspname || '.' || c.relname || E' (\n' ||
                string_agg(
                    '  ' || a.attname || ' ' ||
                    pg_catalog.format_type(a.atttypid, a.atttypmod) ||
                    CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END,
                    E',\n'
                    ORDER BY a.attnum
                ) || E'\n);'
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_attribute a ON c.oid = a.attrelid
            WHERE n.nspname = %s AND c.relname = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            GROUP BY n.nspname, c.relname
        """, (schema, object_name))
        row = cur.fetchone()
        if row:
            return row[0]

    return "-- DDL not available"

@app.route('/search')
def search():
    """Full-text search across objects and columns in all schemas."""
    q      = request.args.get('q', '').strip()
    schema = request.args.get('schema', '')
    limit  = min(int(request.args.get('limit', 40)), 100)

    if len(q) < 2:
        return jsonify([])

    pattern = f'%{q}%'
    ext_filter = """
        AND NOT EXISTS (
            SELECT 1 FROM pg_depend d JOIN pg_extension e ON d.refobjid = e.oid
            WHERE d.classid = %s AND d.objid = oid_col AND d.deptype = 'e'
        )
    """
    ns_filter = """
        AND n.nspname NOT LIKE 'pg_%%'
        AND n.nspname NOT LIKE 'gp_%%'
        AND n.nspname != 'information_schema'
    """
    schema_cond = "AND n.nspname = %s" if schema else ""

    results = []
    with services.connection.cursor() as cur:
        # Objects (tables/views/mat-views)
        params = [pattern]
        if schema:
            params.append(schema)
        cur.execute(f"""
            SELECT n.nspname, c.relname,
                   CASE c.relkind
                       WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW'
                       WHEN 'm' THEN 'MATERIALIZED_VIEW' ELSE 'TABLE' END,
                   'object'
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relname ILIKE %s
              AND c.relkind IN ('r','v','m','p','f')
              {ns_filter} {schema_cond}
              AND NOT EXISTS (
                  SELECT 1 FROM pg_depend d JOIN pg_extension e ON d.refobjid = e.oid
                  WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e'
              )
            ORDER BY c.relname LIMIT %s
        """, params + [limit])
        for r in cur.fetchall():
            results.append({'schema': r[0], 'name': r[1], 'type': r[2],
                            'result_type': 'object', 'match_field': 'name'})

        # Functions
        cur.execute(f"""
            SELECT n.nspname, p.proname, 'FUNCTION', 'object'
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.proname ILIKE %s
              {ns_filter} {schema_cond}
              AND NOT EXISTS (
                  SELECT 1 FROM pg_depend d JOIN pg_extension e ON d.refobjid = e.oid
                  WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'
              )
            ORDER BY p.proname LIMIT %s
        """, params + [limit // 2])
        for r in cur.fetchall():
            results.append({'schema': r[0], 'name': r[1], 'type': r[2],
                            'result_type': 'object', 'match_field': 'name'})

        # Columns (only if query >= 3 chars)
        if len(q) >= 3:
            cur.execute(f"""
                SELECT n.nspname, c.relname,
                       CASE c.relkind
                           WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW'
                           WHEN 'm' THEN 'MATERIALIZED_VIEW' ELSE 'TABLE' END,
                       a.attname, 'column'
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE a.attname ILIKE %s
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND c.relkind IN ('r','v','m')
                  {ns_filter} {schema_cond}
                ORDER BY a.attname, c.relname LIMIT %s
            """, params + [limit // 2])
            for r in cur.fetchall():
                results.append({'schema': r[0], 'name': r[1], 'type': r[2],
                                'column': r[3], 'result_type': 'column', 'match_field': 'column'})

    # Deduplicate and return
    seen, unique = set(), []
    for item in results:
        key = (item['schema'], item['name'], item.get('column', ''))
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return jsonify(unique[:limit])


@app.route('/metadata/<schema>/<object_name>')
def get_metadata(schema, object_name):
    """Object metadata: owner, size, row count, last analyze, description."""
    with services.connection.cursor() as cur:
        cur.execute("""
            SELECT
                r.rolname                                            AS owner,
                pg_size_pretty(pg_total_relation_size(c.oid))       AS size,
                c.reltuples::bigint                                  AS est_rows,
                s.last_analyze,
                s.n_live_tup,
                obj_description(c.oid, 'pg_class')                  AS description,
                c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_roles r ON c.relowner = r.oid
            LEFT JOIN pg_stat_user_tables s
                   ON s.schemaname = n.nspname AND s.relname = c.relname
            WHERE n.nspname = %s AND c.relname = %s
        """, (schema, object_name))
        row = cur.fetchone()

    if not row:
        # Try function
        with services.connection.cursor() as cur:
            cur.execute("""
                SELECT r.rolname, NULL, NULL, NULL, NULL,
                       obj_description(p.oid, 'pg_proc'), 'f'
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                LEFT JOIN pg_roles r ON p.proowner = r.oid
                WHERE n.nspname = %s AND p.proname = %s LIMIT 1
            """, (schema, object_name))
            row = cur.fetchone()

    if not row:
        return jsonify({'schema': schema, 'name': object_name, 'error': 'not found'})

    kind_label = {'r': 'Table', 'v': 'View', 'm': 'Materialized View',
                  'p': 'Partitioned Table', 'f': 'Function'}.get(row[6], 'Object')

    return jsonify({
        'schema':       schema,
        'name':         object_name,
        'object_type':  kind_label,
        'owner':        row[0],
        'size':         row[1],
        'est_rows':     row[2],
        'last_analyze': str(row[3]) if row[3] else None,
        'live_rows':    row[4],
        'description':  row[5],
    })


@app.route('/impact/<schema>/<object_name>')
def get_impact(schema, object_name):
    """
    Objects that depend on (use) this object — impact if this object changes.
    Returns: {direct: [...], transitive: [...], total, cycles}
    """
    from src.db_scanner.models import DBObject

    with services.connection.cursor() as cur:
        obj_type = _resolve_obj_type(schema, object_name, cur)
        if obj_type is None:
            return jsonify({'error': 'Object not found', 'direct': [], 'transitive': [], 'total': 0})

    obj = DBObject(schema=schema, name=object_name, obj_type=obj_type, definition='')
    graph = services.scanner.build_dependency_graph(obj, max_depth=10, direction='upstream')

    central_id = f"{schema}.{object_name}"
    # Build adjacency for direct detection
    direct_ids = set()
    for lnk in graph['links']:
        if lnk['source'] == central_id:
            direct_ids.add(lnk['target'])
        elif lnk['target'] == central_id:
            direct_ids.add(lnk['source'])

    direct, transitive = [], []
    for node in graph['nodes']:
        if node['isCentral']:
            continue
        entry = {'schema': node['schema'], 'name': node['name'],
                 'type': node['type'], 'id': node['id']}
        if node['id'] in direct_ids:
            direct.append(entry)
        else:
            transitive.append(entry)

    return jsonify({
        'object':            central_id,
        'direct':            direct,
        'transitive':        transitive,
        'total':             len(direct) + len(transitive),
        'cycles':            graph.get('cycles', []),
    })


@app.route('/function/<schema>/<object_name>/outputs')
def get_function_outputs(schema, object_name):
    """
    Extract output columns from a function's signature via pg_catalog.
    Supports RETURNS TABLE(...), OUT/INOUT params, and RETURNS composite type.
    """
    import re as _re
    with services.connection.cursor() as cur:
        cur.execute("""
            SELECT
                p.proargnames,
                p.proargmodes,
                p.proretset,
                p.prorettype,
                pg_catalog.pg_get_function_result(p.oid) AS result_sig
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s
            LIMIT 1
        """, (schema, object_name))
        row = cur.fetchone()

    if not row:
        return jsonify({'error': 'Function not found', 'outputs': [], 'returns_set': False})

    argnames, argmodes, retset, rettype, result_sig = row
    outputs = []

    # Priority 1: OUT / INOUT / TABLE params from proargmodes
    if argmodes and argnames:
        for i, mode in enumerate(argmodes):
            if mode in ('o', 'b', 't'):
                name = argnames[i] if i < len(argnames) else f'col_{i}'
                if name:
                    outputs.append({'name': name, 'source': 'param'})

    # Priority 2: parse result_sig string "TABLE(col1 type1, col2 type2, ...)"
    if not outputs and result_sig:
        m = _re.match(r'TABLE\s*\((.+)\)\s*$', result_sig.strip(), _re.IGNORECASE | _re.DOTALL)
        if m:
            for col_def in _re.split(r',\s*(?=[a-zA-Z_])', m.group(1)):
                parts = col_def.strip().split()
                if parts:
                    outputs.append({'name': parts[0], 'pg_type': ' '.join(parts[1:]), 'source': 'signature'})

    # Priority 3: RETURNS composite type — expand via pg_attribute
    if not outputs and rettype:
        with services.connection.cursor() as cur:
            cur.execute("""
                SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
                FROM pg_type t
                JOIN pg_class c ON t.typrelid = c.oid
                JOIN pg_attribute a ON c.oid = a.attrelid
                WHERE t.oid = %s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (rettype,))
            for r in cur.fetchall():
                outputs.append({'name': r[0], 'pg_type': r[1], 'source': 'composite_type'})

    return jsonify({
        'function':     f'{schema}.{object_name}',
        'outputs':      outputs,
        'returns_set':  bool(retset),
        'result_sig':   result_sig or '',
    })


@app.route('/function/<schema>/<object_name>/inputs')
def get_function_inputs(schema, object_name):
    """
    Detect which columns of referenced tables/views are actually used
    in the function body (word-boundary text search with dot-qualifier context).
    """
    import re as _re

    with services.connection.cursor() as cur:
        cur.execute("""
            SELECT pg_get_functiondef(p.oid)
            FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s LIMIT 1
        """, (schema, object_name))
        row = cur.fetchone()

    if not row or not row[0]:
        return jsonify({'inputs': [], 'tables': [], 'error': 'Function body unavailable'})

    func_body = row[0]
    func_lower = func_body.lower()

    # Find all table/view references in the function body
    table_refs = services.scanner._extract_table_references(func_body, schema)

    inputs = []
    tables_used = []

    with services.connection.cursor() as cur:
        for ref in sorted(table_refs):
            parts = ref.split('.')
            s, t = (parts[0], parts[1]) if len(parts) == 2 else (schema, parts[0])

            if _is_system_object_name(s):
                continue

            # Confirm it's a real table/view
            cur.execute("""
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = %s AND c.relname = %s
                  AND c.relkind IN ('r','v','m') LIMIT 1
            """, (s, t))
            if not cur.fetchone():
                continue

            tables_used.append(f'{s}.{t}')

            # Get columns of this table/view
            cur.execute("""
                SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = %s AND c.relname = %s
                  AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (s, t))
            columns = [(r[0], r[1]) for r in cur.fetchall()]

            for col_name, col_type in columns:
                col_l = col_name.lower()
                # Dot-qualified reference: alias.col or schema.table (strong signal)
                dot_match  = bool(_re.search(rf'\.\s*{_re.escape(col_l)}\b', func_lower))
                # Standalone column name in SQL keyword context (weaker signal)
                sql_ctx    = bool(_re.search(
                    rf'\b(?:select|where|set|returning|group\s+by|order\s+by|having|on)\b'
                    rf'[^;]{{0,200}}\b{_re.escape(col_l)}\b',
                    func_lower, _re.DOTALL
                ))
                if not (dot_match or sql_ctx):
                    continue

                inputs.append({
                    'table_schema':  s,
                    'table_name':    t,
                    'table_fqn':     f'{s}.{t}',
                    'column':        col_name,
                    'pg_type':       col_type,
                    'confirmed':     dot_match,   # True = dot-qualified (strong signal)
                })

    # Deduplicate (same column might appear in multiple scan passes)
    seen, unique_inputs = set(), []
    for item in inputs:
        key = (item['table_fqn'], item['column'])
        if key not in seen:
            seen.add(key)
            unique_inputs.append(item)

    return jsonify({
        'function': f'{schema}.{object_name}',
        'inputs':   unique_inputs,
        'tables':   list(dict.fromkeys(tables_used)),  # ordered unique
    })


def _is_system_object_name(schema: str) -> bool:
    from src.db_scanner.scanner_recursive import _is_system_object
    return _is_system_object(schema)


def _get_object_ddl_text(schema: str, name: str) -> str:
    """Return raw DDL text for an object (used for text-search fallback)."""
    with services.connection.cursor() as cur:
        # VIEW / MAT_VIEW
        cur.execute("""
            SELECT pg_get_viewdef(c.oid, true)
            FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('v','m')
        """, (schema, name))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
        # FUNCTION
        cur.execute("""
            SELECT pg_get_functiondef(p.oid)
            FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s LIMIT 1
        """, (schema, name))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    return ''


@app.route('/impact/<schema>/<object_name>/column/<column>')
def get_column_impact(schema, object_name, column):
    """
    Simulate drop/rename of a specific column.

    Classification:
      breaking  — column lineage confirmed this dependent uses the column
      warning   — lineage unavailable/imprecise, but column name found in DDL text
                  (possible usage, not confirmed)
      (omitted) — dependent objects that do NOT reference this column at all
    """
    from src.db_scanner.models import DBObject

    with services.connection.cursor() as cur:
        obj_type = _resolve_obj_type(schema, object_name, cur)
        if obj_type is None:
            return jsonify({'error': 'Object not found', 'breaking': [], 'warning': [], 'total': 0})

    obj = DBObject(schema=schema, name=object_name, obj_type=obj_type, definition='')
    graph = services.scanner.build_dependency_graph(obj, max_depth=8, direction='upstream')

    breaking, warning = [], []
    col_lower     = column.lower()
    central_id    = f"{schema}.{object_name}"
    # Also match short name without schema prefix for text search
    table_name    = object_name.lower()

    for node in graph['nodes']:
        if node['isCentral']:
            continue

        dep_obj_type = _resolve_obj_type_str(node['type'])
        if dep_obj_type is None:
            continue

        dep_obj  = DBObject(schema=node['schema'], name=node['name'],
                            obj_type=dep_obj_type, definition='')
        col_data = services.col_scanner.get_column_lineage(dep_obj)
        lineage  = col_data.get('lineage', [])

        # ── Step 1: column lineage confirms direct usage ─────────────────────
        uses_col_lineage = any(
            r['source_col'].lower() == col_lower and
            (central_id.lower() in r['source_obj'].lower() or
             table_name in r['source_obj'].lower())
            for r in lineage
        )

        if uses_col_lineage:
            breaking.append({
                'schema': node['schema'], 'name': node['name'],
                'type':   node['type'],   'id':   node['id'],
                'detail': f"Column '{column}' confirmed via lineage",
                'method': 'lineage',
            })
            continue

        # ── Step 2: text fallback when lineage is empty/unavailable ─────────
        # Only apply fallback when lineage is empty due to parser limitations,
        # NOT when lineage ran successfully and returned rows for OTHER columns
        # (that would mean this column is simply not used).
        lineage_available = bool(lineage) and not col_data.get('error')
        if lineage_available:
            # Lineage ran OK but didn't find this column → object does NOT use it
            continue

        # Lineage was not available — use DDL text search as heuristic
        ddl_text = _get_object_ddl_text(node['schema'], node['name'])
        if not ddl_text:
            continue

        ddl_lower = ddl_text.lower()
        # Column name must appear AND the source table must be referenced
        col_in_ddl   = f'\\b{col_lower}\\b' if len(col_lower) > 2 else col_lower
        col_found    = col_lower in ddl_lower
        table_found  = (central_id.lower() in ddl_lower or
                        table_name in ddl_lower)

        if col_found and table_found:
            warning.append({
                'schema': node['schema'], 'name': node['name'],
                'type':   node['type'],   'id':   node['id'],
                'detail': f"Column '{column}' found in DDL text (lineage unavailable — verify manually)",
                'method': 'text_search',
            })
        # else: no evidence of column usage → omit from results

    return jsonify({
        'column':   column,
        'object':   central_id,
        'breaking': breaking,
        'warning':  warning,
        'total':    len(breaking) + len(warning),
    })


@app.route('/pipeline/<schema>/<object_name>')
def get_pipeline(schema, object_name):
    """
    Full pipeline view: upstream chain + downstream chain, with column lineage at each hop.
    Returns ordered chains and column alias evolution per attribute.
    """
    from src.db_scanner.models import DBObject

    with services.connection.cursor() as cur:
        obj_type = _resolve_obj_type(schema, object_name, cur)
        if obj_type is None:
            return jsonify({'error': 'Object not found'})

    central = DBObject(schema=schema, name=object_name, obj_type=obj_type, definition='')

    # Build full graph both directions
    graph = services.scanner.build_dependency_graph(central, max_depth=15, direction='both')

    central_id = f"{schema}.{object_name}"

    # Topological sort: find ordered chains upstream and downstream
    import networkx as nx
    G = nx.DiGraph()
    for lnk in graph['links']:
        G.add_edge(lnk['source'], lnk['target'])

    node_map = {n['id']: n for n in graph['nodes']}

    # Upstream: nodes from which central is reachable (ancestors)
    upstream_ids = nx.ancestors(G, central_id) if central_id in G else set()
    # Downstream: nodes reachable from central (descendants)
    downstream_ids = nx.descendants(G, central_id) if central_id in G else set()

    def chain_nodes(ids, reverse=False):
        sub = G.subgraph(ids | {central_id})
        try:
            ordered = list(nx.topological_sort(sub))
        except Exception:
            ordered = list(ids)
        result = []
        for nid in ordered:
            if nid in ids and nid in node_map:
                result.append(node_map[nid])
        if reverse:
            result.reverse()
        return result

    upstream_chain   = chain_nodes(upstream_ids, reverse=True)
    downstream_chain = chain_nodes(downstream_ids, reverse=False)

    # Get column lineage for central object
    central_cols = services.col_scanner.get_column_lineage(central)

    # Get column lineage for each hop to show alias evolution
    def get_hop_cols(node_info):
        try:
            hop_type = _resolve_obj_type_str(node_info['type'])
            if hop_type is None:
                return []
            hop_obj = DBObject(schema=node_info['schema'], name=node_info['name'],
                               obj_type=hop_type, definition='')
            d = services.col_scanner.get_column_lineage(hop_obj)
            return d.get('lineage', [])
        except Exception:
            return []

    # Build alias evolution chains for each target column of central
    alias_chains = []
    for row in (central_cols.get('lineage') or []):
        tgt_col = row['target_col']
        try:
            trace = services.alias_tracker.trace(schema, object_name, tgt_col, direction='both')
            if trace.get('total_hops', 0) > 0:
                alias_chains.append({
                    'column': tgt_col,
                    'path':   trace.get('path', []),
                    'renames': trace.get('renames', []),
                })
        except Exception:
            pass

    return jsonify({
        'central':         node_map.get(central_id, {'id': central_id, 'schema': schema,
                                                      'name': object_name, 'type': obj_type.name,
                                                      'isCentral': True}),
        'upstream_chain':  upstream_chain,
        'downstream_chain': downstream_chain,
        'central_columns':  central_cols,
        'alias_chains':    alias_chains,
        'stats': {
            'upstream_count':   len(upstream_chain),
            'downstream_count': len(downstream_chain),
            'column_count':     len(central_cols.get('columns', [])),
            'alias_chain_count': len(alias_chains),
        },
    })


def _resolve_obj_type_str(type_str: str):
    """Convert type string like 'TABLE' to ObjectType enum."""
    from src.db_scanner.models import ObjectType
    try:
        return ObjectType[type_str]
    except KeyError:
        return None


@app.route('/debug/refs/<schema>/<object_name>')
def debug_refs(schema, object_name):
    """Debug: show what _extract_table_references finds for a function DDL."""
    with services.connection.cursor() as cur:
        cur.execute("""
            SELECT pg_get_functiondef(p.oid)
            FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s
        """, (schema, object_name))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'function not found', 'schema': schema, 'name': object_name})
        func_sql = row[0]

    refs = services.scanner._extract_table_references(func_sql, schema)

    # Try to resolve each ref in the DB
    resolved = []
    with services.connection.cursor() as cur:
        for ref in sorted(refs):
            parts = ref.split('.')
            s, n = (parts[0], parts[1]) if len(parts) == 2 else (schema, parts[0])
            cur.execute("""
                SELECT 'TABLE' FROM pg_class c JOIN pg_namespace ns ON c.relnamespace=ns.oid
                WHERE ns.nspname=%s AND c.relname=%s AND c.relkind IN ('r','v','m')
                UNION ALL
                SELECT 'FUNCTION' FROM pg_proc p JOIN pg_namespace ns ON p.pronamespace=ns.oid
                WHERE ns.nspname=%s AND p.proname=%s LIMIT 1
            """, (s, n, s, n))
            hit = cur.fetchone()
            resolved.append({'ref': ref, 'schema': s, 'name': n,
                             'found_as': hit[0] if hit else None})

    return jsonify({
        'function':     f'{schema}.{object_name}',
        'body_length':  len(func_sql),
        'body_preview': func_sql[:800],
        'extracted_refs': sorted(refs),
        'resolved':     resolved,
    })


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5001)