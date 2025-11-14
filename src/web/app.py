"""
Database Object Dependency Graph Scanner - Web Application
Copyright (c) 2025 Dmitry Solonnikov
Licensed under MIT License - see LICENSE file for details
"""

from flask import Flask, render_template, jsonify
from src.db_scanner.scanner_recursive import DBScanner
from src.db.connection import DBConnection
import os
import json

app = Flask(__name__)

# Инициализация подключения к БД
connection = DBConnection()
scanner = DBScanner(connection)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/schemas')
def get_schemas():
    """Получение списка схем"""
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT nspname 
            FROM pg_namespace 
            WHERE nspname NOT LIKE 'pg_%' 
            AND nspname != 'information_schema'
        """)
        schemas = [row[0] for row in cur.fetchall()]
    return jsonify(schemas)

@app.route('/objects/<schema>')
def get_objects(schema):
    """Получение списка объектов в схеме"""
    with connection.cursor() as cur:
        cur.execute("""
            SELECT 
                c.relname as name,
                CASE c.relkind
                    WHEN 'r' THEN 'TABLE'
                    WHEN 'v' THEN 'VIEW'
                    WHEN 'm' THEN 'MATERIALIZED_VIEW'
                    WHEN 'f' THEN 'FOREIGN_TABLE'
                    WHEN 'p' THEN 'PARTITIONED_TABLE'
                END as type
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
            AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
            UNION
            SELECT 
                p.proname as name,
                'FUNCTION' as type
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s
            ORDER BY name
        """, (schema, schema))
        objects = [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
    return jsonify(objects)

@app.route('/graph/<schema>/<object_name>')
def get_graph(schema, object_name):
    """Получение данных для графа зависимостей"""
    # Создаем центральный объект
    from src.db_scanner.models import DBObject, ObjectType

    # Определяем тип объекта
    with connection.cursor() as cur:
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
        """, (schema, object_name, schema, object_name))

        result = cur.fetchone()
        if not result:
            return jsonify({"nodes": [], "links": []})

        obj_type = ObjectType[result[0]]

    central_obj = DBObject(
        schema=schema,
        name=object_name,
        obj_type=obj_type,
        definition=''
    )

    # Рекурсивно строим дерево зависимостей
    nodes = []
    links = []
    node_ids = set()
    processed_objects = set()

    def add_node(obj):
        node_id = f"{obj.schema}.{obj.name}"
        if node_id not in node_ids:
            nodes.append({
                "id": node_id,
                "name": obj.name,
                "schema": obj.schema,
                "type": obj.obj_type.name,
                "isCentral": node_id == central_node_id
            })
            node_ids.add(node_id)

    def build_full_dependency_tree(obj, max_depth=10, current_depth=0):
        """Рекурсивно строит полное дерево зависимостей, исследуя каждый узел"""
        obj_key = (obj.schema, obj.name)
        if current_depth >= max_depth or obj_key in processed_objects:
            return

        processed_objects.add(obj_key)

        # Получаем прямые зависимости объекта
        obj_deps = scanner.find_related_objects(obj)

        for dep in obj_deps:
            source_id = f"{dep.source.schema}.{dep.source.name}"
            target_id = f"{dep.target.schema}.{dep.target.name}"

            add_node(dep.source)
            add_node(dep.target)

            # Создаем связи в правильном направлении
            if dep.relationship_type == 'depends_on':
                # source зависит от target
                links.append({
                    "source": source_id,
                    "target": target_id,
                    "type": dep.relationship_type,
                    "depth": dep.depth
                })
            elif dep.relationship_type == 'used_by':
                # source используется target'ом
                links.append({
                    "source": target_id,
                    "target": source_id,
                    "type": dep.relationship_type,
                    "depth": dep.depth
                })

        # Важно: исследуем зависимости каждого найденного объекта рекурсивно
        for dep in obj_deps:
            # Исследуем target объект (зависимый объект)
            if (dep.target.schema, dep.target.name) not in processed_objects:
                build_full_dependency_tree(dep.target, max_depth, current_depth + 1)

            # Исследуем source объект (родительский объект)
            if (dep.source.schema, dep.source.name) not in processed_objects:
                build_full_dependency_tree(dep.source, max_depth, current_depth + 1)

    central_node_id = f"{schema}.{object_name}"
    add_node(central_obj)
    build_full_dependency_tree(central_obj, max_depth=10)

    return jsonify({"nodes": nodes, "links": links})

@app.route('/ddl/<schema>/<object_name>')
def get_ddl(schema, object_name):
    """Получение DDL объекта"""
    with connection.cursor() as cur:
        # Сначала пытаемся получить определение как обычного объекта
        cur.execute("""
            SELECT pg_get_viewdef(c.oid, true) as definition
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            AND c.relkind IN ('v', 'm')
            UNION
            SELECT pg_get_tabledef(c.oid) as definition
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            AND c.relkind = 'r'
            UNION
            SELECT pg_get_functiondef(p.oid) as definition
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = %s AND p.proname = %s
        """, (schema, object_name, schema, object_name, schema, object_name))
        
        result = cur.fetchone()
        if result:
            return result[0]
        return "DDL not available"

if __name__ == '__main__':
    app.run(debug=True)