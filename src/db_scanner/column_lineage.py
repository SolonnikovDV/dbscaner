"""
Column-level lineage scanner using sqllineage.
Copyright (c) 2025 Dmitry Solonnikov
Licensed under MIT License - see LICENSE file for details
"""
from typing import List, Dict, Optional
import re

from .models import DBObject, ObjectType


class ColumnLineageScanner:
    """
    Extracts column-level lineage from SQL object definitions (VIEW, MATERIALIZED VIEW, FUNCTION).

    Returns per-column source → target mappings parsed from the object's DDL body.
    Falls back to regex extraction for PL/pgSQL function bodies where sqllineage
    cannot reach the SQL statements directly.
    """

    def __init__(self, conn):
        self.conn = conn

    # ── Public API ────────────────────────────────────────────────────────────

    def get_column_lineage(self, obj: DBObject) -> Dict:
        """
        Return column lineage for the given object.

        Result schema:
        {
            "object":        "<schema>.<name>",
            "object_type":   str,
            "columns":       List[str],          # all column names involved
            "lineage":       List[{
                "source_col": str,
                "source_obj": str,
                "target_col": str,
                "target_obj": str,
            }],
            "source_tables": List[str],
            "target_tables": List[str],
            "error":         str | None,
        }
        """
        base = {
            "object":        f"{obj.schema}.{obj.name}",
            "object_type":   obj.obj_type.name,
            "columns":       [],
            "lineage":       [],
            "source_tables": [],
            "target_tables": [],
            "error":         None,
        }

        if obj.obj_type == ObjectType.TABLE:
            # Tables are leaf nodes — return their column list only
            cols = self._get_table_columns(obj.schema, obj.name)
            base["columns"] = cols
            base["source_tables"] = [f"{obj.schema}.{obj.name}"]
            return base

        try:
            raw_sql = self._fetch_object_sql(obj)
            if not raw_sql:
                base["error"] = "No SQL definition found for this object."
                # Try pg_depend fallback for views
                if obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                    fallback = self._pg_depend_fallback(obj)
                    if fallback["lineage"]:
                        fallback["error"] = "sqllineage unavailable — showing table-level lineage via pg_depend"
                        base.update(fallback)
                return base

            normalized = self._normalize_for_lineage(raw_sql, obj)
            if not normalized or not normalized.strip():
                # pg_depend fallback
                fallback = self._pg_depend_fallback(obj)
                if fallback["lineage"]:
                    fallback["error"] = "Could not parse SQL body — showing table-level lineage via pg_depend"
                    base.update(fallback)
                else:
                    base["error"] = "Could not extract parseable SQL from this object."
                return base

            result = self._run_sqllineage(normalized, obj)
            # If sqllineage parsed OK but returned no lineage, try pg_depend fallback
            if not result["lineage"] and obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                fallback = self._pg_depend_fallback(obj)
                if fallback["lineage"]:
                    fallback["error"] = "sqllineage returned no columns — showing table-level lineage via pg_depend"
                    base.update(fallback)
                    return base
            base.update(result)
            return base

        except Exception as exc:
            base["error"] = f"Lineage extraction failed: {exc}"
            return base

    def get_table_columns(self, schema: str, name: str) -> List[str]:
        """Return ordered list of column names for a table or view."""
        return self._get_table_columns(schema, name)

    # ── Internal: fetch DDL ───────────────────────────────────────────────────

    def _fetch_object_sql(self, obj: DBObject) -> Optional[str]:
        with self.conn.cursor() as cur:
            if obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                cur.execute(
                    "SELECT pg_get_viewdef(c.oid, true) "
                    "FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid "
                    "WHERE n.nspname = %s AND c.relname = %s",
                    (obj.schema, obj.name),
                )
            elif obj.obj_type == ObjectType.FUNCTION:
                cur.execute(
                    "SELECT pg_get_functiondef(p.oid) "
                    "FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid "
                    "WHERE n.nspname = %s AND p.proname = %s",
                    (obj.schema, obj.name),
                )
            else:
                return None

            row = cur.fetchone()
            return row[0] if row else None

    def _get_table_columns(self, schema: str, name: str) -> List[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT attname
                FROM pg_attribute
                JOIN pg_class ON attrelid = pg_class.oid
                JOIN pg_namespace ON relnamespace = pg_namespace.oid
                WHERE nspname = %s AND relname = %s
                  AND attnum > 0 AND NOT attisdropped
                ORDER BY attnum
                """,
                (schema, name),
            )
            return [row[0] for row in cur.fetchall()]

    # ── Internal: SQL normalization ───────────────────────────────────────────

    def _normalize_for_lineage(self, sql: str, obj: DBObject) -> str:
        """Wrap raw object SQL into a form sqllineage can parse."""
        if obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
            # pg_get_viewdef returns the SELECT body only
            body = sql.strip().rstrip(";")
            return f"CREATE VIEW {obj.schema}.{obj.name} AS {body};"

        if obj.obj_type == ObjectType.FUNCTION:
            return self._extract_sql_from_function(sql, obj)

        return sql

    def _extract_sql_from_function(self, func_def: str, obj: DBObject) -> str:
        """
        Extract SQL statements from a PL/pgSQL function body.
        Wraps RETURN QUERY SELECT patterns as INSERT INTO ... SELECT for sqllineage.
        """
        # Try common dollar-quote delimiters
        body_patterns = [
            r"AS\s+\$\$\s*(.*?)\s*\$\$",
            r"AS\s+\$BODY\$\s*(.*?)\s*\$BODY\$",
            r"AS\s+'(.*?)'(?:\s*;)",
        ]
        body = None
        for pat in body_patterns:
            m = re.search(pat, func_def, re.IGNORECASE | re.DOTALL)
            if m:
                body = m.group(1)
                break

        if not body:
            return ""

        stmts: List[str] = []
        target = f"{obj.schema}.{obj.name}_result"

        # RETURN QUERY SELECT … → wrap as INSERT INTO … SELECT
        for m in re.finditer(
            r"RETURN\s+QUERY\s+(SELECT\b[^;]+)",
            body, re.IGNORECASE | re.DOTALL
        ):
            stmts.append(f"INSERT INTO {target} {m.group(1).strip()}")

        if stmts:
            return ";\n".join(stmts) + ";"

        # Fallback: bare SELECT statements
        for m in re.finditer(
            r"\b(SELECT\b[^;]+)",
            body, re.IGNORECASE | re.DOTALL
        ):
            stmts.append(m.group(1).strip())

        return (";\n".join(stmts) + ";") if stmts else ""

    # ── Internal: run sqllineage ──────────────────────────────────────────────

    def _pg_depend_fallback(self, obj: DBObject) -> Dict:
        """
        When sqllineage cannot parse the SQL, fall back to pg_depend to get
        at least the table-level source objects and their columns.
        """
        source_tables: List[str] = []
        with self.conn.cursor() as cur:
            if obj.obj_type in (ObjectType.VIEW, ObjectType.MATERIALIZED_VIEW):
                cur.execute("""
                    SELECT DISTINCT n.nspname || '.' || c.relname
                    FROM pg_rewrite rw
                    JOIN pg_class src ON rw.ev_class = src.oid
                    JOIN pg_depend d ON d.objid = rw.oid
                    JOIN pg_class c ON d.refobjid = c.oid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_namespace srcn ON src.relnamespace = srcn.oid
                    WHERE srcn.nspname = %s AND src.relname = %s
                      AND d.deptype = 'n' AND c.relkind IN ('r','v','m')
                """, (obj.schema, obj.name))
                source_tables = [r[0] for r in cur.fetchall()]

        if not source_tables:
            return {"columns": [], "lineage": [], "source_tables": [], "target_tables": [], "error": None}

        # Get columns for each source table
        lineage_pairs: List[Dict] = []
        all_cols: set = set()
        with self.conn.cursor() as cur:
            for fqn in source_tables:
                parts = fqn.split('.', 1)
                s, t = (parts[0], parts[1]) if len(parts) == 2 else (obj.schema, parts[0])
                cur.execute("""
                    SELECT attname FROM pg_attribute
                    JOIN pg_class ON attrelid = pg_class.oid
                    JOIN pg_namespace ON relnamespace = pg_namespace.oid
                    WHERE nspname = %s AND relname = %s
                      AND attnum > 0 AND NOT attisdropped
                    ORDER BY attnum
                """, (s, t))
                for (col,) in cur.fetchall():
                    all_cols.add(col)
                    lineage_pairs.append({
                        "source_col": col,
                        "source_obj": fqn,
                        "target_col": col,
                        "target_obj": f"{obj.schema}.{obj.name}",
                    })

        return {
            "columns":       sorted(all_cols),
            "lineage":       lineage_pairs,
            "source_tables": source_tables,
            "target_tables": [f"{obj.schema}.{obj.name}"],
            "error":         None,
        }

    def _run_sqllineage(self, sql: str, obj: DBObject) -> Dict:
        from sqllineage.runner import LineageRunner

        runner = LineageRunner(sql, dialect="non_validating")

        lineage_pairs: List[Dict] = []
        column_names: set = set()

        for col_chain in runner.get_column_lineage():
            if len(col_chain) < 2:
                continue

            src_col = col_chain[0]
            tgt_col = col_chain[-1]

            src_name = self._col_name(src_col)
            tgt_name = self._col_name(tgt_col)
            src_obj  = self._col_table(src_col, "unknown")
            tgt_obj  = self._col_table(tgt_col, f"{obj.schema}.{obj.name}")

            lineage_pairs.append({
                "source_col": src_name,
                "source_obj": src_obj,
                "target_col": tgt_name,
                "target_obj": tgt_obj,
            })
            column_names.add(src_name)
            column_names.add(tgt_name)

        return {
            "columns":       sorted(column_names),
            "lineage":       lineage_pairs,
            "source_tables": [str(t) for t in runner.source_tables()],
            "target_tables": [str(t) for t in runner.target_tables()],
            "error":         None,
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _col_name(col) -> str:
        if hasattr(col, "raw_name"):
            return str(col.raw_name)
        name = str(col)
        # sqllineage may return "table.column" — keep only column part
        return name.split(".")[-1] if "." in name else name

    @staticmethod
    def _col_table(col, fallback: str) -> str:
        try:
            tbl = col.source_table
            if tbl is not None:
                return str(tbl)
        except AttributeError:
            pass
        # fallback: try to extract table from "table.column" string
        s = str(col)
        if "." in s:
            return s.rsplit(".", 1)[0]
        return fallback
