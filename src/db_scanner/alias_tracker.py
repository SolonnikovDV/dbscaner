"""
Attribute alias tracker — traces a column name through a chain of DB objects.

Algorithm:
  1. Start from (schema, object, column).
  2. Use ColumnLineageScanner to get the column lineage of the object.
  3. Find mappings where target_col == column  → follow source_col in source_obj.
  4. Find mappings where source_col == column  → follow target_col in target_obj.
  5. Repeat recursively (BFS) up to max_depth hops.
  6. Return an ordered path: [{step, obj, col, alias_of, direction}]

Copyright (c) 2025 Dmitry Solonnikov — MIT License
"""
from __future__ import annotations

from typing import List, Dict, Optional, Tuple, Set
from collections import deque

from .models import DBObject, ObjectType
from .column_lineage import ColumnLineageScanner


class AliasTracker:
    """
    Trace how a single column/attribute moves and is renamed across
    a chain of database objects (views, materialized views, functions).

    Each step in the returned path describes one hop:
      - which object holds this column alias
      - what the column is called in that object
      - what it was called in the previous object (alias_of)
      - direction: 'upstream' (toward source tables) or 'downstream' (toward consumers)
    """

    def __init__(self, conn, max_depth: int = 15):
        self.conn = conn
        self.col_scanner = ColumnLineageScanner(conn)
        self.max_depth = max_depth

    # ── Public API ────────────────────────────────────────────────────────────

    def trace(
        self,
        schema: str,
        object_name: str,
        column: str,
        direction: str = "both",   # 'upstream' | 'downstream' | 'both'
    ) -> Dict:
        """
        Trace column lineage starting from (schema, object_name, column).

        Returns:
        {
            "start": {"schema": ..., "object": ..., "column": ...},
            "direction": ...,
            "path": [
                {
                    "step":      int,        # hop number (0 = start)
                    "schema":    str,
                    "object":    str,
                    "obj_type":  str,
                    "column":    str,        # column name in THIS object
                    "alias_of":  str | None, # column name in the PREVIOUS object
                    "direction": str,        # 'start' | 'upstream' | 'downstream'
                }
            ],
            "renames": [          # only hops where column name actually changed
                {"from_col": ..., "from_obj": ..., "to_col": ..., "to_obj": ...}
            ],
            "total_hops": int,
            "error": str | None,
        }
        """
        result: Dict = {
            "start":      {"schema": schema, "object": object_name, "column": column},
            "direction":  direction,
            "path":       [],
            "renames":    [],
            "total_hops": 0,
            "error":      None,
        }

        try:
            obj_type = self._get_object_type(schema, object_name)
            if obj_type is None:
                result["error"] = f"Object {schema}.{object_name} not found."
                return result

            start_step = {
                "step":      0,
                "schema":    schema,
                "object":    object_name,
                "obj_type":  obj_type,
                "column":    column,
                "alias_of":  None,
                "direction": "start",
            }
            result["path"].append(start_step)

            visited: Set[Tuple[str, str, str]] = {(schema, object_name, column)}

            if direction in ("upstream", "both"):
                self._bfs(
                    schema, object_name, column,
                    mode="upstream",
                    visited=visited,
                    path=result["path"],
                    start_step=0,
                )

            if direction in ("downstream", "both"):
                self._bfs(
                    schema, object_name, column,
                    mode="downstream",
                    visited=visited,
                    path=result["path"],
                    start_step=0,
                )

            # Collect renames
            result["renames"] = self._extract_renames(result["path"])
            result["total_hops"] = len(result["path"]) - 1

        except Exception as exc:
            result["error"] = str(exc)

        return result

    # ── BFS traversal ─────────────────────────────────────────────────────────

    def _bfs(
        self,
        start_schema: str,
        start_object: str,
        start_col: str,
        mode: str,              # 'upstream' | 'downstream'
        visited: Set[Tuple],
        path: List[Dict],
        start_step: int,
    ) -> None:
        queue: deque = deque()
        queue.append((start_schema, start_object, start_col, start_step, start_col))

        while queue:
            schema, obj_name, col, _, __ = queue.popleft()

            obj_type = self._get_object_type(schema, obj_name)
            if obj_type is None:
                continue

            obj = DBObject(schema=schema, name=obj_name, obj_type=obj_type, definition="")
            lineage_data = self.col_scanner.get_column_lineage(obj)

            if lineage_data.get("error") and not lineage_data.get("lineage"):
                continue

            lineage = lineage_data.get("lineage", [])

            # For downstream traversal: TABLE and FUNCTION leaf nodes have empty lineage.
            # In this case we must look at CONSUMERS' lineage to find where
            # this column flows next.
            if mode == "downstream" and not lineage:
                next_hops = self._find_downstream_from_source(schema, obj_name, col)
            else:
                next_hops = self._find_next_hops(col, lineage, mode)

            for next_col, next_obj_fqn in next_hops:
                next_schema, next_obj_name = self._parse_fqn(next_obj_fqn, schema)
                key = (next_schema, next_obj_name, next_col)

                if key in visited:
                    continue
                if len(path) >= self.max_depth + 1:
                    continue

                visited.add(key)
                next_obj_type = self._get_object_type(next_schema, next_obj_name) or "UNKNOWN"
                step_num = len(path)

                step: Dict = {
                    "step":      step_num,
                    "schema":    next_schema,
                    "object":    next_obj_name,
                    "obj_type":  next_obj_type,
                    "column":    next_col,
                    "alias_of":  col if next_col != col else None,
                    "direction": mode,
                }
                path.append(step)
                queue.append((next_schema, next_obj_name, next_col, step_num, col))

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _find_downstream_from_source(
        self,
        schema: str,
        obj_name: str,
        col: str,
    ) -> List[Tuple[str, str]]:
        """
        Find the next downstream hops for a TABLE or FUNCTION leaf node.

        When a source object has no column lineage (it IS the source),
        we must look at objects that CONSUME it and check their lineage
        to see where `col` flows.

        Returns list of (next_col_name, consumer_fqn).
        """
        col_lower  = col.lower()
        src_fqn    = f"{schema}.{obj_name}".lower()
        src_name   = obj_name.lower()
        results: List[Tuple[str, str]] = []

        try:
            with self.conn.cursor() as cur:
                # Find views / mat-views that directly reference this object
                cur.execute("""
                    SELECT DISTINCT srcn.nspname, src.relname
                    FROM pg_rewrite rw
                    JOIN pg_class src ON rw.ev_class = src.oid
                    JOIN pg_depend d  ON d.objid = rw.oid
                    JOIN pg_class c   ON d.refobjid = c.oid
                    JOIN pg_namespace n    ON n.oid = c.relnamespace
                    JOIN pg_namespace srcn ON src.relnamespace = srcn.oid
                    WHERE n.nspname  = %s
                      AND c.relname  = %s
                      AND d.deptype  = 'n'
                      AND src.relkind IN ('v', 'm')
                """, (schema, obj_name))
                consumers = cur.fetchall()

            for cons_schema, cons_name in consumers:
                cons_type = self._get_object_type(cons_schema, cons_name)
                if cons_type is None:
                    continue
                cons_obj     = DBObject(
                    schema=cons_schema, name=cons_name,
                    obj_type=cons_type, definition=""
                )
                cons_lineage = self.col_scanner.get_column_lineage(cons_obj)
                for row in cons_lineage.get("lineage", []):
                    src_match = (
                        src_fqn in row["source_obj"].lower() or
                        src_name in row["source_obj"].lower()
                    )
                    if row["source_col"].lower() == col_lower and src_match:
                        results.append(
                            (row["target_col"], f"{cons_schema}.{cons_name}")
                        )
        except Exception as exc:
            print(f"[AliasTracker] _find_downstream_from_source error: {exc}")

        return results

    def _find_next_hops(
        self,
        column: str,
        lineage: List[Dict],
        mode: str,
    ) -> List[Tuple[str, str]]:
        """
        Given a column name and lineage mappings, return next (col, obj) pairs.

        upstream   → find rows where target_col == column → follow (source_col, source_obj)
        downstream → find rows where source_col == column → follow (target_col, target_obj)
        """
        results = []
        col_lower = column.lower()

        for row in lineage:
            if mode == "upstream":
                if row["target_col"].lower() == col_lower:
                    results.append((row["source_col"], row["source_obj"]))
            else:  # downstream
                if row["source_col"].lower() == col_lower:
                    results.append((row["target_col"], row["target_obj"]))

        return results

    def _get_object_type(self, schema: str, name: str) -> Optional[str]:
        """Two separate queries — Greenplum UNION ALL can reorder operands."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = %s AND p.proname = %s LIMIT 1
            """, (schema, name))
            if cur.fetchone() is not None:
                return 'FUNCTION'
            cur.execute("""
                SELECT CASE c.relkind
                    WHEN 'r' THEN 'TABLE' WHEN 'v' THEN 'VIEW'
                    WHEN 'm' THEN 'MATERIALIZED_VIEW'
                END
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = %s AND c.relname = %s
                  AND c.relkind IN ('r', 'v', 'm') LIMIT 1
            """, (schema, name))
            row = cur.fetchone()
            return row[0] if row else None

    @staticmethod
    def _parse_fqn(fqn: str, default_schema: str) -> Tuple[str, str]:
        """Parse 'schema.name' or just 'name' into (schema, name)."""
        parts = fqn.split(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return default_schema, parts[0]

    @staticmethod
    def _extract_renames(path: List[Dict]) -> List[Dict]:
        """Return only hops where the column name actually changed."""
        renames = []
        for i in range(1, len(path)):
            prev = path[i - 1]
            curr = path[i]
            if curr["alias_of"] and curr["alias_of"].lower() != curr["column"].lower():
                renames.append({
                    "from_col": curr["alias_of"],
                    "from_obj": f"{prev['schema']}.{prev['object']}",
                    "to_col":   curr["column"],
                    "to_obj":   f"{curr['schema']}.{curr['object']}",
                    "step":     curr["step"],
                })
        return renames
