"""
Copyright 2024-2026 Confluent, Inc.

Catalyst logical plan to Flink SQL: visitor that walks Spark's plan JSON
(from DataFrame._jdf.queryExecution().optimizedPlan().toJSON() or .logical().toJSON())
and produces Flink SQL.

Plan JSON can be a list of nodes (Spark flattens the tree; root is the last element)
or a single node dict with nested "child". See visit() for handling.
"""

from __future__ import annotations

import json
from typing import Any, Callable


class FlinkSQLGenerator:
    """
    Visitor that maps a Catalyst logical plan (as JSON) to Flink SQL parts
    and assembles a SELECT ... FROM ... WHERE ... GROUP BY statement.
    """

    def __init__(self) -> None:
        self.sql_parts: dict[str, Any] = {
            "select": [],
            "from": "",
            "where": [],
            "group_by": [],
            "order_by": [],
            "limit": None,
        }

    def visit(self, node: list[dict] | dict) -> None:
        """
        Recursively visit nodes in the Catalyst plan.
        node: either a list of plan nodes (Spark format; root is first, child is next index)
        or a single node dict with nested "child".
        """
        if isinstance(node, list):
            if not node:
                return
            # Spark plan list: root at index 0, child at current_index + 1 + child_value
            self._visit_at(node, 0)
            return
        if isinstance(node, dict):
            self._visit_node(node, lambda: None)
            return
        raise TypeError("node must be list of plan nodes or a single node dict")

    def _visit_at(self, plan: list[dict], index: int) -> None:
        """Visit the node at plan[index] and recurse to its child (next index)."""
        node = plan[index]
        child_ref = node.get("child")

        def recurse() -> None:
            # child: 0 typically means "next node in list"
            if child_ref is not None and isinstance(child_ref, int):
                next_index = index + 1 + child_ref
                if 0 <= next_index < len(plan):
                    self._visit_at(plan, next_index)

        self._visit_node(node, recurse)

    def _visit_node(self, node: dict, recurse: Callable[[], None]) -> None:
        """Dispatch by node class and call recurse to go to child."""
        class_name = node.get("class", "") or node.get("nodeName", "")
        if "Aggregate" in class_name:
            self._visit_aggregate(node)
            recurse()
        elif "Filter" in class_name:
            self._visit_filter(node)
            recurse()
        elif "Project" in class_name:
            self._visit_project(node)
            recurse()
        elif "LogicalRelation" in class_name:
            self._visit_relation(node)
        elif "LogicalRDD" in class_name:
            self._visit_logical_rdd(node)
        else:
            recurse()

    def _visit_relation(self, node: dict) -> None:
        """Extract table name from LogicalRelation (catalog/identifier)."""
        # CatalogRelation or similar may have identifier
        ident = node.get("identifier") or node.get("table")
        if isinstance(ident, list):
            self.sql_parts["from"] = ".".join(str(x) for x in ident)
        elif isinstance(ident, str):
            self.sql_parts["from"] = ident
        else:
            self.sql_parts["from"] = "default.ecommerce_events"

    def _visit_logical_rdd(self, node: dict) -> None:
        """LogicalRDD is from createDataFrame/tempView; use view name if we have it."""
        if not self.sql_parts["from"]:
            self.sql_parts["from"] = "default.ecommerce_events"

    def _visit_filter(self, node: dict) -> None:
        """Extract the condition (e.g. event_name = 'purchase')."""
        condition = node.get("condition")
        if condition is None:
            return
        if isinstance(condition, list):
            # Use fallback first; Spark condition list often has root at 0 with self-refs
            cond_str = self._parse_condition_fallback(condition)
            if not cond_str:
                cond_str = self._parse_expression_list(condition)
            if cond_str:
                self.sql_parts["where"].append(cond_str)
        else:
            cond_str = self._parse_expression(condition)
            if cond_str:
                self.sql_parts["where"].append(cond_str)

    def _parse_condition_fallback(self, exprs: list) -> str:
        """Fallback: find EqualTo(AttributeReference, Literal). Spark may use relative refs (i+1+left)."""
        if not exprs:
            return ""
        for i, e in enumerate(exprs):
            if not isinstance(e, dict) or "EqualTo" not in (e.get("class") or ""):
                continue
            left_ref = e.get("left")
            right_ref = e.get("right")
            if left_ref is None or right_ref is None or not isinstance(left_ref, int) or not isinstance(right_ref, int):
                continue
            # Try absolute indices first, then relative (i+1+ref)
            for left_i, right_i in [
                (left_ref, right_ref),
                (i + 1 + left_ref, i + 1 + right_ref),
            ]:
                if left_i < 0 or left_i >= len(exprs) or right_i < 0 or right_i >= len(exprs):
                    continue
                left_node = exprs[left_i]
                right_node = exprs[right_i]
                if not isinstance(left_node, dict) or not isinstance(right_node, dict):
                    continue
                lcls = left_node.get("class") or ""
                rcls = right_node.get("class") or ""
                if "AttributeReference" in lcls and "Literal" in rcls:
                    name = left_node.get("name", "?")
                    val = right_node.get("value")
                    lit = f"'{val}'" if isinstance(val, str) else str(val)
                    return f"{name} = {lit}"
                if "Literal" in lcls and "AttributeReference" in rcls:
                    val = left_node.get("value")
                    lit = f"'{val}'" if isinstance(val, str) else str(val)
                    name = right_node.get("name", "?")
                    return f"{name} = {lit}"
        return ""

    def _visit_aggregate(self, node: dict) -> None:
        """Extract grouping keys and aggregate expressions."""
        grouping = node.get("groupingExpressions") or []
        agg_exprs = node.get("aggregateExpressions") or []
        for g in grouping:
            if isinstance(g, list):
                s = self._parse_expression_list(g)
            else:
                s = self._parse_expression(g)
            if s and s not in self.sql_parts["group_by"]:
                self.sql_parts["group_by"].append(s)
        for a in agg_exprs:
            if isinstance(a, list):
                s = self._parse_expression_list(a)
            else:
                s = self._parse_expression(a)
            if s and s not in self.sql_parts["select"]:
                self.sql_parts["select"].append(s)

    def _visit_project(self, node: dict) -> None:
        """Project lists output columns; use for SELECT when no Aggregate above."""
        project_list = node.get("projectList") or []
        for p in project_list:
            if isinstance(p, list):
                s = self._parse_expression_list(p)
            else:
                s = self._parse_expression(p)
            if s and s not in self.sql_parts["select"]:
                self.sql_parts["select"].append(s)

    def _parse_expression_list(self, exprs: list) -> str:
        """
        Parse a Spark expression list. Spark flattens the tree: refs (left, right, child)
        are indices into this list. Root can be first (index 0) or last (index -1)
        depending on serialization; we try last as root first (common for condition).
        """
        if not exprs:
            return ""
        resolved: dict[int, str] = {}

        def resolve_ref(i: int, ref: int, node_index: int) -> int:
            """Resolve ref: if ref points to self (node_index), use relative i+1+ref."""
            if ref == node_index:
                return node_index + 1 + ref
            if 0 <= ref < len(exprs):
                return ref
            return node_index + 1 + ref

        def parse_at(i: int) -> str:
            if i in resolved:
                return resolved[i]
            if i < 0 or i >= len(exprs):
                return ""
            expr = exprs[i]
            if not isinstance(expr, dict):
                resolved[i] = str(expr)
                return resolved[i]
            class_name = expr.get("class", "") or expr.get("nodeName", "")
            out = ""
            if "AttributeReference" in class_name:
                out = expr.get("name", "?")
            elif "Literal" in class_name:
                val = expr.get("value")
                if isinstance(val, str):
                    out = f"'{val}'"
                else:
                    out = str(val) if val is not None else "NULL"
            elif "EqualTo" in class_name:
                left_i = expr.get("left")
                right_i = expr.get("right")
                if left_i is not None and right_i is not None and 0 <= left_i < len(exprs) and 0 <= right_i < len(exprs):
                    l = parse_at(left_i)
                    r = parse_at(right_i)
                    out = f"{l} = {r}"
            elif "Alias" in class_name:
                child_ref = expr.get("child")
                name = expr.get("name", "")
                if child_ref is not None:
                    child_i = resolve_ref(i, child_ref, i)
                    inner = parse_at(child_i)
                    out = f"{inner} AS {name}" if name else inner
            elif "Count" in class_name:
                out = "COUNT(1)"
            elif "AggregateExpression" in class_name:
                agg_ref = expr.get("aggregateFunction")
                if agg_ref is not None:
                    # aggregateFunction is typically relative (next node in list)
                    agg_i = i + 1 + agg_ref
                    if 0 <= agg_i < len(exprs):
                        out = parse_at(agg_i)
                    else:
                        out = "COUNT(1)"
                else:
                    out = "COUNT(1)"
            elif "And" in class_name:
                left_i = expr.get("left")
                right_i = expr.get("right")
                if left_i is not None and right_i is not None and 0 <= left_i < len(exprs) and 0 <= right_i < len(exprs):
                    l = parse_at(left_i)
                    r = parse_at(right_i)
                    out = f"({l} AND {r})" if l and r else (l or r)
            elif "IsNotNull" in class_name:
                child_ref = expr.get("child")
                if child_ref is not None:
                    child_i = resolve_ref(i, child_ref, i)
                    c = parse_at(child_i)
                    out = f"{c} IS NOT NULL"
            if out:
                resolved[i] = out
            return resolved.get(i, "")

        # Spark expression list: root can be first (index 0) or last. Try 0 first for condition.
        root_index = 0
        return parse_at(root_index)

    def _parse_expression(self, expr_node: dict) -> str:
        """Map a single Catalyst expression node to a Flink SQL expression string."""
        if not expr_node or not isinstance(expr_node, dict):
            return ""
        class_name = expr_node.get("class", "") or expr_node.get("nodeName", "")
        if "AttributeReference" in class_name:
            return expr_node.get("name", "?")
        if "Literal" in class_name:
            val = expr_node.get("value")
            if isinstance(val, str):
                return f"'{val}'"
            return str(val) if val is not None else "NULL"
        if "EqualTo" in class_name:
            left = self._parse_expression(expr_node.get("left") or {})
            right = self._parse_expression(expr_node.get("right") or {})
            return f"{left} = {right}" if left and right else ""
        if "Alias" in class_name:
            child = expr_node.get("child")
            name = expr_node.get("name", "")
            inner = self._parse_expression(child) if isinstance(child, dict) else ""
            return f"{inner} AS {name}" if inner and name else inner
        if "Count" in class_name:
            return "COUNT(1)"
        return ""

    def generate_sql(self) -> str:
        """Assemble the mapped parts into a Flink SQL string."""
        select = self.sql_parts["select"]
        from_clause = self.sql_parts["from"]
        where = self.sql_parts["where"]
        group_by = self.sql_parts["group_by"]

        if not from_clause:
            from_clause = "default.ecommerce_events"
        select_str = ", ".join(select) if select else "*"
        sql = f"SELECT {select_str}\n"
        sql += f"FROM {from_clause}\n"
        if where:
            sql += f"WHERE {' AND '.join(where)}\n"
        if group_by:
            sql += f"GROUP BY {', '.join(group_by)}"
        if not sql.strip().endswith(";"):
            sql += ";"
        return sql


def catalyst_plan_to_flink_sql(plan_json: str | list | dict) -> str:
    """
    Convenience: parse plan (if string), run FlinkSQLGenerator, return Flink SQL.
    """
    if isinstance(plan_json, str):
        plan = json.loads(plan_json)
    else:
        plan = plan_json
    gen = FlinkSQLGenerator()
    gen.visit(plan)
    return gen.generate_sql()
