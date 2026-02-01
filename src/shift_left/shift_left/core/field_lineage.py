"""
Copyright 2024-2025 Confluent, Inc.

Field-level lineage: compute column lineage from a DML sink table back to sources,
crawl parent hierarchy, and persist metadata plus interactive graph.
"""
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Any, Optional

from pydantic import BaseModel, Field

from shift_left.core.utils.app_config import logger, shift_left_dir
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.utils.file_search import (
    PIPELINE_FOLDER_NAME,
    get_or_build_inventory,
    get_table_ref_from_inventory,
    from_pipeline_to_absolute,
    from_absolute_to_pipeline,
    get_table_type_from_file_path,
)


class ColumnInfo(BaseModel):
    name: str
    type: str = "STRING"


class TableInfo(BaseModel):
    table_name: str
    type: str = "unknown"
    dml_ref: str = ""
    ddl_ref: str = ""
    columns: List[ColumnInfo] = Field(default_factory=list)


class FieldEdge(BaseModel):
    target_table: str
    target_column: str
    source_table: str
    source_column: str


class RunInfo(BaseModel):
    dml_file: str = ""
    pipeline_path: str = ""
    sink_table: str = ""
    started_at: str = ""


class FieldLineageResult(BaseModel):
    run: RunInfo = Field(default_factory=RunInfo)
    tables: List[TableInfo] = Field(default_factory=list)
    field_edges: List[FieldEdge] = Field(default_factory=list)


def _resolve_dml_path(dml_file: str, pipeline_path: str) -> str:
    if not dml_file:
        return ""
    if dml_file.startswith(PIPELINE_FOLDER_NAME):
        root = os.path.dirname(pipeline_path) if pipeline_path else os.getenv("PIPELINES", "")
        return os.path.join(root, dml_file)
    if os.path.isabs(dml_file):
        return dml_file
    return os.path.abspath(dml_file)


def _table_entry(
    table_name: str,
    table_type: str,
    dml_ref: str,
    ddl_ref: str,
    ddl_content: str,
    parser: SQLparser,
) -> TableInfo:
    columns_meta = parser.build_column_metadata_from_sql_content(ddl_content) if ddl_content else {}
    columns = [
        ColumnInfo(name=name, type=meta.get("type", "STRING"))
        for name, meta in columns_meta.items()
    ]
    return TableInfo(
        table_name=table_name,
        type=table_type,
        dml_ref=dml_ref,
        ddl_ref=ddl_ref,
        columns=columns,
    )


def _crawl_table(
    table_name: str,
    dml_abs: str,
    ddl_abs: str,
    dml_content: str,
    ddl_content: str,
    inventory: Dict,
    parser: SQLparser,
    pipeline_path: str,
    tables_seen: Set[str],
    tables_out: List[TableInfo],
    field_edges_out: List[FieldEdge],
) -> None:
    if table_name in tables_seen:
        return
    tables_seen.add(table_name)
    table_type = get_table_type_from_file_path(dml_abs)
    dml_ref = from_absolute_to_pipeline(dml_abs) if (pipeline_path and os.getenv("PIPELINES")) else dml_abs
    ddl_ref = from_absolute_to_pipeline(ddl_abs) if (pipeline_path and os.getenv("PIPELINES")) else ddl_abs
    entry = _table_entry(table_name, table_type, dml_ref, ddl_ref, ddl_content, parser)
    tables_out.append(entry)
    refs = parser.extract_table_references(dml_content)
    parent_names = refs - {table_name} if table_name in refs else refs
    edges = parser.extract_field_lineage_edges_from_dml(dml_content, table_name, parent_names)
    for e in edges:
        field_edges_out.append(
            FieldEdge(
                target_table=e["target_table"],
                target_column=e["target_column"],
                source_table=e["source_table"],
                source_column=e["source_column"],
            )
        )
    for pname in parent_names:
        if pname in tables_seen:
            continue
        try:
            table_ref = get_table_ref_from_inventory(pname, inventory)
        except Exception:
            logger.warning("Table %s not in inventory, skipping parent crawl", pname)
            continue
        p_dml_ref = table_ref.dml_ref or ""
        p_ddl_ref = table_ref.ddl_ref or ""
        p_dml_abs = (
            from_pipeline_to_absolute(p_dml_ref)
            if p_dml_ref.startswith(PIPELINE_FOLDER_NAME)
            else (os.path.join(pipeline_path, p_dml_ref) if pipeline_path and p_dml_ref else p_dml_ref)
        )
        p_ddl_abs = p_dml_abs.replace("dml.", "ddl.") if p_dml_abs else ""
        if not p_ddl_abs and p_ddl_ref:
            p_ddl_abs = (
                from_pipeline_to_absolute(p_ddl_ref)
                if p_ddl_ref.startswith(PIPELINE_FOLDER_NAME)
                else (os.path.join(pipeline_path, p_ddl_ref) if pipeline_path else p_ddl_ref)
            )
        p_dml_content = ""
        p_ddl_content = ""
        if p_dml_abs and os.path.exists(p_dml_abs):
            with open(p_dml_abs) as f:
                p_dml_content = f.read()
        if p_ddl_abs and os.path.exists(p_ddl_abs):
            with open(p_ddl_abs) as f:
                p_ddl_content = f.read()
        if p_dml_content:
            _crawl_table(
                pname,
                p_dml_abs,
                p_ddl_abs,
                p_dml_content,
                p_ddl_content,
                inventory,
                parser,
                pipeline_path,
                tables_seen,
                tables_out,
                field_edges_out,
            )
        else:
            tables_seen.add(pname)
            p_type = get_table_type_from_file_path(p_ddl_abs or p_dml_abs)
            p_entry = _table_entry(pname, p_type, p_dml_ref, p_ddl_ref, p_ddl_content, parser)
            tables_out.append(p_entry)


def build_field_lineage(dml_file: str, pipeline_path: str) -> FieldLineageResult:
    """Build field lineage from DML file, crawling parents to sources."""
    prev_pipelines = os.environ.get("PIPELINES")
    if pipeline_path:
        os.environ["PIPELINES"] = pipeline_path
    try:
        inventory = get_or_build_inventory(pipeline_path, pipeline_path, False)
        dml_abs = _resolve_dml_path(dml_file, pipeline_path)
        if not dml_abs or not os.path.exists(dml_abs):
            raise FileNotFoundError(f"DML file not found: {dml_file}")
        with open(dml_abs) as f:
            dml_content = f.read()
        ddl_abs = dml_abs.replace("dml.", "ddl.")
        ddl_content = ""
        if os.path.exists(ddl_abs):
            with open(ddl_abs) as f:
                ddl_content = f.read()
        parser = SQLparser()
        target_table = parser.extract_table_name_from_insert_into_statement(dml_content)
        if not target_table or target_table == "No-Table":
            target_table = os.path.splitext(os.path.basename(dml_abs))[0].replace("dml.", "")
        tables_seen: Set[str] = set()
        tables_out: List[TableInfo] = []
        field_edges_out: List[FieldEdge] = []
        _crawl_table(
            target_table,
            dml_abs,
            ddl_abs,
            dml_content,
            ddl_content,
            inventory,
            parser,
            pipeline_path,
            tables_seen,
            tables_out,
            field_edges_out,
        )
        run_info = RunInfo(
            dml_file=dml_file,
            pipeline_path=pipeline_path or "",
            sink_table=target_table,
            started_at=datetime.now(tz=timezone.utc).isoformat(),
        )
        return FieldLineageResult(run=run_info, tables=tables_out, field_edges=field_edges_out)
    finally:
        if prev_pipelines is not None:
            os.environ["PIPELINES"] = prev_pipelines
        else:
            os.environ.pop("PIPELINES", None)


def write_lineage_json(result: FieldLineageResult, output_path: str) -> None:
    """Write lineage result to JSON file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(result.model_dump_json(indent=2))


def _subgraph_for_sink_table(
    result: FieldLineageResult,
) -> tuple[Set[tuple[str, str]], List[tuple[str, str, str, str]]]:
    """Return (nodes_set, edges_list) for the lineage subgraph of the sink table's fields only.

    Nodes are (table_name, column_name). Edges are (source_table, source_column, target_table, target_column).
    """
    sink_table = result.run.sink_table
    sink_columns: Set[tuple[str, str]] = set()
    for table in result.tables:
        if table.table_name == sink_table:
            for col in table.columns:
                sink_columns.add((table.table_name, col.name))
            break
    if not sink_columns:
        return set(), []
    nodes_to_show: Set[tuple[str, str]] = set(sink_columns)
    edges_to_show: List[tuple[str, str, str, str]] = []
    frontier = set(sink_columns)
    while frontier:
        next_frontier: Set[tuple[str, str]] = set()
        for edge in result.field_edges:
            tgt = (edge.target_table, edge.target_column)
            if tgt not in frontier:
                continue
            src = (edge.source_table, edge.source_column)
            edges_to_show.append(
                (edge.source_table, edge.source_column, edge.target_table, edge.target_column)
            )
            if src not in nodes_to_show:
                nodes_to_show.add(src)
                next_frontier.add(src)
        frontier = next_frontier
    return nodes_to_show, edges_to_show


def build_field_lineage_graph_html(result: FieldLineageResult, output_path: str) -> None:
    """Build interactive HTML graph for field lineage using pyvis.

    Shows only the fields of the specified (sink) table and their upstream source fields.
    """
    from pyvis.network import Network

    nodes_to_show, edges_to_show = _subgraph_for_sink_table(result)
    if not nodes_to_show and not edges_to_show:
        nodes_to_show = set()
        for table in result.tables:
            for col in table.columns:
                nodes_to_show.add((table.table_name, col.name))
        edges_to_show = [
            (e.source_table, e.source_column, e.target_table, e.target_column)
            for e in result.field_edges
        ]

    net = Network(
        height="800px",
        width="100%",
        bgcolor="#222222",
        font_color="white",
        directed=True,
        notebook=False,
    )
    net.set_options("""
    {
      "layout": {
        "hierarchical": {
          "enabled": true,
          "direction": "LR",
          "sortMethod": "directed",
          "levelSeparation": 150,
          "nodeSpacing": 200,
          "treeSpacing": 200,
          "blockShifting": true,
          "edgeMinimization": true,
          "parentCentralization": true
        }
      },
      "physics": {
        "hierarchicalRepulsion": {
          "centralGravity": 0.0,
          "springLength": 200,
          "springConstant": 0.01,
          "nodeDistance": 200,
          "damping": 0.09
        },
        "maxVelocity": 50,
        "minVelocity": 0.75,
        "solver": "hierarchicalRepulsion"
      }
    }
    """)
    for (table_name, col_name) in nodes_to_show:
        nid = f"{table_name}.{col_name}"
        label = f"{table_name}\n.{col_name}"
        net.add_node(nid, label=label.replace(".", "\n"), title=nid, shape="box", font={"size": 12})
    for (src_table, src_col, tgt_table, tgt_col) in edges_to_show:
        src_id = f"{src_table}.{src_col}"
        tgt_id = f"{tgt_table}.{tgt_col}"
        net.add_edge(src_id, tgt_id, arrows="to", color={"color": "#7f8c8d", "highlight": "#3498db"})
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    net.save_graph(str(path))


def run_field_lineage(
    dml_file: str,
    pipeline_path: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> str:
    """Run field lineage and write JSON + HTML to output_dir. Returns output_dir path."""
    pipeline_path = pipeline_path or os.getenv("PIPELINES", "")
    if not pipeline_path:
        raise ValueError("pipeline_path or PIPELINES environment variable is required")
    result = build_field_lineage(dml_file, pipeline_path)
    run_id = f"{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}_{result.run.sink_table}"
    base_dir = Path(output_dir or os.path.join(shift_left_dir, "field_lineage", run_id))
    base_dir.mkdir(parents=True, exist_ok=True)
    write_lineage_json(result, str(base_dir / "lineage.json"))
    build_field_lineage_graph_html(result, str(base_dir / "field_lineage.html"))
    return str(base_dir)


def run_field_lineage_from_table(
    table_name: str,
    pipeline_path: str,
    output_dir: Optional[str] = None,
) -> str:

    inventory = get_or_build_inventory(pipeline_path, pipeline_path, False)
    table_name_lower = table_name.lower()
    if table_name_lower not in inventory:
        raise ValueError(f"Table not found in inventory: {table_name}")
    table_ref = get_table_ref_from_inventory(table_name_lower, inventory)
    dml_ref = table_ref.dml_ref or ""
    if not dml_ref:
        raise ValueError(f"Table {table_name} has no DML reference in inventory")
    dml_file = (
        from_pipeline_to_absolute(dml_ref)
        if dml_ref.startswith(PIPELINE_FOLDER_NAME)
        else (os.path.join(pipeline_path, dml_ref) if pipeline_path else dml_ref)
    )
    if not os.path.isabs(dml_file) and pipeline_path:
        dml_file = os.path.join(pipeline_path, dml_ref)
    return run_field_lineage(dml_file, pipeline_path, output_dir)

