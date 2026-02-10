"""
Copyright 2024-2025 Confluent, Inc.

CLI for RAG: build vector index from ksql-project corpus.
"""
import os
from pathlib import Path
from typing_extensions import Annotated

import typer

from shift_left.core.utils.secure_typer import create_secure_typer_app

app = create_secure_typer_app(pretty_exceptions_show_locals=False)


@app.command()
def build(
    corpus: Annotated[
        str,
        typer.Argument(help="Path to ksql-project root (contains flink-references/ and sources/)."),
    ] = ".",
    index: Annotated[
        str,
        typer.Option("--index", "-o", help="Output path for ChromaDB index (default: <corpus>/rag_index or SL_RAG_INDEX_PATH)."),
    ] = "",
):
    """
    Build or rebuild the RAG vector index from a ksql-project corpus.

    Loads (ksql, Flink DDL/DML) example pairs from flink-references and sources,
    then indexes them for similar-ksql retrieval during translation.

    Example:
      shift_left rag build /path/to/ksql-project --index /path/to/rag_index
      shift_left rag build $FLINK_PROJECT/../ksql-project
    """
    corpus_path = Path(corpus).resolve()
    if not corpus_path.is_dir():
        typer.echo(f"Error: corpus path is not a directory: {corpus_path}", err=True)
        raise typer.Exit(1)
    refs = corpus_path / "flink-references"
    if not refs.is_dir():
        typer.echo(f"Error: corpus must contain flink-references/: {corpus_path}", err=True)
        raise typer.Exit(1)

    index_path = index.strip() or os.getenv("SL_RAG_INDEX_PATH", "").strip()
    if not index_path:
        index_path = str(corpus_path / "rag_index")
    index_path = str(Path(index_path).resolve())
    Path(index_path).mkdir(parents=True, exist_ok=True)

    try:
        from shift_left.ai.rag import KsqlFlinkVectorStore, load_ksql_flink_corpus
    except ImportError:
        typer.echo(
            "Error: RAG dependencies not installed. Install with: pip install -e '.[rag]'",
            err=True,
        )
        raise typer.Exit(1)

    pairs = load_ksql_flink_corpus(corpus_path)
    if not pairs:
        typer.echo("Warning: no example pairs loaded from corpus. Check flink-references/ and sources/.", err=True)
    for pair in pairs:
        typer.echo(f"KSQL Example: {pair.name}")
    typer.echo("-" * 80)
    store = KsqlFlinkVectorStore(index_path)
    n = store.index_corpus(corpus_path, pairs=pairs)
    typer.echo(f"Indexed {n} ksql→Flink example pairs at {index_path}")
    typer.echo(f"Set SL_RAG_ENABLED=1 and SL_RAG_INDEX_PATH={index_path} to use RAG during ksql migration.")
