"""
RAG (Retrieval-Augmented Generation) for ksql to Flink SQL translation.

Provides corpus loading of (ksql, Flink DDL/DML) example pairs and vector search
to inject similar examples into the translator prompt.
"""
from shift_left.ai.rag.corpus_loader import ExamplePair, load_ksql_flink_corpus
from shift_left.ai.rag.rag_config import rag_enabled, rag_index_path, rag_top_k
from shift_left.ai.rag.vector_store import KsqlFlinkVectorStore, get_rag_store

__all__ = [
    "ExamplePair",
    "load_ksql_flink_corpus",
    "KsqlFlinkVectorStore",
    "get_rag_store",
    "rag_enabled",
    "rag_index_path",
    "rag_top_k",
]
