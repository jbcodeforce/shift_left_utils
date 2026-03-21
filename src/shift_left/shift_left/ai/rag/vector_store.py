"""
Copyright 2024-2025 Confluent, Inc.

Vector store for ksql-to-Flink SQL RAG: index (ksql_text -> flink_ddl, flink_dml) and search by similar ksql.
Uses ChromaDB and sentence-transformers when the optional [rag] dependencies are installed.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

from shift_left.ai.rag.corpus_loader import ExamplePair, load_ksql_flink_corpus

_COLLECTION_NAME = "ksql_flink_examples"
# ChromaDB metadata value length limit; keep flink_ddl/dml under this or store by reference
_METADATA_MAX_LEN = 40000


def _truncate_for_metadata(s: str, max_len: int = _METADATA_MAX_LEN) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 20] + "\n... [truncated]"


class KsqlFlinkVectorStore:
    """
    Vector store that indexes ksql text and returns (ksql, flink_ddl, flink_dml) pairs on search.
    Requires optional dependencies: chromadb, sentence-transformers (install with pip install -e ".[rag]").
    """

    def __init__(
        self,
        persist_directory: str | Path,
        *,
        embedding_model: str = "all-MiniLM-L6-v2",
        # "text-embedding-all-minilm-l6-v2-embedding"
        collection_name: str = _COLLECTION_NAME,
    ):
        self._persist_directory = Path(persist_directory)
        self._embedding_model = embedding_model
        self._collection_name = collection_name
        self._client = None
        self._collection = None
        self._embedding_fn = None

    def _ensure_client(self) -> None:
        if self._client is not None:
            return
        try:
            import chromadb
            from chromadb.config import Settings
            from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
        except ImportError as e:
            raise ImportError(
                "RAG vector store requires optional dependencies: pip install -e '.[rag]' (chromadb, sentence-transformers)"
            ) from e
        self._client = chromadb.PersistentClient(
            path=str(self._persist_directory),
            settings=Settings(anonymized_telemetry=False),
        )
        self._embedding_fn = SentenceTransformerEmbeddingFunction(model_name=self._embedding_model)
        self._collection = self._client.get_or_create_collection(
            name=self._collection_name,
            embedding_function=self._embedding_fn,
        )

    def index_corpus(self, corpus_root: str | Path,
        pairs: List[ExamplePair],
        manifest_path: Optional[str | Path] = None) -> int:
        """
        Load example pairs from corpus_root and index them (embed ksql_text, store flink_ddl/dml in metadata).
        Returns number of documents indexed.
        """
        if not pairs:
            pairs = load_ksql_flink_corpus(corpus_root, manifest_path=manifest_path)
            if not pairs:
                return 0
        self._ensure_client()
        ids = []
        documents = []
        metadatas = []
        for pair in pairs:
            ids.append(pair.name)
            documents.append(pair.ksql_text)
            metadatas.append({
                "name": pair.name,
                "flink_ddl": _truncate_for_metadata(pair.flink_ddl),
                "flink_dml": _truncate_for_metadata(pair.flink_dml),
            })
        self._collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
        return len(pairs)

    def search(self, ksql_query: str, top_k: int = 3) -> List[ExamplePair]:
        """
        Search for similar ksql examples; returns ExamplePair list (ksql_text, flink_ddl, flink_dml).
        """
        self._ensure_client()
        n = self._collection.count()
        if n == 0:
            return []
        results = self._collection.query(
            query_texts=[ksql_query],
            n_results=min(top_k, n),
            include=["documents", "metadatas", "distances"],
        )
        if not results or not results["ids"] or not results["ids"][0]:
            return []
        pairs: List[ExamplePair] = []
        for i, doc_id in enumerate(results["ids"][0]):
            meta = (results["metadatas"] or [None])[0]
            meta = meta[i] if meta and i < len(meta) else {}
            doc = (results["documents"] or [None])[0]
            doc = doc[i] if doc and i < len(doc) else ""
            pairs.append(
                ExamplePair(
                    name=meta.get("name", doc_id),
                    ksql_text=doc or "",
                    flink_ddl=meta.get("flink_ddl", ""),
                    flink_dml=meta.get("flink_dml", ""),
                )
            )
        return pairs

    def count(self) -> int:
        """Return number of indexed documents."""
        self._ensure_client()
        return self._collection.count()


def get_rag_store(
    index_path: Optional[str | Path] = None,
    *,
    embedding_model: str = "all-MiniLM-L6-v2",
) -> Optional[KsqlFlinkVectorStore]:
    """
    Return a KsqlFlinkVectorStore if index_path exists and RAG deps are available; else None.
    """
    path = index_path or os.getenv("SL_RAG_INDEX_PATH")
    if not path or not Path(path).is_dir():
        return None
    try:
        return KsqlFlinkVectorStore(path, embedding_model=embedding_model)
    except ImportError:
        return None
