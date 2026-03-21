"""
Copyright 2024-2025 Confluent, Inc.

RAG configuration: enabled, index path, top_k.
Reads from environment variables so RAG stays optional and backward compatible.
"""
import os
from pathlib import Path
from typing import Optional


def rag_enabled() -> bool:
    """True if RAG is enabled (SL_RAG_ENABLED=1 or true). Default: False."""
    v = os.getenv("SL_RAG_ENABLED", "").strip().lower()
    return v in ("1", "true", "yes")


def rag_index_path() -> Optional[str]:
    """Path to ChromaDB index directory. Default: None (RAG disabled)."""
    v = os.getenv("SL_RAG_INDEX_PATH", "").strip()
    if not v:
        return None
    p = Path(v)
    return str(p) if p.is_dir() else None


def rag_top_k() -> int:
    """Number of similar examples to retrieve. Default: 3."""
    try:
        return max(1, min(10, int(os.getenv("SL_RAG_TOP_K", "3"))))
    except ValueError:
        return 3
