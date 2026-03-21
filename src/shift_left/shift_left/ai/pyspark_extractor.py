"""
Copyright 2024-2026 Confluent, Inc.

LLM-backed extraction of the migratable DataFrame pipeline from a PySpark Python file.
Uses the same LLM config as the translator (SL_LLM_MODEL, SL_LLM_BASE_URL, SL_LLM_API_KEY).
"""
from __future__ import annotations

import json
import os
import re
from typing import List, Tuple

import importlib.resources
from openai import OpenAI
from pydantic import BaseModel

from shift_left.core.utils.app_config import logger


class PySparkExtractResult(BaseModel):
    """Structured LLM response: runnable snippet and table/view names the snippet reads from."""

    code_snippet: str
    table_names: List[str] = []


def _load_prompt() -> str:
    fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath(
        "pyspark_extract.txt"
    )
    with fname.open("r") as f:
        return f.read()


def _get_llm_client() -> OpenAI:
    model_name = os.getenv("SL_LLM_MODEL", "qwen3-coder-30b-a3b-instruct-mlx-4bit")
    base_url = os.getenv("SL_LLM_BASE_URL", "http://localhost:1337/v1")
    api_key = os.getenv("SL_LLM_API_KEY", "no_llm_key")
    logger.info("PySpark extractor using %s with %s", model_name, base_url)
    return OpenAI(api_key=api_key, base_url=base_url)


def extract_pipeline_from_pyspark(pyspark_source: str) -> Tuple[str, List[str]]:
    """
    Use the LLM to extract the migratable DataFrame-building snippet from PySpark code.

    Args:
        pyspark_source: Full contents of a PySpark Python file.

    Returns:
        Tuple of (code_snippet, table_names). The snippet must assign the final
        DataFrame to a variable named result_df. table_names lists views/tables
        the snippet expects to exist (e.g. ["ecommerce_events"]).

    Raises:
        ValueError: If the LLM response cannot be parsed or is invalid.
    """
    system_prompt = _load_prompt()
    client = _get_llm_client()
    model_name = os.getenv("SL_LLM_MODEL", "qwen3-coder-30b-a3b-instruct-mlx-4bit")

    user_content = f"Extract the migratable DataFrame pipeline from this PySpark file.\n\n```python\n{pyspark_source}\n```"

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]

    try:
        response = client.chat.completions.parse(
            model=model_name,
            response_format=PySparkExtractResult,
            messages=messages,
            temperature=0.1,
            max_completion_tokens=4096,
        )
        parsed = response.choices[0].message.parsed
        if not parsed or not (getattr(parsed, "code_snippet", "") or "").strip():
            raise ValueError("LLM did not return a valid code_snippet")
        snippet = (parsed.code_snippet or "").strip()
        tables = list(parsed.table_names or [])
        return snippet, tables
    except Exception as e:
        if "parse" in str(e).lower() or "response_format" in str(e).lower():
            return _extract_via_json_fallback(client, model_name, messages, e)
        raise ValueError(f"LLM extraction failed: {e}") from e


def _extract_via_json_fallback(client, model_name: str, messages: list, original_error: Exception) -> Tuple[str, List[str]]:
    """Fallback: ask for JSON and parse manually when .parse() is not supported."""
    fallback_user = (
        messages[-1]["content"]
        + "\n\nReply with a single JSON object with keys: code_snippet (string), table_names (list of strings). "
        "No markdown, no explanation."
    )
    fallback_messages = [messages[0], {"role": "user", "content": fallback_user}]
    response = client.chat.completions.create(
        model=model_name,
        messages=fallback_messages,
        temperature=0.1,
        max_tokens=4096,
    )
    content = (response.choices[0].message.content or "").strip()
    content = re.sub(r"^```\w*\n?", "", content)
    content = re.sub(r"\n?```\s*$", "", content)
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM extraction failed and JSON fallback parse failed: {original_error!s}; {e}") from e
    snippet = (data.get("code_snippet") or "").strip()
    if not snippet:
        raise ValueError("LLM did not return a valid code_snippet")
    tables = list(data.get("table_names") or [])
    return snippet, tables
