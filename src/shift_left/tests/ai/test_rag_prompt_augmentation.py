"""
Tests for RAG prompt augmentation: _build_rag_examples_block and prompt content when RAG enabled/disabled.
"""
import unittest
from unittest.mock import MagicMock, patch
import pathlib
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent / "config-ccloud.yaml")

from shift_left.ai.ksql_code_agent import KsqlToFlinkSqlAgent
from shift_left.ai.rag.corpus_loader import ExamplePair
from shift_left.ai.translator_to_flink_sql import _strip_markdown_json


class TestRAGPromptAugmentation(unittest.TestCase):
    def test_rag_disabled_returns_empty_block(self):
        agent = KsqlToFlinkSqlAgent()
        agent.use_rag_for_translation = False
        block = agent._build_rag_examples_block("CREATE STREAM x WITH (...);")
        self.assertEqual(block, "")

    def test_rag_enabled_mock_store_injects_examples(self):
        agent = KsqlToFlinkSqlAgent()
        agent.use_rag_for_translation = True
        mock_pair = ExamplePair(
            name="agg",
            ksql_text="CREATE STREAM orders AS SELECT ...",
            flink_ddl="CREATE TABLE IF NOT EXISTS orders (...);",
            flink_dml="INSERT INTO orders SELECT ...",
        )
        mock_store = MagicMock()
        mock_store.search.return_value = [mock_pair]

        with patch("shift_left.ai.rag.get_rag_store", return_value=mock_store):
            with patch("shift_left.ai.rag.rag_enabled", return_value=True):
                block = agent._build_rag_examples_block("CREATE STREAM x WITH (...);")

        self.assertIn("Retrieved similar examples", block)
        self.assertIn("Example 1", block)
        self.assertIn("CREATE STREAM orders AS SELECT", block)
        self.assertIn("CREATE TABLE IF NOT EXISTS orders", block)
        self.assertIn("INSERT INTO orders SELECT", block)

    def test_do_translation_system_prompt_includes_rag_block_when_enabled(self):
        agent = KsqlToFlinkSqlAgent()
        agent.use_rag_for_translation = True
        mock_pair = ExamplePair(
            name="ex",
            ksql_text="KSQL_SNIP",
            flink_ddl="DDL_SNIP",
            flink_dml="DML_SNIP",
        )
        mock_store = MagicMock()
        mock_store.search.return_value = [mock_pair]

        with patch("shift_left.ai.rag.get_rag_store", return_value=mock_store):
            with patch("shift_left.ai.rag.rag_enabled", return_value=True):
                agent.llm_client = MagicMock()
                response = MagicMock()
                response.choices = [MagicMock()]
                response.choices[0].message = MagicMock()
                response.choices[0].message.content = '{"flink_ddl_output": "OUT_DDL", "flink_dml_output": "OUT_DML"}'
                agent.llm_client.chat.completions.create.return_value = response

                ddl, dml = agent._do_translation_with_agent("ksql_input: CREATE STREAM x;")

        call_args = agent.llm_client.chat.completions.create.call_args
        messages = call_args.kwargs.get("messages") or call_args[1].get("messages")
        self.assertIsNotNone(messages)
        system_msg = next((m for m in messages if m.get("role") == "system"), None)
        self.assertIsNotNone(system_msg)
        self.assertIn("Retrieved similar examples", system_msg["content"])
        self.assertIn("KSQL_SNIP", system_msg["content"])
        self.assertIn("ksql_input:", next((m for m in messages if m.get("role") == "user"), {}).get("content", ""))

    def test_strip_markdown_json_removes_code_fences(self):
        wrapped = '```json\n{\n  "flink_ddl_output": "CREATE TABLE x",\n  "flink_dml_output": ""\n}\n```'
        stripped = _strip_markdown_json(wrapped)
        self.assertTrue(stripped.startswith("{"), stripped)
        self.assertTrue(stripped.endswith("}"), stripped)
        self.assertNotIn("```", stripped)

    def test_do_translation_parses_markdown_wrapped_json(self):
        agent = KsqlToFlinkSqlAgent()
        agent.use_rag_for_translation = False
        agent.llm_client = MagicMock()
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message = MagicMock()
        response.choices[0].message.content = (
            '```json\n{"flink_ddl_output": "OUT_DDL", "flink_dml_output": "OUT_DML"}\n```'
        )
        agent.llm_client.chat.completions.create.return_value = response

        ddl, dml = agent._do_translation_with_agent("CREATE STREAM x;")

        self.assertEqual(ddl, "OUT_DDL")
        self.assertEqual(dml, "OUT_DML")
