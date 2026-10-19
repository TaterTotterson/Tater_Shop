from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


TEST_DIR = Path(__file__).resolve().parent
VERBA_DIR = TEST_DIR if (TEST_DIR / "tater_assistant_support.py").exists() else TEST_DIR.parent / "verba"
MODULES = (
    ("tater_assistant_support", "tater_assistant_support"),
    ("wake_word_trainer_support", "wake_word_trainer_support"),
    ("tater_tube_support", "tater_tube_support"),
)


def _action_failure(*, code, message, **kwargs):
    return {"ok": False, "error": {"code": code, "message": message}, **kwargs}


def _research_success(*, answer, highlights=None, sources=None, say_hint=""):
    return {
        "ok": True,
        "result_type": "research",
        "answer": answer,
        "highlights": highlights or [],
        "sources": sources or [],
        "say_hint": say_hint,
    }


def _load_module(module_name: str):
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})
    verba_result = types.ModuleType("verba_result")
    verba_result.action_failure = _action_failure
    verba_result.research_success = _research_success
    helpers = types.ModuleType("helpers")
    async def llm_structured_chat(client, *, messages, **kwargs):
        return await client.chat(messages=messages, **kwargs)

    helpers.redis_client = types.SimpleNamespace(hgetall=lambda _key: {})
    helpers.llm_structured_chat = llm_structured_chat

    path = VERBA_DIR / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(f"{module_name}_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {"verba_base": verba_base, "verba_result": verba_result, "helpers": helpers},
    ):
        spec.loader.exec_module(module)
    return module


class _SelectionLLM:
    def __init__(self, payload):
        self.payload = payload

    async def chat(self, **_kwargs):
        return {"message": {"content": json.dumps(self.payload)}}


class DiscordProductSupportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loaded = [(expected_id, _load_module(module_name)) for module_name, expected_id in MODULES]

    def test_each_verba_is_discord_only_and_uses_fixed_public_repositories(self):
        seen_ids = set()
        for expected_id, module in self.loaded:
            with self.subTest(verba=expected_id):
                plugin = module.verba
                self.assertEqual(plugin.name, expected_id)
                self.assertEqual(plugin.platforms, ["discord"])
                self.assertTrue(plugin.description.startswith("Use when a Discord user asks"))
                self.assertEqual(plugin.verba_dec, plugin.description)
                self.assertEqual(plugin.argument_schema["required"], ["query"])
                self.assertNotIn(plugin.name, seen_ids)
                seen_ids.add(plugin.name)
                self.assertGreaterEqual(len(plugin.repositories), 2)
                for repo in plugin.repositories:
                    self.assertTrue(repo["slug"].startswith("TaterTotterson/"))
                    self.assertEqual(repo["ref"], "main")
                if expected_id == "wake_word_trainer_support":
                    self.assertIn("Do not use for openWakeWord or NanoWakeWord", plugin.description)
                    self.assertEqual(
                        {repo["slug"] for repo in plugin.repositories},
                        {
                            "TaterTotterson/microWakeWord-Trainer-AppleSilicon",
                            "TaterTotterson/microWakeWord-Trainer-Nvidia-Docker",
                        },
                    )
                if expected_id == "tater_assistant_support":
                    self.assertEqual(
                        {repo["slug"] for repo in plugin.repositories},
                        {
                            "TaterTotterson/Tater",
                            "TaterTotterson/Tater_Shop",
                            "TaterTotterson/Tater_Integrations",
                        },
                    )

    def test_path_filter_blocks_build_outputs_traversal_and_sensitive_names(self):
        for expected_id, module in self.loaded:
            plugin = module.verba
            with self.subTest(verba=expected_id):
                self.assertTrue(plugin._allowed_path("README.md"))
                self.assertTrue(plugin._allowed_path("docs/INSTALL.md"))
                self.assertTrue(plugin._allowed_path("src/discord_portal.py"))
                self.assertTrue(plugin._allowed_path(".env.example"))
                self.assertFalse(plugin._allowed_path("../outside.py"))
                self.assertFalse(plugin._allowed_path("node_modules/pkg/index.js"))
                self.assertFalse(plugin._allowed_path("config/private_key.txt"))
                self.assertFalse(plugin._allowed_path("frontend/app.min.js"))

    def test_file_selection_accepts_only_catalog_entries(self):
        expected_id, module = self.loaded[0]
        plugin = module.verba
        slug = plugin.repositories[0]["slug"]
        candidates = {slug: ["README.md", "setup_tater.sh"]}
        llm = _SelectionLLM(
            {
                "files": [
                    {"repo": slug, "path": "README.md"},
                    {"repo": slug, "path": "../../etc/passwd"},
                    {"repo": "someone/else", "path": "README.md"},
                ]
            }
        )
        selected = asyncio.run(plugin._select_files("How do I install it?", candidates, llm))
        self.assertEqual(selected, [(slug, "README.md")])

    def test_handle_returns_research_result_with_exact_loaded_sources(self):
        for expected_id, module in self.loaded:
            plugin = module.verba
            material = {
                "repo": plugin.repositories[0]["slug"],
                "path": "README.md",
                "url": f"https://github.com/{plugin.repositories[0]['slug']}/blob/main/README.md",
                "text": "Install instructions",
            }
            with self.subTest(verba=expected_id), patch.object(
                plugin, "_materials", AsyncMock(return_value=([material], []))
            ), patch.object(
                plugin, "_answer", AsyncMock(return_value="Use the documented install steps.")
            ):
                result = asyncio.run(
                    plugin._handle({"query": "How do I install it?"}, _SelectionLLM({"files": []}))
                )
                self.assertTrue(result["ok"])
                self.assertEqual(result["result_type"], "research")
                self.assertEqual(result["answer"], "Use the documented install steps.")
                self.assertEqual(result["sources"][0]["url"], material["url"])

    def test_missing_question_fails_before_network_lookup(self):
        for expected_id, module in self.loaded:
            plugin = module.verba
            with self.subTest(verba=expected_id):
                result = asyncio.run(plugin._handle({}, object()))
                self.assertFalse(result["ok"])
                self.assertEqual(result["error"]["code"], "missing_query")


if __name__ == "__main__":
    unittest.main()
