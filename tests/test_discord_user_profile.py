from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "verba" / "discord_user_profile.py"


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


class _Redis:
    def __init__(self, values=None):
        self.values = values or {}

    def hgetall(self, _key):
        return dict(self.values)


def _load_module(redis_values=None):
    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = type("ToolVerba", (), {})
    verba_result = types.ModuleType("verba_result")
    verba_result.action_failure = _action_failure
    verba_result.research_success = _research_success
    helpers = types.ModuleType("helpers")
    helpers.redis_client = _Redis(redis_values)

    async def llm_structured_chat(client, *, messages, **kwargs):
        return await client.chat(messages=messages, **kwargs)

    helpers.llm_structured_chat = llm_structured_chat
    spec = importlib.util.spec_from_file_location("discord_user_profile_test_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    with patch.dict(
        sys.modules,
        {"verba_base": verba_base, "verba_result": verba_result, "helpers": helpers},
    ):
        spec.loader.exec_module(module)
    return module


def _author(user_id, name, *, bot=False):
    return types.SimpleNamespace(
        id=user_id,
        display_name=name,
        global_name=name,
        name=name.lower().replace(" ", "_"),
        bot=bot,
    )


def _history_message(message_id, created_at, author, content):
    return types.SimpleNamespace(
        id=message_id,
        created_at=created_at,
        author=author,
        content=content,
        mentions=[],
        attachments=[],
        stickers=[],
    )


class _Channel:
    def __init__(self, rows):
        self.id = 99
        self.name = "general"
        self.guild = None
        self.rows = list(rows)
        self.history_kwargs = None

    def history(self, **kwargs):
        self.history_kwargs = kwargs

        async def generate():
            rows = self.rows if kwargs.get("oldest_first") else reversed(self.rows)
            for row in rows:
                yield row

        return generate()


class _CaptureLLM:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def chat(self, **kwargs):
        self.calls.append(kwargs)
        return {"message": {"content": self.responses.pop(0)}}


class DiscordUserProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_module()

    def setUp(self):
        self.plugin = self.module.DiscordUserProfileVerba()
        self.now = datetime(2026, 10, 21, 15, 30, tzinfo=timezone.utc)

    def test_metadata_is_discord_only_and_natural_language_first(self):
        self.assertEqual(self.plugin.name, "discord_user_profile")
        self.assertEqual(self.plugin.platforms, ["discord"])
        self.assertEqual(self.plugin.argument_schema["required"], ["query"])
        self.assertEqual(set(self.plugin.argument_schema["properties"]), {"query"})
        self.assertIn("evidence-based behavioral profile", self.plugin.description)

    def test_default_and_everything_we_know_windows(self):
        default = self.plugin._deterministic_window("profile Alex", self.now, 30)
        self.assertEqual(default["start"], self.now - timedelta(days=30))
        self.assertEqual(default["end"], self.now)

        entire = self.plugin._deterministic_window(
            "tell me everything we know about Alex",
            self.now,
            30,
        )
        self.assertIsNone(entire["start"])
        self.assertEqual(entire["label"], "all available history")

    def test_self_and_replied_user_resolution(self):
        requester = _author(100, "Requester")
        target = _author(42, "Alex")
        message = types.SimpleNamespace(
            author=requester,
            mentions=[],
            reference=types.SimpleNamespace(
                resolved=types.SimpleNamespace(author=target),
                cached_message=None,
            ),
        )
        self_plan = asyncio.run(
            self.plugin._interpret_request("profile me", message, None, self.now, 30)
        )
        self.assertEqual(self_plan["target_ids"], {"100"})

        reply_plan = asyncio.run(
            self.plugin._interpret_request("profile this user", message, None, self.now, 30)
        )
        self.assertEqual(reply_plan["target_ids"], {"42"})

    def test_sensitive_contact_and_secret_like_text_is_redacted(self):
        raw = (
            "Email me at person@example.com or +1 (312) 555-0199. "
            "Use token abcdefghijklmnopqrstuvwxyz123456 and https://example.com/private?key=secret"
        )
        clean = self.plugin._sanitize_content(raw)
        self.assertNotIn("person@example.com", clean)
        self.assertNotIn("555-0199", clean)
        self.assertNotIn("abcdefghijklmnopqrstuvwxyz123456", clean)
        self.assertNotIn("/private", clean)
        self.assertIn("[email redacted]", clean)
        self.assertIn("[phone number redacted]", clean)
        self.assertIn("[secret-like text redacted]", clean)
        self.assertIn("[link: example.com]", clean)

    def test_handler_profiles_only_selected_member(self):
        alex = _author(42, "Alex")
        sam = _author(7, "Sam")
        requester = _author(100, "Requester")
        rows = [
            _history_message(1, self.now - timedelta(days=2), alex, "I maintain the release automation."),
            _history_message(2, self.now - timedelta(days=1), sam, "The docs need review."),
            _history_message(3, self.now - timedelta(hours=2), alex, "I prefer short pull requests."),
            _history_message(999, self.now - timedelta(minutes=1), requester, "profile <@42>"),
        ]
        channel = _Channel(rows)
        message = types.SimpleNamespace(
            id=999,
            channel=channel,
            guild=None,
            author=requester,
            mentions=[alex],
            reference=None,
        )
        plan = {
            "start": self.now - timedelta(days=30),
            "end": self.now,
            "label": "the last 30 days",
            "target_ids": {"42"},
            "target_names": [],
            "focus": "",
        }
        with patch.object(self.plugin, "_now", return_value=self.now), patch.object(
            self.plugin,
            "_interpret_request",
            AsyncMock(return_value=plan),
        ), patch.object(
            self.plugin,
            "_profile_records",
            AsyncMock(return_value="Alex focuses on release automation and prefers small pull requests."),
        ) as profile:
            result = asyncio.run(
                self.plugin.handle_discord(
                    message,
                    {"query": "profile <@42>"},
                    object(),
                )
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["result_type"], "research")
        self.assertIn("Member: Alex", result["answer"])
        self.assertIn("Member messages analyzed: 2", result["answer"])
        profiled_records = profile.await_args.args[0]
        self.assertEqual([record["author_id"] for record in profiled_records], ["42", "42"])
        self.assertEqual(channel.history_kwargs["limit"], 20001)
        self.assertFalse(channel.history_kwargs["oldest_first"])

    def test_multiple_targets_are_rejected_before_history_read(self):
        first = _author(1, "One")
        second = _author(2, "Two")
        channel = _Channel([])
        message = types.SimpleNamespace(
            id=9,
            channel=channel,
            guild=None,
            author=_author(100, "Requester"),
            mentions=[first, second],
            reference=None,
        )
        plan = {
            "start": self.now - timedelta(days=30),
            "end": self.now,
            "label": "the last 30 days",
            "target_ids": {"1", "2"},
            "target_names": [],
            "focus": "",
        }
        with patch.object(self.plugin, "_now", return_value=self.now), patch.object(
            self.plugin,
            "_interpret_request",
            AsyncMock(return_value=plan),
        ):
            result = asyncio.run(
                self.plugin.handle_discord(message, {"query": "profile both"}, object())
            )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "ambiguous_person")
        self.assertIsNone(channel.history_kwargs)

    def test_missing_target_is_rejected_before_history_read(self):
        channel = _Channel([])
        message = types.SimpleNamespace(
            id=9,
            channel=channel,
            guild=None,
            author=_author(100, "Requester"),
            mentions=[],
            reference=None,
        )
        plan = {
            "start": self.now - timedelta(days=30),
            "end": self.now,
            "label": "the last 30 days",
            "target_ids": set(),
            "target_names": [],
            "focus": "",
        }
        with patch.object(self.plugin, "_now", return_value=self.now), patch.object(
            self.plugin,
            "_interpret_request",
            AsyncMock(return_value=plan),
        ):
            result = asyncio.run(
                self.plugin.handle_discord(message, {"query": "make a profile"}, object())
            )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "missing_person")
        self.assertIsNone(channel.history_kwargs)

    def test_profile_prompt_uses_positive_observable_evidence_scope(self):
        llm = _CaptureLLM(["Evidence notes", "Final brief"])
        records = [
            {
                "created_at": self.now,
                "author_id": "42",
                "author": "Alex",
                "aliases": ["Alex"],
                "content": "I like release automation.",
            }
        ]
        settings = {
            "chunk_characters": 14000,
            "profile_max_tokens": 1400,
        }
        result = asyncio.run(
            self.plugin._profile_records(
                records,
                "Alex",
                {"label": "the last 30 days", "focus": ""},
                llm,
                settings,
            )
        )
        self.assertEqual(result, "Final brief")
        system_prompts = "\n".join(call["messages"][0]["content"] for call in llm.calls)
        self.assertIn("observable, non-sensitive participation", system_prompts)
        self.assertIn("Ground every claim in the messages", system_prompts)
        self.assertIn("fictional case-brief language is presentation", system_prompts)
        self.assertNotIn("Do not diagnose", system_prompts)
        self.assertNotIn("criminality", system_prompts)

    def test_json_result_wrapper_is_rendered_as_discord_markdown(self):
        model_payload = json.dumps(
            {
                "result": json.dumps(
                    {
                        "case_brief": {
                            "analyst_snapshot": {
                                "subject": "heapmaster",
                                "status": "Active / High Engagement",
                                "summary": "A highly technical participant focused on local automation.",
                            },
                            "known_from_messages": {
                                "recurring_topics": [
                                    "LLM deployment and optimization",
                                    "Wake-word testing",
                                ]
                            },
                        }
                    }
                )
            }
        )

        rendered = self.plugin._normalize_model_text(model_payload)

        self.assertIn("## Case Brief", rendered)
        self.assertIn("### Analyst Snapshot", rendered)
        self.assertIn("- **Subject:** heapmaster", rendered)
        self.assertIn("- LLM deployment and optimization", rendered)
        self.assertNotIn('{"result"', rendered)
        self.assertNotIn("\\n", rendered)

    def test_prose_chat_does_not_request_structured_tool_output(self):
        llm = _CaptureLLM([json.dumps({"case_brief": {"summary": "Readable brief"}})])

        rendered = asyncio.run(
            self.plugin._chat(
                llm,
                [{"role": "user", "content": "Write the brief."}],
                500,
            )
        )

        self.assertIn("## Case Brief", rendered)
        self.assertIn("Readable brief", rendered)
        self.assertNotIn("tools", llm.calls[0])


if __name__ == "__main__":
    unittest.main()
