from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "verba" / "discord_chat_summary.py"


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
    spec = importlib.util.spec_from_file_location("discord_chat_summary_test_module", MODULE_PATH)
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
        embeds=[],
    )


class _Channel:
    def __init__(self, rows):
        self.id = 99
        self.name = "general"
        self.rows = list(rows)
        self.history_kwargs = None

    def history(self, **kwargs):
        self.history_kwargs = kwargs

        async def generate():
            rows = self.rows if kwargs.get("oldest_first") else reversed(self.rows)
            for row in rows:
                yield row

        return generate()


class DiscordChatSummaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_module()

    def setUp(self):
        self.plugin = self.module.DiscordChatSummaryVerba()
        self.now = datetime(2026, 10, 21, 15, 30, tzinfo=timezone.utc)

    def test_metadata_is_discord_only_and_natural_language_first(self):
        self.assertEqual(self.plugin.name, "discord_chat_summary")
        self.assertEqual(self.plugin.platforms, ["discord"])
        self.assertEqual(self.plugin.argument_schema["required"], ["query"])
        self.assertEqual(set(self.plugin.argument_schema["properties"]), {"query"})
        self.assertIn("current channel", self.plugin.description)

    def test_deterministic_time_windows_cover_today_and_last_two_days(self):
        today = self.plugin._deterministic_window("give me a summary of chat today", self.now)
        self.assertEqual(today["start"], datetime(2026, 10, 21, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(today["end"], self.now)

        rolling = self.plugin._deterministic_window(
            "tell me what they talked about over the last two days",
            self.now,
        )
        self.assertEqual(rolling["start"], self.now - timedelta(days=2))
        self.assertEqual(rolling["end"], self.now)

    def test_reply_context_resolves_this_user(self):
        replied_author = _author(42, "Alex")
        replied_message = types.SimpleNamespace(author=replied_author)
        message = types.SimpleNamespace(
            reference=types.SimpleNamespace(resolved=replied_message, cached_message=None)
        )
        self.assertEqual(
            self.plugin._reply_author_id(message, "summarize what this user said today"),
            "42",
        )

    def test_name_filter_rejects_ambiguous_partial_names(self):
        records = [
            {"author_id": "1", "author": "Alex One", "aliases": ["Alex One"]},
            {"author_id": "2", "author": "Alex Two", "aliases": ["Alex Two"]},
        ]
        filtered, _people, error = self.plugin._filter_people(
            records,
            {"user_ids": set(), "people": ["Alex"]},
        )
        self.assertEqual(filtered, [])
        self.assertIn("More than one person", error)

    def test_handler_reads_live_history_filters_mentioned_user_and_excludes_request(self):
        alex = _author(42, "Alex")
        sam = _author(7, "Sam")
        request_author = _author(100, "Requester")
        rows = [
            _history_message(1, self.now - timedelta(hours=3), alex, "We should ship Friday."),
            _history_message(2, self.now - timedelta(hours=2), sam, "I can review it."),
            _history_message(3, self.now - timedelta(hours=1), alex, "The migration is ready."),
            _history_message(999, self.now - timedelta(minutes=1), request_author, "summarize <@42> today"),
        ]
        channel = _Channel(rows)
        message = types.SimpleNamespace(
            id=999,
            channel=channel,
            reference=None,
        )
        plan = {
            "start": self.now - timedelta(days=1),
            "end": self.now,
            "label": "today",
            "explicit": True,
            "user_ids": {"42"},
            "people": [],
            "focus": "",
        }

        with patch.object(self.plugin, "_now", return_value=self.now), patch.object(
            self.plugin,
            "_interpret_request",
            AsyncMock(return_value=plan),
        ), patch.object(
            self.plugin,
            "_summarize_records",
            AsyncMock(return_value="Alex discussed the Friday shipment and said the migration was ready."),
        ) as summarize:
            result = asyncio.run(
                self.plugin.handle_discord(
                    message,
                    {"query": "summarize what <@42> talked about today"},
                    object(),
                )
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["result_type"], "research")
        self.assertIn("Messages summarized: 2", result["answer"])
        self.assertIn("Scope: Alex", result["answer"])
        self.assertIn("migration was ready", result["answer"])
        summarized_records = summarize.await_args.args[0]
        self.assertEqual([record["author_id"] for record in summarized_records], ["42", "42"])
        self.assertEqual(channel.history_kwargs["limit"], None)
        self.assertFalse(channel.history_kwargs["oldest_first"])
        self.assertEqual(channel.history_kwargs["after"], plan["start"])
        self.assertEqual(channel.history_kwargs["before"], plan["end"])

    def test_handler_reports_read_history_permission_failure(self):
        class BrokenChannel:
            name = "private"

            def history(self, **_kwargs):
                raise PermissionError("Forbidden")

        message = types.SimpleNamespace(id=5, channel=BrokenChannel(), reference=None)
        plan = {
            "start": self.now - timedelta(days=1),
            "end": self.now,
            "label": "today",
            "user_ids": set(),
            "people": [],
            "focus": "",
        }
        with patch.object(self.plugin, "_now", return_value=self.now), patch.object(
            self.plugin,
            "_interpret_request",
            AsyncMock(return_value=plan),
        ):
            result = asyncio.run(
                self.plugin.handle_discord(message, {"query": "summarize today"}, object())
            )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "history_unavailable")
        self.assertIn("Read Message History", result["needs"][0])


if __name__ == "__main__":
    unittest.main()
