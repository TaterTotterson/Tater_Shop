#!/usr/bin/env python3
"""Focused compatibility tests for streamed portal replies."""

import asyncio
import importlib.util
import sys
import unittest
from pathlib import Path
from typing import Any, Dict


SHOP_ROOT = Path(__file__).resolve().parents[1]
TATER_ROOT = SHOP_ROOT.parent / "Tater"
sys.path.insert(0, str(TATER_ROOT))


def _load_portal(name: str):
    path = SHOP_ROOT / "portals" / f"{name}_portal.py"
    spec = importlib.util.spec_from_file_location(f"stream_test_{name}_portal", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


discord_portal = _load_portal("discord")
telegram_portal = _load_portal("telegram")
matrix_portal = _load_portal("matrix")


class _DiscordMessage:
    def __init__(self, content: str, *, edit_failures: int = 0, delete_fails: bool = False):
        self.content = content
        self.edits = []
        self.deleted = False
        self.edit_failures = edit_failures
        self.delete_fails = delete_fails

    async def edit(self, *, content: str):
        if self.edit_failures > 0:
            self.edit_failures -= 1
            raise RuntimeError("transient Discord edit failure")
        self.content = content
        self.edits.append(content)

    async def delete(self):
        if self.delete_fails:
            raise RuntimeError("Discord delete failure")
        self.deleted = True


class _DiscordChannel:
    def __init__(self, *, first_edit_failures: int = 0, first_delete_fails: bool = False):
        self.messages = []
        self.first_edit_failures = first_edit_failures
        self.first_delete_fails = first_delete_fails

    async def send(self, content: str):
        message = _DiscordMessage(
            content,
            edit_failures=self.first_edit_failures if not self.messages else 0,
            delete_fails=self.first_delete_fails if not self.messages else False,
        )
        self.messages.append(message)
        return message


class _TelegramApi:
    def __init__(self):
        self.calls = []
        self.next_message_id = 100

    def __call__(
        self,
        method: str,
        payload: Dict[str, Any],
        timeout: int,
    ) -> Dict[str, Any]:
        self.calls.append((method, dict(payload), timeout))
        if method == "sendMessage":
            self.next_message_id += 1
            return {"ok": True, "result": {"message_id": self.next_message_id}}
        return {"ok": True, "result": True}


class _MatrixPlatform:
    def __init__(self, *, replacement_failures: int = 0):
        self.events = []
        self.sent = []
        self.redacted = []
        self.replacement_failures = replacement_failures

    async def _send_stream_text_event(
        self,
        room_id: str,
        text: str,
        *,
        replacement_event_id: str = "",
        formatted: bool = False,
    ) -> str:
        if replacement_event_id and self.replacement_failures > 0:
            self.replacement_failures -= 1
            raise RuntimeError("Matrix replacement failure")
        self.events.append(
            {
                "room_id": room_id,
                "text": text,
                "replacement_event_id": replacement_event_id,
                "formatted": formatted,
            }
        )
        return "$preview"

    async def _send_with_trust(self, room_id: str, text: str):
        self.sent.append((room_id, text))

    async def _redact_stream_event(self, room_id: str, event_id: str):
        self.redacted.append((room_id, event_id))


class DiscordStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_shot_reply_keeps_normal_send_path(self):
        channel = _DiscordChannel()
        stream = discord_portal._DiscordReplyStream(channel, max_length=2000)
        stream.on_chunk("complete response")

        delivered = await stream.finish("complete response")

        self.assertFalse(delivered)
        self.assertEqual(channel.messages, [])

    async def test_incremental_reply_uses_preview_then_final_edit(self):
        channel = _DiscordChannel()
        stream = discord_portal._DiscordReplyStream(channel, max_length=2000)
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("hello")

        self.assertTrue(delivered)
        self.assertEqual(len(channel.messages), 1)
        self.assertTrue(channel.messages[0].edits)
        self.assertEqual(channel.messages[0].content, "hello")

    async def test_finalization_does_not_cancel_inflight_preview_send(self):
        channel = _DiscordChannel()
        stream = discord_portal._DiscordReplyStream(channel, max_length=2000)
        stream.text = "Spud Lord"
        stream.chunk_count = 3
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_start():
            started.set()
            await release.wait()
            stream.message = _DiscordMessage("Spud Lord ▌")
            stream.last_sent_text = stream.text
            stream.update_count = 1

        stream._start_task = asyncio.create_task(slow_start())
        stream._flush_task = asyncio.create_task(stream._flush_after_delay())
        await started.wait()
        finish_task = asyncio.create_task(stream.finish("Spud Lord, I’m here!"))
        await asyncio.sleep(0)
        release.set()

        delivered = await asyncio.wait_for(finish_task, timeout=1)

        self.assertTrue(delivered)
        self.assertFalse(stream._start_task.cancelled())
        self.assertEqual(stream.message.content, "Spud Lord, I’m here!")

    async def test_missing_final_payload_uses_accumulated_stream(self):
        channel = _DiscordChannel()
        stream = discord_portal._DiscordReplyStream(channel, max_length=2000)
        stream.on_chunk("Spud")
        stream.on_chunk(" Lord")
        stream.on_chunk(", the full reply arrived.")

        delivered = await stream.finish("")

        self.assertTrue(delivered)
        self.assertEqual(channel.messages[0].content, "Spud Lord, the full reply arrived.")
        self.assertNotIn("▌", channel.messages[0].content)

    async def test_final_edit_retries_before_falling_back(self):
        channel = _DiscordChannel(first_edit_failures=1)
        stream = discord_portal._DiscordReplyStream(channel, max_length=2000)
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("hello")

        self.assertTrue(delivered)
        self.assertEqual(len(channel.messages), 1)
        self.assertEqual(channel.messages[0].content, "hello")

    async def test_failed_final_edit_sends_answer_and_removes_preview(self):
        channel = _DiscordChannel(first_edit_failures=3)
        stream = discord_portal._DiscordReplyStream(channel, max_length=2000)
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("hello")

        self.assertTrue(delivered)
        self.assertEqual(len(channel.messages), 2)
        self.assertTrue(channel.messages[0].deleted)
        self.assertEqual(channel.messages[1].content, "hello")


class TelegramStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_shot_reply_keeps_normal_send_path(self):
        api = _TelegramApi()
        stream = telegram_portal._TelegramReplyStream(
            api,
            chat_id="123",
            private_chat=True,
            draft_id=7,
        )
        stream.on_chunk("complete response")

        delivered = await stream.finish("complete response")

        self.assertFalse(delivered)
        self.assertEqual(api.calls, [])

    async def test_private_incremental_reply_uses_ephemeral_draft(self):
        api = _TelegramApi()
        stream = telegram_portal._TelegramReplyStream(
            api,
            chat_id="123",
            private_chat=True,
            draft_id=7,
        )
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("hello")

        self.assertTrue(delivered)
        self.assertEqual(
            [call[0] for call in api.calls],
            ["sendMessageDraft", "sendMessage"],
        )
        self.assertEqual(api.calls[0][1]["draft_id"], 7)

    async def test_missing_final_payload_uses_buffered_draft_text(self):
        api = _TelegramApi()
        stream = telegram_portal._TelegramReplyStream(
            api,
            chat_id="123",
            private_chat=True,
            draft_id=7,
        )
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("")

        self.assertTrue(delivered)
        self.assertEqual(
            [call[0] for call in api.calls],
            ["sendMessageDraft", "sendMessage"],
        )
        self.assertEqual(api.calls[-1][1]["text"], "hello")

    async def test_group_incremental_reply_finalizes_edited_message(self):
        api = _TelegramApi()
        stream = telegram_portal._TelegramReplyStream(
            api,
            chat_id="-123",
            private_chat=False,
            draft_id=7,
        )
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("hello")

        self.assertTrue(delivered)
        self.assertEqual(
            [call[0] for call in api.calls],
            ["sendMessage", "editMessageText"],
        )
        self.assertEqual(api.calls[-1][1]["text"], "hello")

    async def test_finalization_does_not_cancel_inflight_preview_send(self):
        api = _TelegramApi()
        stream = telegram_portal._TelegramReplyStream(
            api,
            chat_id="123",
            private_chat=True,
            draft_id=7,
        )
        stream.text = "Spud Lord"
        stream.chunk_count = 3
        stream.mode = "draft"
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_start():
            started.set()
            await release.wait()
            stream.last_sent_text = stream.text
            stream.update_count = 1

        stream._start_task = asyncio.create_task(slow_start())
        stream._flush_task = asyncio.create_task(stream._flush_after_delay())
        await started.wait()
        finish_task = asyncio.create_task(stream.finish("Spud Lord, I’m here!"))
        await asyncio.sleep(0)
        release.set()

        delivered = await asyncio.wait_for(finish_task, timeout=1)

        self.assertTrue(delivered)
        self.assertFalse(stream._start_task.cancelled())
        self.assertEqual(api.calls[-1][0], "sendMessage")


class MatrixStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_shot_reply_keeps_normal_send_path(self):
        platform = _MatrixPlatform()
        stream = matrix_portal._MatrixReplyStream(
            platform,
            room_id="!room:example.test",
            max_length=4000,
        )
        stream.on_chunk("complete response")

        delivered = await stream.finish("complete response")

        self.assertFalse(delivered)
        self.assertEqual(platform.events, [])

    async def test_incremental_reply_uses_original_event_for_replacement(self):
        platform = _MatrixPlatform()
        stream = matrix_portal._MatrixReplyStream(
            platform,
            room_id="!room:example.test",
            max_length=4000,
        )
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("hello")

        self.assertTrue(delivered)
        self.assertEqual(len(platform.events), 2)
        self.assertEqual(platform.events[1]["replacement_event_id"], "$preview")
        self.assertTrue(platform.events[1]["formatted"])
        self.assertEqual(platform.events[1]["text"], "hello")

    async def test_finalization_does_not_cancel_inflight_preview_send(self):
        platform = _MatrixPlatform()
        stream = matrix_portal._MatrixReplyStream(
            platform,
            room_id="!room:example.test",
            max_length=4000,
        )
        stream.text = "Spud Lord"
        stream.chunk_count = 3
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_start():
            started.set()
            await release.wait()
            stream.event_id = "$preview"
            stream.last_sent_text = stream.text
            stream.update_count = 1

        stream._start_task = asyncio.create_task(slow_start())
        stream._flush_task = asyncio.create_task(stream._flush_after_delay())
        await started.wait()
        finish_task = asyncio.create_task(stream.finish("Spud Lord, I’m here!"))
        await asyncio.sleep(0)
        release.set()

        delivered = await asyncio.wait_for(finish_task, timeout=1)

        self.assertTrue(delivered)
        self.assertFalse(stream._start_task.cancelled())
        self.assertEqual(platform.events[-1]["text"], "Spud Lord, I’m here!")

    async def test_missing_final_payload_uses_buffered_stream_text(self):
        platform = _MatrixPlatform()
        stream = matrix_portal._MatrixReplyStream(
            platform,
            room_id="!room:example.test",
            max_length=4000,
        )
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("")

        self.assertTrue(delivered)
        self.assertEqual(platform.events[-1]["text"], "hello")
        self.assertTrue(platform.events[-1]["formatted"])

    async def test_failed_replacement_sends_normal_final_and_redacts_preview(self):
        platform = _MatrixPlatform(replacement_failures=1)
        stream = matrix_portal._MatrixReplyStream(
            platform,
            room_id="!room:example.test",
            max_length=4000,
        )
        stream.on_chunk("hel")
        stream.on_chunk("lo")

        delivered = await stream.finish("hello")

        self.assertTrue(delivered)
        self.assertEqual(platform.sent, [("!room:example.test", "hello")])
        self.assertEqual(platform.redacted, [("!room:example.test", "$preview")])

    def test_replacement_content_uses_matrix_standard_relation(self):
        content = matrix_portal.MatrixPlatform._stream_message_content(
            "hello",
            replacement_event_id="$preview",
            formatted=False,
        )

        self.assertEqual(content["m.relates_to"]["rel_type"], "m.replace")
        self.assertEqual(content["m.relates_to"]["event_id"], "$preview")
        self.assertEqual(content["m.new_content"]["body"], "hello")


if __name__ == "__main__":
    unittest.main()
