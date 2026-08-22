#!/usr/bin/env python3
"""Focused compatibility tests for streamed portal replies."""

import asyncio
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch


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


class DiscordAttachmentTests(unittest.TestCase):
    def test_discord_mp3_alias_is_normalized_for_audio_understanding(self):
        self.assertEqual(
            discord_portal._normalize_discord_attachment_mimetype(
                "sm64_super_mario_64_theme.mp3",
                "audio/mpeg3",
            ),
            "audio/mpeg",
        )

    def test_discord_octet_stream_uses_filename_mimetype(self):
        self.assertEqual(
            discord_portal._normalize_discord_attachment_mimetype(
                "song.mp3",
                "application/octet-stream",
            ),
            "audio/mpeg",
        )

    def test_discord_supported_mimetype_is_preserved(self):
        self.assertEqual(
            discord_portal._normalize_discord_attachment_mimetype(
                "clip.wav",
                "audio/wav; charset=binary",
            ),
            "audio/wav",
        )


class _DiscordPermissions:
    def __init__(self, *, view_channel: bool = True, send_messages: bool = True):
        self.view_channel = view_channel
        self.send_messages = send_messages


class _DiscordGuild:
    def __init__(self, guild_id: int, name: str, *, unavailable: bool = False):
        self.id = guild_id
        self.name = name
        self.unavailable = unavailable
        self.me = object()
        self.text_channels = []


class _DiscordInventoryChannel:
    def __init__(
        self,
        channel_id: int,
        name: str,
        guild: _DiscordGuild,
        *,
        news: bool = False,
        view_channel: bool = True,
        send_messages: bool = True,
        position: int = 0,
    ):
        self.id = channel_id
        self.name = name
        self.guild = guild
        self.position = position
        self._news = news
        self._permissions = _DiscordPermissions(
            view_channel=view_channel,
            send_messages=send_messages,
        )

    def permissions_for(self, member):
        return self._permissions

    def is_news(self):
        return self._news


class DiscordDestinationInventoryTests(unittest.TestCase):
    def test_inventory_includes_text_and_announcement_channels(self):
        guild = _DiscordGuild(10, "Tater Town")
        general = _DiscordInventoryChannel(101, "general", guild, position=2)
        announcements = _DiscordInventoryChannel(102, "announcements", guild, news=True, position=1)
        guild.text_channels = [general, announcements]

        channels = discord_portal._discord_sendable_channels([guild])
        payload = discord_portal._discord_destination_inventory_payload(channels)

        self.assertEqual(
            [row["channel_id"] for row in payload["destinations"]],
            ["102", "101"],
        )
        self.assertEqual(payload["destinations"][0]["channel_type"], "announcement")
        self.assertEqual(payload["destinations"][1]["channel_type"], "text")
        self.assertEqual(payload["destinations"][0]["guild_name"], "Tater Town")

    def test_inventory_excludes_channels_the_bot_cannot_send_to(self):
        guild = _DiscordGuild(10, "Tater Town")
        guild.text_channels = [
            _DiscordInventoryChannel(101, "general", guild),
            _DiscordInventoryChannel(102, "read-only", guild, send_messages=False),
            _DiscordInventoryChannel(103, "hidden", guild, view_channel=False),
        ]

        channels = discord_portal._discord_sendable_channels([guild])

        self.assertEqual([channel.id for channel in channels], [101])

    def test_inventory_can_exclude_deleted_channel_or_departed_guild(self):
        first = _DiscordGuild(10, "First")
        second = _DiscordGuild(20, "Second")
        first.text_channels = [_DiscordInventoryChannel(101, "general", first)]
        second.text_channels = [_DiscordInventoryChannel(201, "general", second)]

        without_channel = discord_portal._discord_sendable_channels(
            [first, second],
            excluded_channel_ids=[101],
        )
        without_guild = discord_portal._discord_sendable_channels(
            [first, second],
            excluded_guild_ids=[20],
        )

        self.assertEqual([channel.id for channel in without_channel], [201])
        self.assertEqual([channel.id for channel in without_guild], [101])


class _DiscordRedisPipeline:
    def __init__(self, client):
        self.client = client
        self.operations = []

    def rpush(self, key, *values):
        self.operations.append(("rpush", key, values))
        return self

    def delete(self, *keys):
        self.operations.append(("delete", keys))
        return self

    def execute(self):
        for operation in self.operations:
            if operation[0] == "rpush":
                _, key, values = operation
                self.client.rpush(key, *values)
            else:
                _, keys = operation
                self.client.delete(*keys)
        return []


class _DiscordInventoryRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}
        self.hashes = {}

    def set(self, key, value):
        self.values[str(key)] = value
        return True

    def get(self, key):
        return self.values.get(str(key))

    def scan_iter(self, match):
        import fnmatch

        for key in sorted(set(self.values) | set(self.lists)):
            if fnmatch.fnmatch(key, match):
                yield key

    def lrange(self, key, start, end):
        return list(self.lists.get(str(key)) or [])

    def rpush(self, key, *values):
        self.lists.setdefault(str(key), []).extend(values)
        return len(self.lists[str(key)])

    def delete(self, *keys):
        for key in keys:
            self.values.pop(str(key), None)
            self.lists.pop(str(key), None)
        return len(keys)

    def hset(self, key, field, value):
        self.hashes.setdefault(str(key), {})[str(field)] = value
        return 1

    def pipeline(self):
        return _DiscordRedisPipeline(self)


class DiscordDestinationReconciliationTests(unittest.TestCase):
    def test_reconcile_removes_stale_records_and_archives_history(self):
        client = _DiscordInventoryRedis()
        client.values["tater:room_label:discord:101"] = "#current"
        client.values["tater:room_meta:discord:101"] = json.dumps({"guild_id": "10"})
        client.values["tater:room_label:discord:202"] = "#retired"
        client.values["tater:room_meta:discord:202"] = json.dumps({"guild_id": "20"})
        client.lists["tater:channel:101:history"] = ["current message"]
        client.lists["tater:channel:202:history"] = ["retired message"]
        client.lists["tater:discord:old-room:history"] = ["legacy message"]

        with patch.object(discord_portal, "redis_client", client):
            retired = discord_portal._reconcile_discord_destination_records(["101"])

        self.assertEqual(retired, 2)
        self.assertIn("tater:room_label:discord:101", client.values)
        self.assertNotIn("tater:room_label:discord:202", client.values)
        self.assertEqual(client.lists["tater:channel:101:history"], ["current message"])
        self.assertNotIn("tater:channel:202:history", client.lists)
        self.assertEqual(
            client.lists["tater:archive:discord_channel_history:v1:202"],
            ["retired message"],
        )
        self.assertEqual(
            client.lists["tater:archive:discord_legacy_history:v1:old-room"],
            ["legacy message"],
        )

    def test_response_channel_map_keeps_only_live_channels(self):
        filtered = discord_portal._filter_response_channel_map(
            {10: {101, 102}, 20: {201}},
            ["102", "201"],
        )

        self.assertEqual(filtered, {10: {102}, 20: {201}})

    def test_reconcile_preserves_rooms_from_temporarily_unavailable_guild(self):
        client = _DiscordInventoryRedis()
        client.values["tater:room_label:discord:202"] = "#temporarily-unavailable"
        client.values["tater:room_meta:discord:202"] = json.dumps({"guild_id": "20"})
        client.lists["tater:channel:202:history"] = ["preserve me"]

        with patch.object(discord_portal, "redis_client", client):
            retired = discord_portal._reconcile_discord_destination_records(
                [],
                protected_guild_ids=["20"],
            )

        self.assertEqual(retired, 0)
        self.assertIn("tater:room_label:discord:202", client.values)
        self.assertEqual(client.lists["tater:channel:202:history"], ["preserve me"])


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
    def test_current_discord_speaker_is_explicit_and_history_is_not_identity(self):
        prompt = discord_portal.discord_portal.build_system_prompt(
            object(),
            current_speaker="KnightInd",
            current_user_id="current-user-id",
        )

        self.assertIn('display_label="KnightInd"', prompt)
        self.assertIn('identity="current-user-id"', prompt)
        self.assertIn("Only the latest user message belongs to this speaker", prompt)
        self.assertIn("names on older history messages belong to those older speakers", prompt)
        self.assertIn("claim inside message text", prompt)

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
