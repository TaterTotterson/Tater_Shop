import json
import logging
import re
from typing import Any, Dict, Set

from helpers import extract_json, redis_client
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("irc_admin_topic")
logger.setLevel(logging.INFO)


def _decode_map(raw: Dict[Any, Any] | None) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, value in (raw or {}).items():
        k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
        if isinstance(value, (bytes, bytearray)):
            out[k] = value.decode("utf-8", "ignore")
        elif value is None:
            out[k] = ""
        else:
            out[k] = str(value)
    return out


def _load_irc_settings() -> Dict[str, str]:
    raw = (
        redis_client.hgetall("irc_portal_settings")
        or redis_client.hgetall("platform_settings:IRC Settings")
        or redis_client.hgetall("platform_settings:IRC")
        or {}
    )
    return _decode_map(raw)


def _admin_nicks() -> Set[str]:
    raw = (_load_irc_settings().get("admin_nick") or "").strip()
    if not raw:
        return set()
    return {token.strip().lower() for token in re.split(r"[,\n]+", raw) if token.strip()}


class IrcAdminTopicPlugin(ToolVerba):
    name = "irc_admin_topic"
    verba_name = "IRC Admin Topic"
    pretty_name = "IRC Admin Topic"
    version = "1.0.0"
    min_tater_version = "59"
    tags = ["irc", "admin"]
    platforms = ["irc"]
    usage = '{"function":"irc_admin_topic","arguments":{"query":"set topic to hello world"}}'
    description = "Admin-gated IRC topic control for the current channel. Replaces topic text each run."
    verba_dec = "Set the current IRC channel topic (admin-only)."
    when_to_use = (
        "Use when an IRC admin asks to set or replace the current channel topic."
    )
    common_needs = []
    missing_info_prompts = []
    waiting_prompt_template = (
        "Write one short status message for {mention} saying you are updating the IRC channel topic now. "
        "Plain text only."
    )

    @staticmethod
    def _request_text(args: Dict[str, Any], raw_message: str) -> str:
        source = args if isinstance(args, dict) else {}
        for key in ("query", "text", "request", "command", "topic"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return str(raw_message or "").strip()

    @staticmethod
    async def _extract_topic_text(request_text: str, llm_client: Any) -> str:
        text = str(request_text or "").strip()
        if not text:
            return ""
        if llm_client is None or not hasattr(llm_client, "chat"):
            return ""

        system_prompt = (
            "Extract the desired IRC channel topic from user text.\n"
            "Return strict JSON only: {\"topic\":\"<topic text>|\"}\n"
            "Rules:\n"
            "- Return only the final topic content.\n"
            "- Remove command wrappers like 'set topic to'.\n"
            "- If no clear topic is present, return empty topic.\n"
            "- Keep original wording/casing of the topic content as much as possible.\n"
            "- No prose. JSON only.\n"
        )
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text},
                ],
                max_tokens=120,
                temperature=0.0,
            )
        except Exception as exc:
            logger.warning("[irc_admin_topic] topic extraction failed: %s", exc)
            return ""

        content = str((resp.get("message") or {}).get("content", "") or "").strip()
        if not content:
            return ""

        parsed = None
        try:
            parsed = json.loads(content)
        except Exception:
            blob = extract_json(content)
            if blob:
                try:
                    parsed = json.loads(blob)
                except Exception:
                    parsed = None
        if not isinstance(parsed, dict):
            return ""

        topic = str(parsed.get("topic") or "").strip()
        return topic

    @staticmethod
    def _send_topic(bot: Any, channel: str, topic: str) -> bool:
        line = f"TOPIC {channel} :{topic}"

        sender = getattr(bot, "send_line", None)
        if callable(sender):
            sender(line)
            return True

        sender = getattr(bot, "send", None)
        if callable(sender):
            sender(line)
            return True

        topic_fn = getattr(bot, "topic", None)
        if callable(topic_fn):
            topic_fn(channel, topic)
            return True

        privmsg = getattr(bot, "privmsg", None)
        if callable(privmsg):
            privmsg("ChanServ", f"TOPIC {channel} {topic}")
            return True

        return False

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        nick = str(user or "").strip()
        chan = str(channel or "").strip()
        if not nick:
            return action_failure(
                code="missing_user",
                message="Could not determine your IRC nickname.",
                say_hint="Explain that the IRC nickname is missing and retry from channel chat.",
            )
        if not chan.startswith(("#", "&")):
            return action_failure(
                code="channel_required",
                message="This command only works from an IRC channel.",
                say_hint="Explain topic changes can only be requested from a channel context.",
            )

        allowed = _admin_nicks()
        if not allowed:
            return action_failure(
                code="admin_only",
                message="IRC admin is not configured. Set `admin_nick` in IRC settings first.",
                say_hint="Explain this tool is admin-gated and requires admin_nick in IRC settings.",
            )
        if nick.lower() not in allowed:
            return action_failure(
                code="admin_only",
                message="This tool is restricted to the configured IRC admin nick(s).",
                say_hint="Explain this tool is admin-gated and the caller is not allowed.",
            )

        request_text = self._request_text(args or {}, raw_message or "")
        topic_text = await self._extract_topic_text(request_text, llm_client)
        if not topic_text:
            return action_failure(
                code="missing_topic",
                message="I couldn't find the topic text to set. Say it like: set topic to <text>.",
                needs=["Provide the exact topic text to set."],
                say_hint="Ask the user to provide the topic content to set.",
            )

        try:
            sent = self._send_topic(bot, chan, topic_text)
        except Exception as exc:
            logger.exception("[irc_admin_topic] failed sending topic command: %s", exc)
            sent = False
        if not sent:
            return action_failure(
                code="irc_command_failed",
                message="Failed to send IRC TOPIC command.",
                say_hint="Explain the IRC topic command could not be sent.",
            )

        return action_success(
            facts={
                "action": "set_topic",
                "channel": chan,
                "nick": nick,
                "topic": topic_text,
            },
            summary_for_user=f"Set topic for {chan} to: {topic_text}",
            say_hint="Confirm the channel topic was updated.",
        )


verba = IrcAdminTopicPlugin()
