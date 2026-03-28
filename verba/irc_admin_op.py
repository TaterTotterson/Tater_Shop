import json
import logging
import re
from typing import Any, Dict, Set

from helpers import extract_json, redis_client
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("irc_admin_op")
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


class IrcAdminOpPlugin(ToolVerba):
    name = "irc_admin_op"
    verba_name = "IRC Admin OP"
    pretty_name = "IRC Admin OP"
    version = "1.3.1"
    min_tater_version = "59"
    tags = ["irc", "admin"]
    platforms = ["irc"]
    usage = '{"function":"irc_admin_op","arguments":{"query":"op me"}}'
    description = "Gives the user op or voice in the current IRC channel."
    verba_dec = "Gives the user op or voice in the current IRC channel."
    when_to_use = (
        "Use when an IRC admin explicitly asks to op themselves or voice themselves "
        "in the current channel."
    )
    common_needs = []
    missing_info_prompts = []
    waiting_prompt_template = (
        "Write one short status message for {mention} saying you are sending that IRC admin command now. "
        "Plain text only."
    )

    @staticmethod
    def _request_text(args: Dict[str, Any], raw_message: str) -> str:
        source = args if isinstance(args, dict) else {}
        for key in ("query", "text", "request", "command"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return str(raw_message or "").strip()

    @staticmethod
    async def _requested_action_ai(text: str, llm_client: Any) -> str:
        if not str(text or "").strip():
            return ""
        if llm_client is None or not hasattr(llm_client, "chat"):
            return ""

        system_prompt = (
            "Classify IRC admin intent.\n"
            "Return strict JSON only with shape: {\"action\":\"op|voice|unknown\"}\n"
            "Rules:\n"
            "- Select op when the user asks for operator/ops/+o privileges.\n"
            "- Select voice when the user asks for voice/+v privileges.\n"
            "- Select unknown when ambiguous, mixed, or unrelated.\n"
            "- No prose. JSON only."
        )
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": str(text)},
                ],
                max_tokens=60,
                temperature=0.0,
            )
        except Exception as exc:
            logger.warning("[irc_admin] action AI classify failed: %s", exc)
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

        action = str(parsed.get("action") or "").strip().lower()
        if action in {"op", "voice"}:
            return action
        return ""

    async def _requested_action(self, text: str, llm_client: Any) -> str:
        return await self._requested_action_ai(text, llm_client)

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
                say_hint="Explain op/voice can only be requested from a channel context.",
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

        query = self._request_text(args or {}, raw_message or "")
        action = await self._requested_action(query, llm_client)
        if not action:
            return action_failure(
                code="invalid_command",
                message="Could not determine whether you want op or voice. Say it like `op me` or `voice me`.",
                needs=["Specify whether you want op or voice."],
                say_hint="Ask the user to clarify whether they want operator or voice.",
            )

        service_cmd = "OP" if action == "op" else "VOICE"
        try:
            bot.privmsg("ChanServ", f"{service_cmd} {chan} {nick}")
        except Exception as exc:
            logger.exception("[irc_admin] failed sending ChanServ command: %s", exc)
            return action_failure(
                code="irc_command_failed",
                message=f"Failed to send ChanServ {service_cmd} request.",
                say_hint="Explain the IRC service command could not be sent.",
            )

        return action_success(
            facts={
                "action": action,
                "channel": chan,
                "nick": nick,
                "service": "ChanServ",
            },
            summary_for_user=f"Requested ChanServ to {service_cmd} {nick} in {chan}.",
            say_hint="Confirm the IRC admin command was sent.",
        )


verba = IrcAdminOpPlugin()
