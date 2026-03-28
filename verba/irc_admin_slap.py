import logging
import re
from typing import Any, Dict, Set

from helpers import get_tater_name, redis_client
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("irc_admin_slap")
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


class IrcAdminSlapPlugin(ToolVerba):
    name = "irc_admin_slap"
    verba_name = "IRC Admin Slap"
    pretty_name = "IRC Admin Slap"
    version = "1.0.0"
    min_tater_version = "59"
    tags = ["irc", "admin"]
    platforms = ["irc"]
    usage = '{"function":"irc_admin_slap","arguments":{"query":"slap john"}}'
    description = "Admin-gated playful IRC slap action for a target nick."
    verba_dec = "Send a playful trout slap action in IRC (admin-only)."
    when_to_use = "Use when an IRC admin asks to slap a target user (for fun)."
    common_needs = []
    missing_info_prompts = []
    waiting_prompt_template = (
        "Write one short status line for {mention} saying you are sending that slap now. "
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
    def _extract_target(args: Dict[str, Any], request_text: str) -> str:
        source = args if isinstance(args, dict) else {}
        for key in ("target", "nick", "user", "username", "who"):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                token = value.strip()
                token = token.lstrip("@").strip()
                token = token.strip(".,!?;:")
                if token:
                    return token

        text = str(request_text or "").strip()
        patterns = [
            r"\bslap\s+([^\s]+)",
            r"\btrout\s+slap\s+([^\s]+)",
            r"\bsmack\s+([^\s]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue
            token = (match.group(1) or "").strip()
            token = token.lstrip("@").strip()
            token = token.strip(".,!?;:")
            if token:
                return token
        return ""

    @staticmethod
    def _actor_name() -> str:
        try:
            first, _last = get_tater_name()
        except Exception:
            first = ""
        actor = str(first or "tater").strip().split()[0].strip().lower()
        return actor or "tater"

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        del bot, llm_client
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
                say_hint="Explain this action can only be used in a channel context.",
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
        target = self._extract_target(args or {}, request_text)
        if not target:
            return action_failure(
                code="missing_target",
                message="Who should I slap? Example: slap john",
                needs=["Provide the target nickname to slap."],
                say_hint="Ask the user who should be slapped.",
            )

        action_text = f"*{self._actor_name()} slaps {target} with a large trout*"
        return action_success(
            facts={
                "action": "slap",
                "channel": chan,
                "actor": self._actor_name(),
                "target": target,
            },
            summary_for_user=action_text,
            say_hint="Output exactly the action line and nothing else.",
        )


verba = IrcAdminSlapPlugin()
