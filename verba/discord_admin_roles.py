import json
import logging
import os
import re
from typing import Any, Dict, Iterable, Optional

import discord

from verba_base import ToolVerba
from helpers import extract_json, redis_blob_client, redis_client
from verba_result import action_failure, action_success

logger = logging.getLogger("discord_admin_roles")
logger.setLevel(logging.INFO)


DISCORD_SETTINGS_KEY = "discord_portal_settings"
RESPONSE_CHANNEL_IDS_BY_GUILD_KEY = "response_channel_ids_by_guild"
MAX_ROLES = 10
MAX_CATEGORIES = 8
MAX_CHANNELS_PER_CATEGORY = 12
ICON_MAX_BYTES = 10 * 1024 * 1024
VALID_ROLE_PERMISSIONS = set(getattr(discord.Permissions, "VALID_FLAGS", {}).keys())
VALID_CHANNEL_PERMISSIONS = set(getattr(discord.Permissions, "VALID_FLAGS", {}).keys())
VALID_RESPONSE_ACTIONS = {"none", "add_current", "remove_current", "set_only_current", "clear_all"}
DELETION_DISABLED_TEXT = (
    "Channel/category deletion is disabled in this plugin for safety. "
    "Please delete channels or categories manually in Discord."
)
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


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _parse_response_channel_ids(raw: Any) -> set[int]:
    if raw is None:
        return set()
    if isinstance(raw, (list, tuple, set)):
        parts = [str(item or "").strip() for item in raw]
    else:
        text = str(raw or "").strip()
        if not text:
            return set()
        parts = [token.strip() for token in re.split(r"[,\s]+", text) if token.strip()]

    out: set[int] = set()
    for part in parts:
        token = str(part or "").strip().strip("<>#")
        if token.startswith("!"):
            token = token[1:]
        if not token:
            continue
        try:
            parsed = int(token)
        except Exception:
            continue
        if parsed > 0:
            out.add(parsed)
    return out


def _serialize_response_channel_ids(values: Iterable[int]) -> str:
    cleaned: set[int] = set()
    for value in values or []:
        try:
            parsed = int(value)
        except Exception:
            continue
        if parsed > 0:
            cleaned.add(parsed)
    return ",".join(str(item) for item in sorted(cleaned))


def _parse_response_channel_map(raw: Any) -> dict[int, set[int]]:
    payload = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except Exception:
            return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[int, set[int]] = {}
    for key, value in payload.items():
        try:
            guild_id = int(str(key).strip())
        except Exception:
            continue
        if guild_id <= 0:
            continue
        out[guild_id] = _parse_response_channel_ids(value)
    return out


def _serialize_response_channel_map(values: Dict[Any, Iterable[int]] | None) -> str:
    out: Dict[str, str] = {}
    if isinstance(values, dict):
        for key, item in values.items():
            try:
                guild_id = int(str(key).strip())
            except Exception:
                continue
            if guild_id <= 0:
                continue
            out[str(guild_id)] = _serialize_response_channel_ids(item or [])
    return json.dumps(out, sort_keys=True, separators=(",", ":"))


def _text_channel_slug(name: str) -> str:
    text = str(name or "").strip().lower()
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^a-z0-9\-_]", "", text)
    text = re.sub(r"-{2,}", "-", text).strip("-_")
    return text[:90] if text else "chat"


def _clean_name(name: Any, fallback: str) -> str:
    text = str(name or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text[:95] if text else fallback


class DiscordAdminRolesPlugin(ToolVerba):
    name = "discord_admin_roles"
    verba_name = "Discord Admin Roles"
    pretty_name = "Discord Admin Roles"
    version = "1.0.0"
    min_tater_version = "59"
    tags = ["discord", "roles"]
    fixed_route = "role"
    platforms = ["discord"]
    routing_keywords = [
        "discord",
        "role",
        "roles",
        "role me",
        "role yourself",
        "list roles",
        "give me role",
        "assign me role",
        "grant me role",
        "role yourself",
    ]

    usage = (
        '{"function":"discord_admin_roles","arguments":{"request":"list roles"}}'
    )
    description = (
        "List server roles and assign requested roles to the caller or bot (role hierarchy permitting)."
    )
    when_to_use = (
        "Use for Discord role commands like list roles, role me, or role yourself."
    )
    how_to_use = (
        "Provide one role command in `request` or `query` (for example: list roles, give me the Builders role)."
    )
    verba_dec = "Discord role listing and role assignment commands."
    argument_schema = {
        "type": "object",
        "properties": {
            "request": {
                "type": "string",
                "description": "Role command request.",
            },
            "query": {
                "type": "string",
                "description": "The role command to run (for example: list roles, give me Builders).",
            },
            "role": {
                "type": "string",
                "description": "Optional role name hint.",
            },
            "roles": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional list of role names.",
            },
        },
        "required": [],
    }
    common_needs = [
        "A single Discord role command.",
    ]
    missing_info_prompts = []

    waiting_prompt_template = (
        "Write one short, natural status update to {mention} that you are working on their Discord admin request now. "
        "Use fresh wording (avoid repeating stock phrases). "
        "Do not use markdown. Only output the message."
    )

    @staticmethod
    def _normalize_permission_tokens(value: Any, *, valid: set[str]) -> list[str]:
        if isinstance(value, str):
            parts = [token.strip() for token in re.split(r"[,\n]+", value) if token.strip()]
        elif isinstance(value, list):
            parts = [str(item or "") for item in value]
        else:
            return []
        out: list[str] = []
        seen = set()
        for item in parts:
            token = str(item or "").strip().lower().replace(" ", "_")
            if not token or token not in valid or token in seen:
                continue
            seen.add(token)
            out.append(token)
        return out

    @staticmethod
    def _parse_color(value: Any) -> Optional[int]:
        if value is None:
            return None
        text = str(value).strip().lower()
        if not text:
            return None
        if text.startswith("#"):
            text = text[1:]
        try:
            if text.startswith("0x"):
                parsed = int(text, 16)
            elif re.fullmatch(r"[0-9a-f]{6}", text):
                parsed = int(text, 16)
            else:
                parsed = int(text)
        except Exception:
            return None
        if parsed < 0:
            return None
        return min(parsed, 0xFFFFFF)

    def _load_discord_settings(self) -> Dict[str, str]:
        return _decode_map(redis_client.hgetall(DISCORD_SETTINGS_KEY) or {})

    def _load_response_channel_map(self, settings: Dict[str, str] | None = None) -> dict[int, set[int]]:
        data = settings if isinstance(settings, dict) else self._load_discord_settings()
        return _parse_response_channel_map(data.get(RESPONSE_CHANNEL_IDS_BY_GUILD_KEY))

    def _load_response_channel_ids_for_guild(
        self,
        guild_id: int,
        settings: Dict[str, str] | None = None,
    ) -> set[int]:
        data = settings if isinstance(settings, dict) else self._load_discord_settings()
        channel_map = self._load_response_channel_map(settings=data)
        gid = int(guild_id or 0)
        if gid <= 0:
            return set()
        return set(channel_map.get(gid) or set())

    def _save_response_channel_ids_for_guild(self, guild_id: int, values: set[int]) -> None:
        gid = int(guild_id or 0)
        if gid <= 0:
            return
        settings = self._load_discord_settings()
        channel_map = self._load_response_channel_map(settings=settings)
        channel_map[gid] = set(values)
        redis_client.hset(
            DISCORD_SETTINGS_KEY,
            mapping={
                RESPONSE_CHANNEL_IDS_BY_GUILD_KEY: _serialize_response_channel_map(channel_map),
            },
        )

    @staticmethod
    def _response_label(guild, channel_ids: set[int]) -> str:
        if not channel_ids:
            return "mention-only"
        labels: list[str] = []
        for channel_id in sorted(channel_ids):
            channel = None
            if guild is not None and hasattr(guild, "get_channel"):
                try:
                    channel = guild.get_channel(channel_id)
                except Exception:
                    channel = None
            if channel is not None and getattr(channel, "name", None):
                labels.append(f"#{channel.name}")
            else:
                labels.append(str(channel_id))
        return ", ".join(labels)

    @staticmethod
    def _refresh_runtime_response_channels(message, guild_id: int, values: set[int]) -> None:
        bot = getattr(message, "client", None)
        if bot is None:
            return
        if hasattr(bot, "set_guild_response_channels"):
            try:
                bot.set_guild_response_channels(guild_id, values)
                return
            except Exception:
                pass
        if hasattr(bot, "set_response_channels"):
            try:
                bot.set_response_channels(values)
            except Exception:
                pass

    @staticmethod
    def _extract_request(args: Dict[str, Any] | None, message) -> str:
        payload = args or {}
        for key in ("request", "query", "prompt", "text", "message", "theme"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        content = getattr(message, "content", "")
        return str(content or "").strip()

    @staticmethod
    def _role_position(role: Any) -> int:
        try:
            return int(getattr(role, "position", 0) or 0)
        except Exception:
            return 0

    def _normalize_role_command_text(self, text: str, message) -> str:
        out = str(text or "").strip()
        if not out:
            return ""
        bot_obj = getattr(getattr(message, "client", None), "user", None)
        bot_id = int(getattr(bot_obj, "id", 0) or 0)
        if bot_id > 0:
            out = re.sub(rf"<@!?{bot_id}>", " ", out)
        bot_aliases: list[str] = []
        for attr in ("display_name", "name"):
            value = str(getattr(bot_obj, attr, "") or "").strip()
            if value:
                bot_aliases.append(value)
        for alias in bot_aliases:
            out = re.sub(rf"^\s*{re.escape(alias)}[\s,:-]+", "", out, flags=re.IGNORECASE)
        out = " ".join(out.split())
        return out.strip()

    @staticmethod
    def _normalized_request_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        return " ".join(text.split())

    @classmethod
    def _request_delegates_creative_choices(cls, request_text: str) -> bool:
        text = cls._normalized_request_text(request_text)
        if not text:
            return False
        delegate_phrases = (
            "be creative",
            "choose names yourself",
            "pick names yourself",
            "you decide",
            "surprise me",
            "your call",
            "make it your own",
            "style it",
            "design it",
            "setup this discord",
            "set up this discord",
            "setup this server",
            "set up this server",
        )
        if any(phrase in text for phrase in delegate_phrases):
            return True
        if " style" in text or text.endswith(" style"):
            return True
        return False

    @classmethod
    def _infer_setup_route_from_request(cls, request_text: str) -> Dict[str, Any]:
        text = cls._normalized_request_text(request_text)
        if not text:
            return {"route": "none", "setup_requested": False, "setup_mode": "none"}

        role_only_markers = (
            "list roles",
            "show roles",
            "role me",
            "role yourself",
            "assign me role",
            "give me role",
            "grant me role",
        )
        if any(marker in text for marker in role_only_markers):
            return {"route": "none", "setup_requested": False, "setup_mode": "none"}

        response_only_markers = (
            "always talk here",
            "always respond here",
            "mention only here",
            "stop always talking here",
            "reply to every message in this room",
        )
        if any(marker in text for marker in response_only_markers):
            return {"route": "none", "setup_requested": False, "setup_mode": "none"}

        has_scope = any(
            token in text
            for token in (
                "discord",
                "server",
                "guild",
                "channel",
                "channels",
                "category",
                "categories",
                "room",
                "rooms",
                "role",
                "roles",
            )
        )
        has_action = any(
            token in text
            for token in (
                "setup",
                "set up",
                "configure",
                "organize",
                "build",
                "make ",
                "rename",
                "theme",
                "style",
                "vibe",
                "create",
            )
        )
        if not has_scope or not (has_action or cls._request_delegates_creative_choices(text)):
            return {"route": "none", "setup_requested": False, "setup_mode": "none"}

        creative = cls._request_delegates_creative_choices(text) or any(
            token in text
            for token in (
                " for us",
                "hq",
                "vibe",
                "theme",
                "style",
                "gamer",
                "gaming",
            )
        )
        exact = any(
            token in text
            for token in (
                "rename ",
                "called ",
                "named ",
                "exactly",
                "specific",
            )
        )
        setup_mode = "creative" if creative or not exact else "exact"
        return {"route": "setup", "setup_requested": True, "setup_mode": setup_mode}

    @classmethod
    def _extract_excluded_theme_terms(cls, request_text: str) -> set[str]:
        text = cls._normalized_request_text(request_text)
        if not text:
            return set()

        out: set[str] = set()
        ignored = {
            "it",
            "this",
            "that",
            "them",
            "these",
            "those",
            "here",
            "there",
            "us",
            "me",
            "we",
            "our",
            "the",
            "a",
            "an",
        }
        for pattern in (
            r"\bnot\s+([a-z0-9][a-z0-9'_-]{1,31})",
            r"\bwithout\s+([a-z0-9][a-z0-9'_-]{1,31})",
            r"\bavoid\s+([a-z0-9][a-z0-9'_-]{1,31})",
            r"\binstead of\s+([a-z0-9][a-z0-9'_-]{1,31})",
        ):
            for match in re.finditer(pattern, text):
                token = str(match.group(1) or "").strip(" .,!?:;\"'`-_")
                if token and token not in ignored:
                    out.add(token)
        return out

    @staticmethod
    def _name_contains_excluded_term(name: Any, excluded_terms: set[str]) -> bool:
        if not excluded_terms:
            return False
        text = str(name or "").strip().lower()
        if not text:
            return False
        normalized = " " + re.sub(r"[^a-z0-9]+", " ", text) + " "
        for token in excluded_terms:
            clean = str(token or "").strip().lower()
            if not clean:
                continue
            if f" {clean} " in normalized:
                return True
            if clean in text:
                return True
        return False

    @classmethod
    def _preferred_creative_brand(cls, request_text: str, excluded_terms: set[str]) -> tuple[str, str]:
        text = cls._normalized_request_text(request_text)
        if any(token in text for token in ("totty", "totterson")) and "totty" not in excluded_terms:
            return ("Totty", "totty")
        if "phooey" in text and "phooey" not in excluded_terms:
            return ("Phooey", "phooey")
        if "tater" in text and "tater" not in excluded_terms:
            return ("Tater", "tater")
        return ("", "")

    @classmethod
    def _creative_outline_has_actionable_work(cls, outline: Dict[str, Any] | None) -> bool:
        payload = outline if isinstance(outline, dict) else {}
        if str(payload.get("server_name") or "").strip():
            return True
        if _coerce_bool(payload.get("set_guild_icon_from_attachment"), False):
            return True
        if list(payload.get("roles") or []):
            return True
        if list(payload.get("categories") or []):
            return True
        if list(payload.get("rename_channels") or []):
            return True
        return False

    @classmethod
    def _sanitize_creative_outline(
        cls,
        outline: Dict[str, Any],
        *,
        request_text: str,
    ) -> Dict[str, Any]:
        if not isinstance(outline, dict) or not outline:
            return {}

        excluded_terms = cls._extract_excluded_theme_terms(request_text)
        if not excluded_terms:
            return dict(outline)

        cleaned = dict(outline)

        roles: list[Any] = []
        for item in list(cleaned.get("roles") or []):
            if isinstance(item, dict):
                label = item.get("name")
            else:
                label = item
            if cls._name_contains_excluded_term(label, excluded_terms):
                continue
            roles.append(item)
        cleaned["roles"] = roles

        categories: list[Dict[str, Any]] = []
        for category in list(cleaned.get("categories") or []):
            if not isinstance(category, dict):
                continue
            if cls._name_contains_excluded_term(category.get("name"), excluded_terms):
                continue
            next_category = dict(category)
            kept_channels: list[Dict[str, Any]] = []
            for channel in list(category.get("channels") or []):
                if not isinstance(channel, dict):
                    continue
                if cls._name_contains_excluded_term(channel.get("name"), excluded_terms):
                    continue
                kept_channels.append(dict(channel))
            next_category["channels"] = kept_channels
            if kept_channels:
                categories.append(next_category)
        cleaned["categories"] = categories

        rename_channels: list[Dict[str, Any]] = []
        for item in list(cleaned.get("rename_channels") or []):
            if not isinstance(item, dict):
                continue
            if cls._name_contains_excluded_term(item.get("to"), excluded_terms):
                continue
            rename_channels.append(dict(item))
        cleaned["rename_channels"] = rename_channels

        server_name = str(cleaned.get("server_name") or "").strip()
        if cls._name_contains_excluded_term(server_name, excluded_terms):
            cleaned["server_name"] = ""

        return cleaned

    @classmethod
    def _fallback_creative_outline(cls, request_text: str, guild_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        text = cls._normalized_request_text(request_text)
        excluded_terms = cls._extract_excluded_theme_terms(request_text)
        brand_label, brand_slug = cls._preferred_creative_brand(request_text, excluded_terms)

        is_gaming = any(token in text for token in ("gamer", "gaming", "lfg", "raid", "squad", "clips"))
        is_builder = any(
            token in text
            for token in ("hq", "dev", "builder", "build", "workshop", "tool", "tooling", "bot")
        ) or any(token in text for token in ("for us", "workspace", "crew", "team"))

        if is_gaming:
            roles: list[str] = []
            if brand_label:
                roles.append(f"{brand_label} Crew")
            roles.extend(["Shot Callers", "Raid Team", "Clip Keepers"])
            categories = [
                {
                    "name": "Lobby",
                    "channels": [
                        {"name": f"{brand_slug}-lobby" if brand_slug else "party-up", "kind": "text"},
                        {"name": "match-plans", "kind": "text"},
                        {"name": "server-news", "kind": "text"},
                    ],
                },
                {
                    "name": "Squads",
                    "channels": [
                        {"name": "looking-for-chaos", "kind": "text"},
                        {"name": f"{brand_slug}-squad" if brand_slug else "party-room", "kind": "voice"},
                        {"name": "warmup-room", "kind": "voice"},
                    ],
                },
                {
                    "name": "Highlights",
                    "channels": [
                        {"name": "clip-archive", "kind": "text"},
                        {"name": "loadout-lab", "kind": "text"},
                    ],
                },
            ]
        elif is_builder:
            roles = []
            if brand_label:
                roles.append(f"{brand_label} Crew")
            roles.extend(["Builders", "Ops", "Backstage"])
            categories = [
                {
                    "name": "Front Desk",
                    "channels": [
                        {"name": f"{brand_slug}-central" if brand_slug else "front-desk", "kind": "text"},
                        {"name": "bulletin-board", "kind": "text"},
                        {"name": "house-rules", "kind": "text"},
                    ],
                },
                {
                    "name": "Workshop",
                    "channels": [
                        {"name": f"{brand_slug}-lab" if brand_slug else "build-lab", "kind": "text"},
                        {"name": "bot-pit", "kind": "text"},
                        {"name": "wins-and-wreckage", "kind": "text"},
                        {"name": f"{brand_slug}-huddle" if brand_slug else "standup-room", "kind": "voice"},
                    ],
                },
                {
                    "name": "Backstage",
                    "channels": [
                        {"name": "green-room", "kind": "text"},
                        {"name": "inside-jokes", "kind": "text"},
                        {"name": "after-hours", "kind": "voice"},
                    ],
                },
            ]
        else:
            roles = []
            if brand_label:
                roles.append(f"{brand_label} Crew")
            roles.extend(["Regulars", "Hosts", "Night Shift"])
            categories = [
                {
                    "name": "Welcome Wagon",
                    "channels": [
                        {"name": f"{brand_slug}-square" if brand_slug else "town-square", "kind": "text"},
                        {"name": "notice-board", "kind": "text"},
                    ],
                },
                {
                    "name": "Hangout",
                    "channels": [
                        {"name": "story-time", "kind": "text"},
                        {"name": "show-and-tell", "kind": "text"},
                        {"name": f"{brand_slug}-parlor" if brand_slug else "hangout-room", "kind": "voice"},
                    ],
                },
            ]

        outline = {
            "response_channel_action": "none",
            "server_name": "",
            "set_guild_icon_from_attachment": False,
            "roles": roles[:MAX_ROLES],
            "categories": categories[:MAX_CATEGORIES],
            "rename_channels": [],
            "notes": "Autonomous creative fallback plan.",
        }
        return cls._sanitize_creative_outline(outline, request_text=request_text)

    async def _route_request_with_llm(self, text: str, message, llm_client) -> Dict[str, Any]:
        command_text = self._normalize_role_command_text(text, message)
        lowered = self._normalized_request_text(command_text)
        route = str(getattr(self, "fixed_route", "") or "").strip().lower()
        if route not in {"role", "response_mode", "setup"}:
            route = "none"

        out = {
            "route": route,
            "role_action": "none",
            "role": "",
            "roles": [],
            "role_only": False,
            "response_channel_action": "none",
            "setup_requested": False,
            "setup_mode": "none",
            "delete_requested": False,
        }

        if route == "role":
            action = "role_me"
            if any(marker in lowered for marker in ("list roles", "show roles", "list role")):
                action = "list_roles"
            elif any(marker in lowered for marker in ("role yourself", "assign yourself", "give yourself", "grant yourself")):
                action = "role_self"
            out["role_action"] = action
            out["role_only"] = True
            return out

        if route == "response_mode":
            action = "none"
            if any(marker in lowered for marker in ("clear always respond", "clear always-response", "disable always respond everywhere", "clear all always respond")):
                action = "clear_all"
            elif any(marker in lowered for marker in ("only always respond here", "only this room", "this should be the only")):
                action = "set_only_current"
            elif any(marker in lowered for marker in ("mention only here", "stop always talking here", "disable always respond in this room", "only respond to ping here")):
                action = "remove_current"
            elif any(marker in lowered for marker in ("always talk in this room", "always respond here", "reply to every message in this room")):
                action = "add_current"
            out["response_channel_action"] = action
            return out

        if route == "setup":
            inferred = self._infer_setup_route_from_request(command_text)
            mode = str(inferred.get("setup_mode") or "creative").strip().lower()
            if mode not in {"exact", "creative"}:
                mode = "creative"
            out["setup_requested"] = True
            out["setup_mode"] = mode
            out["delete_requested"] = bool(re.search(r"\b(delete|wipe|remove)\b", lowered))
            return out

        return out

    @staticmethod
    def _normalize_role_lookup_token(value: str) -> str:
        token = str(value or "").strip()
        if token.startswith("<@&") and token.endswith(">"):
            token = token[3:-1].strip()
        if token.startswith("@"):
            token = token[1:].strip()
        token = token.strip(" \t\r\n")
        return token

    @classmethod
    def _resolve_role_by_text(cls, guild, role_text: str):
        token = cls._normalize_role_lookup_token(role_text)
        if not token:
            return None

        role_id = 0
        try:
            role_id = int(token)
        except Exception:
            role_id = 0
        if role_id > 0:
            getter = getattr(guild, "get_role", None)
            if callable(getter):
                role_obj = getter(role_id)
                if role_obj is not None:
                    return role_obj
            for role_obj in list(getattr(guild, "roles", []) or []):
                if int(getattr(role_obj, "id", 0) or 0) == role_id:
                    return role_obj

        lowered = token.lower()
        for role_obj in list(getattr(guild, "roles", []) or []):
            if str(getattr(role_obj, "name", "") or "").strip().lower() == lowered:
                return role_obj
        return None

    @staticmethod
    def _normalize_role_match_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return " ".join(text.split())

    def _match_role_names_from_request(self, guild, request_text: str) -> list[str]:
        haystack = self._normalize_role_match_text(request_text)
        if not haystack:
            return []
        matched: list[tuple[int, str]] = []
        for role_obj in list(getattr(guild, "roles", []) or []):
            role_name = str(getattr(role_obj, "name", "") or "").strip()
            if not role_name or role_name == "@everyone":
                continue
            needle = self._normalize_role_match_text(role_name)
            if not needle:
                continue
            pattern = rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])"
            if re.search(pattern, haystack):
                matched.append((len(needle), role_name))
        matched.sort(key=lambda item: (-item[0], item[1].lower()))
        out: list[str] = []
        seen: set[str] = set()
        for _, role_name in matched:
            lowered = role_name.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            out.append(role_name)
        return out

    def _bot_member(self, guild, message):
        if guild is None:
            return None
        bot_member = getattr(guild, "me", None)
        if bot_member is not None:
            return bot_member
        getter = getattr(guild, "get_member", None)
        bot_id = int(getattr(getattr(getattr(message, "client", None), "user", None), "id", 0) or 0)
        if callable(getter) and bot_id > 0:
            try:
                return getter(bot_id)
            except Exception:
                return None
        return None

    def _author_is_configured_admin(self, message) -> bool:
        settings = self._load_discord_settings()
        raw_admin_id = str(settings.get("admin_user_id") or "").strip()
        if not raw_admin_id:
            return False
        try:
            admin_id = int(raw_admin_id)
        except Exception:
            return False
        author_id = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        return author_id > 0 and author_id == admin_id

    def _author_can_manage_roles(self, message) -> bool:
        if self._author_is_configured_admin(message):
            return True
        perms = getattr(getattr(message, "author", None), "guild_permissions", None)
        if perms is None:
            return False
        return bool(getattr(perms, "manage_roles", False) or getattr(perms, "administrator", False))

    async def _handle_direct_role_command(self, message, parsed: Dict[str, Any]) -> Dict[str, Any]:
        guild = getattr(message, "guild", None)
        if guild is None:
            return action_failure(
                code="guild_required",
                message="Role commands only work in a server channel.",
                say_hint="Explain role commands require guild context.",
            )

        action = str(parsed.get("action") or "").strip().lower()
        if action == "list_roles":
            roles = [
                role
                for role in list(getattr(guild, "roles", []) or [])
                if str(getattr(role, "name", "") or "").strip()
                and str(getattr(role, "name", "") or "") != "@everyone"
            ]
            if not roles:
                return action_success(
                    facts={"action": "list_roles", "count": 0},
                    summary_for_user="No assignable roles were found in this server.",
                )
            roles_sorted = sorted(
                roles,
                key=lambda role_obj: (
                    -self._role_position(role_obj),
                    str(getattr(role_obj, "name", "") or "").lower(),
                ),
            )
            labels = [f"`{str(getattr(role_obj, 'name', '') or '').strip()}`" for role_obj in roles_sorted]
            preview: list[str] = []
            total_chars = 0
            for label in labels:
                projected = total_chars + len(label) + (2 if preview else 0)
                if projected > 1700:
                    break
                preview.append(label)
                total_chars = projected
            remainder = len(labels) - len(preview)
            suffix = f" (+{remainder} more)" if remainder > 0 else ""
            return action_success(
                facts={"action": "list_roles", "count": len(labels)},
                summary_for_user="Server roles: " + ", ".join(preview) + suffix,
            )

        requested_role_names: list[str] = []
        role_list = parsed.get("roles")
        if isinstance(role_list, list):
            for item in role_list:
                name = str(item or "").strip()
                if name and name not in requested_role_names:
                    requested_role_names.append(name)
        role_text = str(parsed.get("role") or "").strip()
        if role_text and role_text not in requested_role_names:
            requested_role_names.append(role_text)
        request_text = str(parsed.get("request_text") or "").strip()
        for name in self._match_role_names_from_request(guild, request_text):
            if name not in requested_role_names:
                requested_role_names.append(name)
        if not requested_role_names:
            return action_failure(
                code="role_not_found",
                message="I couldn't tell which role or roles you want. Try `list roles` first.",
                say_hint="Explain that the requested role names could not be identified.",
            )

        role_objs: list[Any] = []
        missing_roles: list[str] = []
        seen_role_ids: set[int] = set()
        for role_name in requested_role_names:
            role_obj = self._resolve_role_by_text(guild, role_name)
            if role_obj is None:
                missing_roles.append(role_name)
                continue
            rid = int(getattr(role_obj, "id", 0) or 0)
            if rid > 0 and rid in seen_role_ids:
                continue
            if rid > 0:
                seen_role_ids.add(rid)
            role_objs.append(role_obj)
        if not role_objs:
            missing_preview = ", ".join(f"`{name}`" for name in missing_roles[:5]) or "those roles"
            return action_failure(
                code="role_not_found",
                message=f"I couldn't find {missing_preview}. Try `list roles` first.",
                say_hint="Explain the requested roles were not found.",
            )

        if not self._author_can_manage_roles(message):
            return action_failure(
                code="insufficient_permissions",
                message="You need `Manage Roles` permission (or be the configured admin user) to do that.",
                say_hint="Explain the caller needs Manage Roles permission.",
            )

        bot_member = self._bot_member(guild, message)
        if bot_member is None:
            return action_failure(
                code="bot_member_missing",
                message="I couldn't resolve my bot member in this guild.",
                say_hint="Explain the bot member could not be resolved.",
            )

        bot_perms = getattr(bot_member, "guild_permissions", None)
        if not bool(getattr(bot_perms, "manage_roles", False)):
            return action_failure(
                code="bot_missing_manage_roles",
                message="I don't have `Manage Roles` permission in this server.",
                say_hint="Explain the bot is missing Manage Roles permission.",
            )

        bot_top = self._role_position(getattr(bot_member, "top_role", None))

        if action == "role_self":
            target_member = bot_member
            target_label = "I"
        else:
            target_member = getattr(message, "author", None)
            target_label = "You"
            target_top = self._role_position(getattr(target_member, "top_role", None))
            if target_top >= bot_top:
                return action_failure(
                    code="target_hierarchy_blocked",
                    message="I can't change roles for your member because of role hierarchy.",
                    say_hint="Explain role hierarchy prevents changing this member.",
                )

        existing_role_ids = {
            int(getattr(item, "id", 0) or 0)
            for item in list(getattr(target_member, "roles", []) or [])
        }
        blocked_managed: list[str] = []
        blocked_hierarchy: list[str] = []
        already_have: list[str] = []
        assignable: list[Any] = []
        assignable_names: list[str] = []
        for role_obj in role_objs:
            role_name = str(getattr(role_obj, "name", "that role") or "that role")
            if bool(getattr(role_obj, "managed", False)):
                blocked_managed.append(role_name)
                continue
            if self._role_position(role_obj) >= bot_top:
                blocked_hierarchy.append(role_name)
                continue
            role_id = int(getattr(role_obj, "id", 0) or 0)
            if role_id > 0 and role_id in existing_role_ids:
                already_have.append(role_name)
                continue
            assignable.append(role_obj)
            assignable_names.append(role_name)

        added_names: list[str] = []
        if assignable:
            try:
                await target_member.add_roles(
                    *assignable,
                    reason=f"Requested by {getattr(getattr(message, 'author', None), 'name', 'user')} via role command",
                )
                added_names = list(assignable_names)
            except Exception as exc:
                if not already_have:
                    return action_failure(
                        code="role_assignment_failed",
                        message=f"I couldn't assign the requested roles: {exc}",
                        say_hint="Explain the role assignment failed and include the error briefly.",
                    )
                blocked_hierarchy.append(str(exc))

        if not added_names and not already_have:
            problems: list[str] = []
            if missing_roles:
                problems.append("missing: " + ", ".join(f"`{name}`" for name in missing_roles[:5]))
            if blocked_managed:
                problems.append("managed: " + ", ".join(f"`{name}`" for name in blocked_managed[:5]))
            if blocked_hierarchy:
                problems.append("blocked: " + ", ".join(f"`{name}`" for name in blocked_hierarchy[:5]))
            detail = "; ".join(problems) or "I couldn't assign those roles."
            return action_failure(
                code="role_assignment_failed",
                message=detail,
                say_hint="Explain which requested roles could not be assigned and why.",
            )

        summary_parts: list[str] = []
        if added_names:
            summary_parts.append(f"{target_label} now have " + ", ".join(f"`{name}`" for name in added_names))
        if already_have:
            summary_parts.append("already had " + ", ".join(f"`{name}`" for name in already_have))
        if missing_roles:
            summary_parts.append("couldn't find " + ", ".join(f"`{name}`" for name in missing_roles))
        if blocked_managed:
            summary_parts.append("couldn't assign managed roles " + ", ".join(f"`{name}`" for name in blocked_managed))
        if blocked_hierarchy:
            summary_parts.append("couldn't assign higher roles " + ", ".join(f"`{name}`" for name in blocked_hierarchy))

        return action_success(
            facts={
                "action": action,
                "requested_roles": requested_role_names,
                "added_roles": added_names,
                "already_had_roles": already_have,
                "missing_roles": missing_roles,
                "blocked_managed_roles": blocked_managed,
                "blocked_hierarchy_roles": blocked_hierarchy,
            },
            summary_for_user="Done. " + "; ".join(summary_parts) + ".",
        )

    @staticmethod
    def _blob_client():
        return redis_blob_client

    @classmethod
    def _read_blob_bytes(cls, blob_key: str) -> bytes:
        key = str(blob_key or "").strip()
        if not key:
            return b""
        try:
            raw = cls._blob_client().get(key.encode("utf-8"))
        except Exception:
            return b""
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw)
        return b""

    @staticmethod
    def _media_ref_is_image(ref: Dict[str, Any]) -> bool:
        kind = str(ref.get("type") or "").strip().lower()
        if kind == "image":
            return True
        mimetype = str(ref.get("mimetype") or "").strip().lower()
        if mimetype.startswith("image/"):
            return True
        name = str(ref.get("name") or "").strip().lower()
        if name.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            return True
        return False

    @classmethod
    def _extract_icon_bytes_from_args(cls, args: Dict[str, Any], stats: Dict[str, Any]) -> bytes:
        payload = args if isinstance(args, dict) else {}
        explicit_blob_key = str(payload.get("icon_blob_key") or "").strip()
        if explicit_blob_key:
            raw = cls._read_blob_bytes(explicit_blob_key)
            if raw:
                return raw
            stats["warnings"].append("Icon blob key was provided, but no image data was found for it.")

        refs: list[Dict[str, Any]] = []
        direct_refs = payload.get("media_refs")
        if isinstance(direct_refs, list):
            refs.extend([item for item in direct_refs if isinstance(item, dict)])

        origin = payload.get("origin") if isinstance(payload.get("origin"), dict) else {}
        origin_refs = origin.get("media_refs") if isinstance(origin, dict) else None
        if isinstance(origin_refs, list):
            refs.extend([item for item in origin_refs if isinstance(item, dict)])

        for ref in refs:
            if not cls._media_ref_is_image(ref):
                continue
            blob_key = str(ref.get("blob_key") or "").strip()
            if not blob_key:
                continue
            raw = cls._read_blob_bytes(blob_key)
            if raw:
                return raw

        return b""

    def _admin_allowed(self, message) -> tuple[bool, str]:
        settings = self._load_discord_settings()
        raw_admin_id = str(settings.get("admin_user_id") or "").strip()
        if not raw_admin_id:
            return False, "Discord admin_user_id is not configured in platform settings."
        try:
            admin_id = int(raw_admin_id)
        except Exception:
            return False, "Discord admin_user_id is invalid in platform settings."
        author_id = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        if author_id != admin_id:
            return False, "This tool is restricted to the configured Discord admin user."
        return True, ""

    @staticmethod
    def _guild_snapshot(guild) -> Dict[str, Any]:
        if guild is None:
            return {
                "name": "",
                "id": "",
                "roles": [],
                "categories": [],
                "channels": [],
            }
        roles: list[Dict[str, Any]] = []
        for role in list(getattr(guild, "roles", []) or [])[:80]:
            role_name = str(getattr(role, "name", "") or "").strip()
            if not role_name:
                continue
            roles.append(
                {
                    "id": int(getattr(role, "id", 0) or 0),
                    "name": role_name,
                }
            )
        categories: list[Dict[str, Any]] = []
        for cat in list(getattr(guild, "categories", []) or [])[:80]:
            cat_name = str(getattr(cat, "name", "") or "").strip()
            if not cat_name:
                continue
            categories.append(
                {
                    "id": int(getattr(cat, "id", 0) or 0),
                    "name": cat_name,
                }
            )
        channels: list[Dict[str, Any]] = []
        for ch in list(getattr(guild, "channels", []) or [])[:200]:
            if isinstance(ch, discord.CategoryChannel):
                continue
            kind = "voice" if isinstance(ch, discord.VoiceChannel) else "text"
            channels.append(
                {
                    "id": int(getattr(ch, "id", 0) or 0),
                    "name": str(getattr(ch, "name", "") or ""),
                    "kind": kind,
                    "category_id": int(getattr(getattr(ch, "category", None), "id", 0) or 0),
                    "category": str(getattr(getattr(ch, "category", None), "name", "") or ""),
                }
            )
        return {
            "name": str(getattr(guild, "name", "") or ""),
            "id": str(getattr(guild, "id", "") or ""),
            "roles": roles,
            "categories": categories,
            "channels": channels,
        }

    @staticmethod
    def _has_destructive_delete_work(plan: Dict[str, Any]) -> bool:
        payload = plan if isinstance(plan, dict) else {}
        if list(payload.get("delete_channels") or []):
            return True
        if list(payload.get("delete_categories") or []):
            return True
        if list(payload.get("delete_all_channels_except_ids") or []):
            return True
        if list(payload.get("delete_all_channels_except_names") or []):
            return True
        if list(payload.get("delete_all_categories_except_ids") or []):
            return True
        if list(payload.get("delete_all_categories_except_names") or []):
            return True
        return False

    @staticmethod
    def _has_delete_intent(raw_plan: Dict[str, Any] | None) -> bool:
        payload = raw_plan if isinstance(raw_plan, dict) else {}
        if list(payload.get("delete_channels") or []):
            return True
        if list(payload.get("delete_categories") or []):
            return True
        if list(payload.get("delete_all_channels_except") or []):
            return True
        if list(payload.get("delete_all_channels_except_ids") or []):
            return True
        if list(payload.get("delete_all_channels_except_names") or []):
            return True
        if list(payload.get("delete_all_categories_except") or []):
            return True
        if list(payload.get("delete_all_categories_except_ids") or []):
            return True
        if list(payload.get("delete_all_categories_except_names") or []):
            return True
        return False

    async def _llm_plan(self, request_text: str, guild, llm_client, *, setup_mode: str = "exact") -> Dict[str, Any]:
        if llm_client is None:
            return {}
        guild_snapshot = self._guild_snapshot(guild)
        mode = str(setup_mode or "exact").strip().lower()
        if mode not in {"exact", "creative"}:
            mode = "exact"
        if mode == "creative":
            return await self._llm_plan_creative(request_text, guild, llm_client)

        allowed_perms = ", ".join(sorted(VALID_CHANNEL_PERMISSIONS))
        plan_schema = (
            "{\n"
            '  "response_channel_action":"none|add_current|remove_current|set_only_current|clear_all",\n'
            '  "server_name":"optional server name",\n'
            '  "set_guild_icon_from_attachment":false,\n'
            '  "roles":[{"name":"Role","apply_mode":"create_only|create_or_update","color":"#RRGGBB","mentionable":false,"hoist":false,"permissions":["manage_messages"]}],\n'
            '  "categories":[{"name":"Category","apply_mode":"create_only|create_or_update","channels":[{"name":"general","kind":"text|voice","topic":"optional",'
            '"slowmode_seconds":0,"nsfw":false,'
            '"apply_mode":"create_only|create_or_update",'
            '"permissions":[{"role":"@everyone","allow":["view_channel"],"deny":["send_messages"]}]}]}],\n'
            '  "rename_channels":[{"from":"channel ref (name,#name,<#id>,id,current)","to":"new channel name","kind":"text|voice|any"}],\n'
            '  "notes":"short optional notes"\n'
            "}\n"
        )
        prompt = (
            "Build a safe Discord admin execution plan. Return strict JSON only with this schema:\n"
            f"{plan_schema}"
            "Rules:\n"
            "- The user gave explicit setup instructions. Follow them literally.\n"
            "- Create or change only the rooms, categories, roles, and settings the user clearly asked for.\n"
            "- Do not invent extra structure, filler rooms, or theme ideas beyond the request.\n"
            "- If no server-structure changes are needed, return empty roles/categories and blank server_name.\n"
            "- Default apply_mode should be create_only. Use create_or_update only when user explicitly asks to modify an existing role/channel/category.\n"
            "- Channel/category deletion is disabled. Never output delete_* fields.\n"
            "- For rename requests, output rename_channels with exact from/to references from the snapshot.\n"
            "- Never represent rename as delete + create.\n"
            f"- Max {MAX_CATEGORIES} categories and {MAX_CHANNELS_PER_CATEGORY} channels per category.\n"
            "- Use channel kind text or voice only.\n"
            "- If user only asks about response behavior, leave roles/categories empty.\n"
            "- Permission names must be valid Discord permission flags.\n"
            f"- Allowed permission tokens include: {allowed_perms}\n\n"
            f"User request: {request_text}\n"
            f"Guild snapshot JSON: {json.dumps(guild_snapshot, ensure_ascii=False)}"
        )
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "You output only strict JSON."},
                    {"role": "user", "content": prompt},
                ]
            )
            raw = str((resp.get("message", {}) or {}).get("content", "") or "").strip()
            blob = extract_json(raw) if raw else None
            if not blob:
                return {}
            parsed = json.loads(blob)
            return parsed if isinstance(parsed, dict) else {}
        except Exception as exc:
            logger.warning(f"[discord_admin] LLM planning failed: {exc}")
            return {}

    @staticmethod
    def _normalize_apply_mode(value: Any) -> str:
        token = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
        if token in {"create_or_update", "upsert", "update_existing", "update"}:
            return "create_or_update"
        return "create_only"

    @staticmethod
    def _normalize_keep_channel_refs(raw_refs: Any) -> Dict[str, Any]:
        refs = raw_refs if isinstance(raw_refs, list) else []
        keep_ids: set[int] = set()
        keep_names: set[str] = set()
        for item in refs:
            token = str(item or "").strip()
            if not token:
                continue
            lowered = token.lower()
            if lowered in {"this", "here", "current", "current_channel", "this_channel"}:
                keep_names.add("__current__")
                continue
            if token.startswith("<#") and token.endswith(">"):
                token = token[2:-1].strip()
            token = token.lstrip("#").strip()
            try:
                parsed = int(token)
                if parsed > 0:
                    keep_ids.add(parsed)
                    continue
            except Exception:
                pass
            cleaned = str(token or "").strip().lower()
            if cleaned:
                keep_names.add(cleaned)
        return {
            "delete_all_channels_except_ids": sorted(keep_ids),
            "delete_all_channels_except_names": sorted(keep_names),
        }

    @staticmethod
    def _normalize_keep_category_refs(raw_refs: Any) -> Dict[str, Any]:
        refs = raw_refs if isinstance(raw_refs, list) else []
        keep_ids: set[int] = set()
        keep_names: set[str] = set()
        for item in refs:
            token = str(item or "").strip()
            if not token:
                continue
            lowered = token.lower()
            if lowered in {"this", "here", "current", "current_category", "this_category"}:
                keep_names.add("__current__")
                continue
            token = token.strip()
            try:
                parsed = int(token)
                if parsed > 0:
                    keep_ids.add(parsed)
                    continue
            except Exception:
                pass
            cleaned = str(token or "").strip().lower()
            if cleaned:
                keep_names.add(cleaned)
        return {
            "delete_all_categories_except_ids": sorted(keep_ids),
            "delete_all_categories_except_names": sorted(keep_names),
        }

    @staticmethod
    def _normalize_delete_channel_targets(raw_targets: Any) -> list[Dict[str, Any]]:
        targets = raw_targets if isinstance(raw_targets, list) else []
        out: list[Dict[str, Any]] = []
        seen: set[tuple[int, str, str]] = set()
        for item in targets:
            ref_raw: Any = item
            kind_raw: Any = ""
            if isinstance(item, dict):
                ref_raw = item.get("ref")
                if ref_raw in (None, ""):
                    ref_raw = item.get("channel")
                if ref_raw in (None, ""):
                    ref_raw = item.get("name")
                if ref_raw in (None, ""):
                    ref_raw = item.get("id")
                kind_raw = item.get("kind")

            kind_token = str(kind_raw or "").strip().lower().replace("-", "_").replace(" ", "_")
            if kind_token in {"voice", "vc", "voice_channel", "voice_channels"}:
                kind = "voice"
            elif kind_token in {"text", "txt", "text_channel", "text_channels"}:
                kind = "text"
            else:
                kind = "any"

            ref = str(ref_raw or "").strip()
            rid = 0
            name = ""
            if ref:
                lowered = ref.lower()
                if lowered in {"voice", "all_voice", "allvoice", "voice_channels"}:
                    kind = "voice"
                    ref = ""
                elif lowered in {"text", "all_text", "alltext", "text_channels"}:
                    kind = "text"
                    ref = ""
                elif lowered in {"this", "here", "current", "current_channel", "this_channel"}:
                    name = "__current__"
                else:
                    token = ref
                    if token.startswith("<#") and token.endswith(">"):
                        token = token[2:-1].strip()
                    token = token.lstrip("#").strip()
                    try:
                        parsed = int(token)
                    except Exception:
                        parsed = 0
                    if parsed > 0:
                        rid = parsed
                    else:
                        cleaned = str(token or "").strip().lower()
                        if cleaned:
                            name = cleaned

            # Require an explicit channel reference; kind-only deletes are too broad.
            if rid <= 0 and not name:
                continue
            key = (rid, name, kind)
            if key in seen:
                continue
            seen.add(key)
            out.append({"id": rid, "name": name, "kind": kind})
        return out

    @staticmethod
    def _normalize_delete_category_targets(raw_targets: Any) -> list[Dict[str, Any]]:
        targets = raw_targets if isinstance(raw_targets, list) else []
        out: list[Dict[str, Any]] = []
        seen: set[tuple[int, str]] = set()
        for item in targets:
            ref_raw: Any = item
            if isinstance(item, dict):
                ref_raw = item.get("ref")
                if ref_raw in (None, ""):
                    ref_raw = item.get("name")
                if ref_raw in (None, ""):
                    ref_raw = item.get("id")
                if ref_raw in (None, ""):
                    ref_raw = item.get("category")

            ref = str(ref_raw or "").strip()
            rid = 0
            name = ""
            if not ref:
                continue
            lowered = ref.lower()
            if lowered in {"this", "here", "current", "current_category", "this_category"}:
                name = "__current__"
            else:
                token = ref.strip()
                try:
                    parsed = int(token)
                except Exception:
                    parsed = 0
                if parsed > 0:
                    rid = parsed
                else:
                    cleaned = str(token or "").strip().lower()
                    if cleaned:
                        name = cleaned
            if rid <= 0 and not name:
                continue
            key = (rid, name)
            if key in seen:
                continue
            seen.add(key)
            out.append({"id": rid, "name": name})
        return out

    @staticmethod
    def _normalize_rename_channel_targets(raw_targets: Any) -> list[Dict[str, Any]]:
        targets = raw_targets if isinstance(raw_targets, list) else []
        out: list[Dict[str, Any]] = []
        seen: set[tuple[int, str, str, str]] = set()
        for item in targets:
            if not isinstance(item, dict):
                continue

            ref_raw = (
                item.get("from")
                or item.get("ref")
                or item.get("channel")
                or item.get("name")
                or item.get("id")
            )
            kind_raw = item.get("kind")
            new_name_raw = item.get("to") or item.get("new_name") or item.get("rename_to")

            kind_token = str(kind_raw or "").strip().lower().replace("-", "_").replace(" ", "_")
            if kind_token in {"voice", "vc", "voice_channel", "voice_channels"}:
                kind = "voice"
            elif kind_token in {"text", "txt", "text_channel", "text_channels"}:
                kind = "text"
            else:
                kind = "any"

            ref = str(ref_raw or "").strip()
            rid = 0
            name = ""
            if ref:
                lowered = ref.lower()
                if lowered in {"this", "here", "current", "current_channel", "this_channel"}:
                    name = "__current__"
                else:
                    token = ref
                    if token.startswith("<#") and token.endswith(">"):
                        token = token[2:-1].strip()
                    token = token.lstrip("#").strip()
                    try:
                        parsed = int(token)
                    except Exception:
                        parsed = 0
                    if parsed > 0:
                        rid = parsed
                    else:
                        cleaned = str(token or "").strip().lower()
                        if cleaned:
                            name = cleaned

            if rid <= 0 and not name:
                continue

            new_name_raw = _clean_name(new_name_raw, "")
            if not new_name_raw:
                continue
            if kind == "text":
                new_name = _text_channel_slug(new_name_raw)
            else:
                new_name = new_name_raw[:95]
            if not new_name:
                continue

            key = (rid, name, kind, new_name.lower())
            if key in seen:
                continue
            seen.add(key)
            out.append({"id": rid, "name": name, "kind": kind, "new_name": new_name})
        return out

    @staticmethod
    def _strip_deletion_fields(plan: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(plan if isinstance(plan, dict) else {})
        out["delete_channels"] = []
        out["delete_categories"] = []
        out["delete_all_channels_except_ids"] = []
        out["delete_all_channels_except_names"] = []
        out["confirm_delete_all_channels"] = False
        out["delete_all_categories_except_ids"] = []
        out["delete_all_categories_except_names"] = []
        out["confirm_delete_all_categories"] = False
        return out

    async def _llm_creative_outline(self, request_text: str, guild_snapshot: Dict[str, Any], llm_client) -> Dict[str, Any]:
        if llm_client is None:
            return {}
        excluded_terms = sorted(self._extract_excluded_theme_terms(request_text))
        prompt = (
            "Design a practical Discord server layout for this broad request.\n"
            "Return strict JSON only with this compact schema:\n"
            "{\n"
            '  "response_channel_action":"none|add_current|remove_current|set_only_current|clear_all",\n'
            '  "server_name":"optional server name",\n'
            '  "set_guild_icon_from_attachment":false,\n'
            '  "roles":["Role A","Role B"],\n'
            '  "categories":[{"name":"Category","channels":[{"name":"general","kind":"text|voice"}]}],\n'
            '  "rename_channels":[{"from":"current|#general|123","to":"new name","kind":"text|voice|any"}],\n'
            '  "notes":"short optional notes"\n'
            "}\n"
            "Rules:\n"
            "- The user asked for a themed or open-ended setup. You must design the layout.\n"
            "- Return a NON-EMPTY layout with concrete categories and channels.\n"
            "- Do not ask follow-up questions. Choose names yourself.\n"
            "- If the user delegated creativity, treat that as full permission to choose channel/category/role names without asking.\n"
            "- If the user says 'not <theme/name>', do not use that theme/name in new roles, categories, channels, or server names.\n"
            "- Do not ask whether excluded themes should be removed; just avoid creating new items that use them unless removal was explicitly requested.\n"
            "- If the guild already has obvious default rooms like general, rules, announcements, or voice, you may rename them in rename_channels instead of asking what they should become.\n"
            "- Keep it purposeful and practical for the request.\n"
            "- For HQ, dev, tooling, builder, workshop, assistant, or chit-chat requests, create a useful working layout.\n"
            f"- Max {MAX_CATEGORIES} categories total.\n"
            f"- Max {MAX_CHANNELS_PER_CATEGORY} channels per category.\n"
            "- Keep role names short and useful.\n"
            "- Do not include @everyone in roles.\n"
            "- If the user did not explicitly ask for always-respond mode, leave response_channel_action as none.\n"
            "- If the user did not explicitly ask for a server name, you may leave server_name blank.\n\n"
            f"Excluded theme/name tokens: {json.dumps(excluded_terms, ensure_ascii=False)}\n"
            f"User request: {request_text}\n"
            f"Guild snapshot JSON: {json.dumps(guild_snapshot, ensure_ascii=False)}"
        )
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "You output only strict JSON."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=900,
            )
            raw = str((resp.get("message", {}) or {}).get("content", "") or "").strip()
            blob = extract_json(raw) if raw else None
            if not blob:
                return {}
            parsed = json.loads(blob)
            if not isinstance(parsed, dict):
                return {}
            return self._sanitize_creative_outline(parsed, request_text=request_text)
        except Exception as exc:
            logger.warning(f"[discord_admin] creative outline planning failed: {exc}")
            return {}

    async def _llm_creative_role_details(
        self,
        request_text: str,
        role_names: list[str],
        llm_client,
    ) -> list[Dict[str, Any]]:
        if llm_client is None or not role_names:
            return []
        prompt = (
            "Add optional role details for this Discord server layout.\n"
            "Return strict JSON only with this schema:\n"
            '{"roles":[{"name":"Role","color":"#RRGGBB","mentionable":false,"hoist":false,"permissions":["manage_messages"]}]}\n'
            "Rules:\n"
            "- Only return roles from the provided role list.\n"
            "- Keep permissions minimal and purposeful.\n"
            "- Do not include @everyone.\n"
            "- Omit unnecessary power/admin permissions unless clearly justified by the request.\n\n"
            f"User request: {request_text}\n"
            f"Role names: {json.dumps(role_names, ensure_ascii=False)}"
        )
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "You output only strict JSON."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=500,
            )
            raw = str((resp.get("message", {}) or {}).get("content", "") or "").strip()
            blob = extract_json(raw) if raw else None
            if not blob:
                return []
            parsed = json.loads(blob)
            items = parsed.get("roles") if isinstance(parsed, dict) else None
            return items if isinstance(items, list) else []
        except Exception as exc:
            logger.warning(f"[discord_admin] creative role detail planning failed: {exc}")
            return []

    async def _llm_plan_creative(self, request_text: str, guild, llm_client) -> Dict[str, Any]:
        guild_snapshot = self._guild_snapshot(guild)
        outline = await self._llm_creative_outline(request_text, guild_snapshot, llm_client)
        if not self._creative_outline_has_actionable_work(outline):
            outline = self._fallback_creative_outline(request_text, guild_snapshot)
        if not self._creative_outline_has_actionable_work(outline):
            return {}

        plan: Dict[str, Any] = {
            "response_channel_action": str(outline.get("response_channel_action") or "none").strip().lower(),
            "server_name": str(outline.get("server_name") or "").strip(),
            "set_guild_icon_from_attachment": _coerce_bool(outline.get("set_guild_icon_from_attachment"), False),
            "roles": [],
            "categories": [],
            "rename_channels": list(outline.get("rename_channels") or []),
            "notes": str(outline.get("notes") or "").strip(),
        }

        role_names: list[str] = []
        for item in list(outline.get("roles") or [])[:MAX_ROLES]:
            if isinstance(item, dict):
                name = _clean_name(item.get("name"), "")
            else:
                name = _clean_name(item, "")
            if not name or name.lower() == "@everyone":
                continue
            role_names.append(name)
            plan["roles"].append({"name": name, "apply_mode": "create_only"})

        role_details = await self._llm_creative_role_details(request_text, role_names, llm_client)
        details_by_name: Dict[str, Dict[str, Any]] = {}
        for item in role_details:
            if not isinstance(item, dict):
                continue
            name = _clean_name(item.get("name"), "")
            if not name:
                continue
            details_by_name[name.lower()] = item

        merged_roles: list[Dict[str, Any]] = []
        for role in plan["roles"]:
            detail = details_by_name.get(str(role.get("name") or "").strip().lower(), {})
            merged_roles.append(
                {
                    "name": role["name"],
                    "apply_mode": "create_only",
                    "color": detail.get("color"),
                    "mentionable": _coerce_bool(detail.get("mentionable"), False),
                    "hoist": _coerce_bool(detail.get("hoist"), False),
                    "permissions": detail.get("permissions") if isinstance(detail.get("permissions"), list) else [],
                }
            )
        plan["roles"] = merged_roles

        categories: list[Dict[str, Any]] = []
        for category in list(outline.get("categories") or [])[:MAX_CATEGORIES]:
            if not isinstance(category, dict):
                continue
            category_name = _clean_name(category.get("name"), "")
            if not category_name:
                continue
            channels: list[Dict[str, Any]] = []
            for channel in list(category.get("channels") or [])[:MAX_CHANNELS_PER_CATEGORY]:
                if not isinstance(channel, dict):
                    continue
                channel_name = _clean_name(channel.get("name"), "")
                if not channel_name:
                    continue
                kind = str(channel.get("kind") or "text").strip().lower()
                if kind not in {"text", "voice", "vc"}:
                    kind = "text"
                channels.append(
                    {
                        "name": channel_name,
                        "kind": "voice" if kind in {"voice", "vc"} else "text",
                        "apply_mode": "create_only",
                    }
                )
            if channels:
                categories.append(
                    {
                        "name": category_name,
                        "apply_mode": "create_only",
                        "channels": channels,
                    }
                )
        plan["categories"] = categories
        return plan

    def _normalize_plan(self, raw_plan: Dict[str, Any]) -> Dict[str, Any]:
        plan = raw_plan if isinstance(raw_plan, dict) else {}
        response_action = str(plan.get("response_channel_action") or "none").strip().lower()
        if response_action not in VALID_RESPONSE_ACTIONS:
            response_action = "none"

        server_name = _clean_name(plan.get("server_name"), "")
        if server_name:
            server_name = server_name[:100]

        set_guild_icon = _coerce_bool(plan.get("set_guild_icon_from_attachment"), False)

        roles: list[Dict[str, Any]] = []
        for role in list(plan.get("roles") or [])[:MAX_ROLES]:
            if not isinstance(role, dict):
                continue
            name = _clean_name(role.get("name"), "")
            if not name or name.lower() == "@everyone":
                continue
            roles.append(
                {
                    "name": name,
                    "apply_mode": self._normalize_apply_mode(role.get("apply_mode") or role.get("mode")),
                    "color": self._parse_color(role.get("color")),
                    "mentionable": _coerce_bool(role.get("mentionable"), False),
                    "hoist": _coerce_bool(role.get("hoist"), False),
                    "permissions": self._normalize_permission_tokens(role.get("permissions"), valid=VALID_ROLE_PERMISSIONS),
                }
            )

        categories: list[Dict[str, Any]] = []
        for category in list(plan.get("categories") or [])[:MAX_CATEGORIES]:
            if not isinstance(category, dict):
                continue
            category_name = _clean_name(category.get("name"), "")
            if not category_name:
                continue
            normalized_channels: list[Dict[str, Any]] = []
            for ch in list(category.get("channels") or [])[:MAX_CHANNELS_PER_CATEGORY]:
                if not isinstance(ch, dict):
                    continue
                kind = str(ch.get("kind") or "text").strip().lower()
                kind = "voice" if kind in {"voice", "vc"} else "text"
                raw_name = _clean_name(ch.get("name"), "")
                if not raw_name:
                    continue
                name = _text_channel_slug(raw_name) if kind == "text" else raw_name[:95]

                perms: list[Dict[str, Any]] = []
                for perm in list(ch.get("permissions") or []):
                    if not isinstance(perm, dict):
                        continue
                    role_name = _clean_name(perm.get("role"), "")
                    if not role_name:
                        continue
                    allow = self._normalize_permission_tokens(perm.get("allow"), valid=VALID_CHANNEL_PERMISSIONS)
                    deny = self._normalize_permission_tokens(perm.get("deny"), valid=VALID_CHANNEL_PERMISSIONS)
                    if not allow and not deny:
                        continue
                    perms.append({"role": role_name, "allow": allow, "deny": deny})

                channel_spec = {
                    "name": name,
                    "kind": kind,
                    "topic": str(ch.get("topic") or "").strip()[:900],
                    "slowmode_seconds": max(0, min(21600, int(ch.get("slowmode_seconds") or 0))),
                    "nsfw": _coerce_bool(ch.get("nsfw"), False),
                    "apply_mode": self._normalize_apply_mode(ch.get("apply_mode") or ch.get("mode")),
                    "permissions": perms,
                }
                normalized_channels.append(channel_spec)

            categories.append(
                {
                    "name": category_name,
                    "apply_mode": self._normalize_apply_mode(category.get("apply_mode") or category.get("mode")),
                    "channels": normalized_channels,
                }
            )

        keep_meta = self._normalize_keep_channel_refs(plan.get("delete_all_channels_except"))
        keep_category_meta = self._normalize_keep_category_refs(plan.get("delete_all_categories_except"))
        delete_channels = self._normalize_delete_channel_targets(plan.get("delete_channels"))
        delete_categories = self._normalize_delete_category_targets(plan.get("delete_categories"))
        rename_channels = self._normalize_rename_channel_targets(plan.get("rename_channels"))
        confirm_delete_all_channels = _coerce_bool(plan.get("confirm_delete_all_channels"), False)
        confirm_delete_all_categories = _coerce_bool(plan.get("confirm_delete_all_categories"), False)

        return {
            "response_channel_action": response_action,
            "server_name": server_name,
            "set_guild_icon_from_attachment": set_guild_icon,
            "roles": roles,
            "categories": categories,
            "rename_channels": rename_channels,
            "delete_channels": delete_channels,
            "delete_categories": delete_categories,
            "delete_all_channels_except_ids": keep_meta.get("delete_all_channels_except_ids") or [],
            "delete_all_channels_except_names": keep_meta.get("delete_all_channels_except_names") or [],
            "confirm_delete_all_channels": confirm_delete_all_channels,
            "delete_all_categories_except_ids": keep_category_meta.get("delete_all_categories_except_ids") or [],
            "delete_all_categories_except_names": keep_category_meta.get("delete_all_categories_except_names") or [],
            "confirm_delete_all_categories": confirm_delete_all_categories,
            "notes": str(plan.get("notes") or "").strip(),
        }

    async def _apply_response_action(self, message, action: str, *, dry_run: bool) -> Dict[str, Any]:
        guild = getattr(message, "guild", None)
        guild_id = int(getattr(guild, "id", 0) or 0)
        if guild_id <= 0:
            return {"ok": False, "error": "Guild context is unavailable.", "channels": set(), "guild_id": 0}

        settings = self._load_discord_settings()
        if action not in VALID_RESPONSE_ACTIONS or action == "none":
            return {
                "ok": True,
                "changed": False,
                "summary": "",
                "channels": self._load_response_channel_ids_for_guild(guild_id, settings=settings),
                "guild_id": guild_id,
            }

        current = self._load_response_channel_ids_for_guild(guild_id, settings=settings)
        updated = set(current)
        current_channel_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
        if current_channel_id <= 0:
            return {"ok": False, "error": "Current channel is unavailable.", "channels": current, "guild_id": guild_id}

        if action == "add_current":
            updated.add(current_channel_id)
            action_summary = "Added this room to always-response channels."
        elif action == "remove_current":
            updated.discard(current_channel_id)
            action_summary = "Removed this room from always-response channels (ping-only here now)."
        elif action == "set_only_current":
            updated = {current_channel_id}
            action_summary = "Set this room as the only always-response channel."
        elif action == "clear_all":
            updated = set()
            action_summary = "Cleared always-response channels for this server (ping-only here now)."
        else:
            return {"ok": False, "error": "Unknown response channel action.", "channels": current, "guild_id": guild_id}

        if not dry_run:
            self._save_response_channel_ids_for_guild(guild_id, updated)
            self._refresh_runtime_response_channels(message, guild_id, updated)
        label = self._response_label(guild, updated)
        summary = f"{action_summary} Current always-response channels for this server: {label}."
        if dry_run:
            summary = f"[Dry run] {summary}"
        return {
            "ok": True,
            "changed": updated != current,
            "summary": summary,
            "channels": updated,
            "guild_id": guild_id,
        }

    @staticmethod
    def _role_lookup(guild) -> Dict[str, discord.Role]:
        out: Dict[str, discord.Role] = {}
        for role in list(getattr(guild, "roles", []) or []):
            name = str(getattr(role, "name", "") or "").strip().lower()
            if name:
                out[name] = role
        return out

    @staticmethod
    def _resolve_permission_target(guild, roles_by_name: Dict[str, discord.Role], role_name: str):
        name = str(role_name or "").strip().lower()
        if not name:
            return None
        if name in {"@everyone", "everyone"}:
            return getattr(guild, "default_role", None)
        return roles_by_name.get(name)

    def _build_overwrites(
        self,
        guild,
        roles_by_name: Dict[str, discord.Role],
        permission_specs: list[Dict[str, Any]],
        warnings: list[str],
    ):
        if not permission_specs:
            return None
        overwrites: Dict[Any, discord.PermissionOverwrite] = {}
        for spec in permission_specs:
            role_name = str(spec.get("role") or "").strip()
            target = self._resolve_permission_target(guild, roles_by_name, role_name)
            if target is None:
                warnings.append(f"Skipped permissions for unknown role '{role_name}'.")
                continue
            overwrite = discord.PermissionOverwrite()
            changed = False
            for token in spec.get("allow") or []:
                if token in VALID_CHANNEL_PERMISSIONS:
                    setattr(overwrite, token, True)
                    changed = True
            for token in spec.get("deny") or []:
                if token in VALID_CHANNEL_PERMISSIONS:
                    setattr(overwrite, token, False)
                    changed = True
            if changed:
                overwrites[target] = overwrite
        return overwrites or None

    async def _ensure_role(self, guild, role_spec: Dict[str, Any], stats: Dict[str, Any]) -> None:
        role_name = role_spec["name"]
        apply_mode = str(role_spec.get("apply_mode") or "create_only").strip().lower()
        existing = None
        for role in list(getattr(guild, "roles", []) or []):
            if str(getattr(role, "name", "")).strip().lower() == role_name.lower():
                existing = role
                break

        perms = discord.Permissions.none()
        for token in role_spec.get("permissions") or []:
            if token in VALID_ROLE_PERMISSIONS:
                setattr(perms, token, True)

        edit_kwargs: Dict[str, Any] = {
            "permissions": perms,
            "mentionable": bool(role_spec.get("mentionable", False)),
            "hoist": bool(role_spec.get("hoist", False)),
            "reason": "Tater Discord admin setup",
        }
        color_value = role_spec.get("color")
        if color_value is not None:
            edit_kwargs["color"] = discord.Color(int(color_value))

        if existing is None:
            created = await guild.create_role(name=role_name, **edit_kwargs)
            stats["created_roles"].append(str(getattr(created, "name", role_name)))
            return

        if apply_mode != "create_or_update":
            stats["skipped_existing_roles"].append(str(getattr(existing, "name", role_name)))
            return

        await existing.edit(**edit_kwargs)
        stats["updated_roles"].append(str(getattr(existing, "name", role_name)))

    async def _ensure_category(self, guild, category_spec: Dict[str, Any], stats: Dict[str, Any]):
        name = str(category_spec.get("name") or "").strip()
        if not name:
            return None
        apply_mode = str(category_spec.get("apply_mode") or "create_only").strip().lower()
        for category in list(getattr(guild, "categories", []) or []):
            if str(getattr(category, "name", "")).strip().lower() == name.lower():
                if apply_mode == "create_or_update":
                    stats["updated_categories"].append(name)
                else:
                    stats["skipped_existing_categories"].append(name)
                return category
        category = await guild.create_category(name=name, reason="Tater Discord admin setup")
        stats["created_categories"].append(name)
        return category

    @staticmethod
    def _find_channel(category, *, kind: str, name: str):
        expected = str(name or "").strip().lower()
        for channel in list(getattr(category, "channels", []) or []):
            if kind == "text" and not isinstance(channel, discord.TextChannel):
                continue
            if kind == "voice" and not isinstance(channel, discord.VoiceChannel):
                continue
            if str(getattr(channel, "name", "")).strip().lower() == expected:
                return channel
        return None

    async def _ensure_channel(
        self,
        guild,
        category,
        channel_spec: Dict[str, Any],
        roles_by_name: Dict[str, discord.Role],
        stats: Dict[str, Any],
    ) -> None:
        kind = channel_spec.get("kind", "text")
        name = channel_spec.get("name")
        apply_mode = str(channel_spec.get("apply_mode") or "create_only").strip().lower()
        existing = self._find_channel(category, kind=kind, name=name)
        overwrites = self._build_overwrites(
            guild=guild,
            roles_by_name=roles_by_name,
            permission_specs=channel_spec.get("permissions") or [],
            warnings=stats["warnings"],
        )
        reason = "Tater Discord admin setup"

        if kind == "text":
            edit_kwargs: Dict[str, Any] = {
                "name": name,
                "category": category,
                "topic": channel_spec.get("topic") or None,
                "slowmode_delay": int(channel_spec.get("slowmode_seconds") or 0),
                "nsfw": bool(channel_spec.get("nsfw", False)),
                "reason": reason,
            }
            if overwrites is not None:
                edit_kwargs["overwrites"] = overwrites
            if existing is None:
                created = await guild.create_text_channel(**edit_kwargs)
                stats["created_channels"].append(f"#{getattr(created, 'name', name)}")
                return
            if apply_mode != "create_or_update":
                stats["skipped_existing_channels"].append(f"#{getattr(existing, 'name', name)}")
                return
            await existing.edit(**edit_kwargs)
            stats["updated_channels"].append(f"#{getattr(existing, 'name', name)}")
            return

        edit_kwargs = {
            "name": name,
            "category": category,
            "reason": reason,
        }
        if overwrites is not None:
            edit_kwargs["overwrites"] = overwrites
        if existing is None:
            created = await guild.create_voice_channel(**edit_kwargs)
            stats["created_channels"].append(str(getattr(created, "name", name)))
            return
        if apply_mode != "create_or_update":
            stats["skipped_existing_channels"].append(str(getattr(existing, "name", name)))
            return
        await existing.edit(**edit_kwargs)
        stats["updated_channels"].append(str(getattr(existing, "name", name)))

    async def _apply_icon_if_requested(
        self,
        message,
        args: Dict[str, Any],
        plan: Dict[str, Any],
        stats: Dict[str, Any],
    ) -> None:
        if not plan.get("set_guild_icon_from_attachment"):
            return
        attachments = list(getattr(message, "attachments", []) or [])

        image_attachment = None
        for attachment in attachments:
            content_type = str(getattr(attachment, "content_type", "") or "").lower()
            filename = str(getattr(attachment, "filename", "") or "").lower()
            if content_type.startswith("image/") or filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
                image_attachment = attachment
                break
        raw = b""
        if image_attachment is not None:
            raw = await image_attachment.read()
        if not raw:
            raw = self._extract_icon_bytes_from_args(args, stats)
        if not raw:
            stats["warnings"].append(
                "Icon update requested, but no image attachment or media reference was found."
            )
            return
        if len(raw) > ICON_MAX_BYTES:
            stats["warnings"].append("Icon update skipped because the attached image is too large.")
            return

        guild = getattr(message, "guild", None)
        if guild is None:
            stats["warnings"].append("Icon update skipped because guild context was unavailable.")
            return
        await guild.edit(icon=raw, reason="Tater Discord admin setup")
        stats["updated_icon"] = True

    @staticmethod
    def _channel_matches_keep_name(channel: Any, keep_names: set[str]) -> bool:
        if not keep_names:
            return False
        name = str(getattr(channel, "name", "") or "").strip().lower()
        if not name:
            return False
        if name in keep_names:
            return True
        if f"#{name}" in keep_names:
            return True
        return False

    @staticmethod
    def _category_matches_keep_name(category: Any, keep_names: set[str]) -> bool:
        if not keep_names:
            return False
        name = str(getattr(category, "name", "") or "").strip().lower()
        if not name:
            return False
        if name in keep_names:
            return True
        return False

    @staticmethod
    def _channel_kind(channel: Any) -> str:
        explicit = str(getattr(channel, "kind", "") or "").strip().lower()
        if explicit in {"voice", "text"}:
            return explicit
        if isinstance(channel, discord.VoiceChannel):
            return "voice"
        if isinstance(channel, discord.TextChannel):
            return "text"
        return "other"

    @classmethod
    def _channel_matches_delete_target(cls, channel: Any, target: Dict[str, Any], current_channel_id: int) -> bool:
        target_kind = str(target.get("kind") or "any").strip().lower()
        channel_kind = cls._channel_kind(channel)
        if target_kind in {"text", "voice"} and channel_kind != target_kind:
            return False

        target_id = int(target.get("id") or 0)
        if target_id > 0:
            return int(getattr(channel, "id", 0) or 0) == target_id

        target_name = str(target.get("name") or "").strip().lower()
        if not target_name:
            return target_kind in {"text", "voice"}
        if target_name == "__current__":
            return int(getattr(channel, "id", 0) or 0) == int(current_channel_id or 0)

        channel_name = str(getattr(channel, "name", "") or "").strip().lower()
        if not channel_name:
            return False
        return channel_name == target_name or f"#{channel_name}" == target_name

    @staticmethod
    def _category_matches_delete_target(category: Any, target: Dict[str, Any], current_category_id: int) -> bool:
        target_id = int(target.get("id") or 0)
        if target_id > 0:
            return int(getattr(category, "id", 0) or 0) == target_id

        target_name = str(target.get("name") or "").strip().lower()
        if not target_name:
            return False
        if target_name == "__current__":
            return int(getattr(category, "id", 0) or 0) == int(current_category_id or 0)
        category_name = str(getattr(category, "name", "") or "").strip().lower()
        return bool(category_name and category_name == target_name)

    async def _apply_channel_renames(self, message, plan: Dict[str, Any], stats: Dict[str, Any]) -> None:
        guild = getattr(message, "guild", None)
        if guild is None:
            return
        rename_targets = list(plan.get("rename_channels") or [])
        if not rename_targets:
            return

        current_channel = getattr(message, "channel", None)
        current_channel_id = int(getattr(current_channel, "id", 0) or 0)

        for target in rename_targets:
            if not isinstance(target, dict):
                continue
            desired_name = str(target.get("new_name") or "").strip()
            if not desired_name:
                continue

            matches = [
                channel
                for channel in list(getattr(guild, "channels", []) or [])
                if not isinstance(channel, discord.CategoryChannel)
                and self._channel_matches_delete_target(channel, target, current_channel_id)
            ]
            if not matches:
                stats["warnings"].append(
                    f"Channel rename skipped: could not find source channel for target "
                    f"`{target.get('name') or target.get('id') or 'unknown'}`."
                )
                continue
            if len(matches) > 1:
                stats["warnings"].append(
                    "Channel rename skipped: target matched multiple channels; use an exact channel id."
                )
                continue

            channel = matches[0]
            channel_kind = self._channel_kind(channel)
            final_name = _text_channel_slug(desired_name) if channel_kind == "text" else desired_name[:95]
            if not final_name:
                stats["warnings"].append("Channel rename skipped: new channel name was invalid.")
                continue

            current_name = str(getattr(channel, "name", "") or "").strip()
            if current_name.lower() == final_name.lower():
                stats["skipped_existing_channels"].append(
                    f"#{current_name}" if channel_kind == "text" else current_name
                )
                continue

            try:
                await channel.edit(name=final_name, reason="Tater Discord admin setup")
                if channel_kind == "text":
                    stats["updated_channels"].append(f"#{current_name} -> #{final_name}")
                else:
                    stats["updated_channels"].append(f"{current_name} -> {final_name}")
            except Exception as exc:
                stats["warnings"].append(
                    f"Channel rename skipped for '{current_name or getattr(channel, 'id', 'unknown')}': {exc}"
                )

    async def _apply_channel_deletions(self, message, plan: Dict[str, Any], stats: Dict[str, Any]) -> None:
        guild = getattr(message, "guild", None)
        if guild is None:
            return

        raw_delete_targets = list(plan.get("delete_channels") or [])
        delete_targets: list[Dict[str, Any]] = []
        for target in raw_delete_targets:
            if not isinstance(target, dict):
                continue
            target_id = int(target.get("id") or 0)
            target_name = str(target.get("name") or "").strip()
            if target_id > 0 or target_name:
                delete_targets.append(target)
                continue
            stats["warnings"].append(
                "Skipped broad channel delete target because it did not name a specific channel."
            )
        plan_keep_ids = {int(x) for x in (plan.get("delete_all_channels_except_ids") or []) if int(x) > 0}
        plan_keep_names = {
            str(x).strip().lower()
            for x in (plan.get("delete_all_channels_except_names") or [])
            if str(x).strip()
        }
        has_delete_all_request = bool(plan_keep_ids or plan_keep_names)
        has_target_request = bool(delete_targets)
        if not has_target_request and not has_delete_all_request:
            return

        current_channel = getattr(message, "channel", None)
        current_channel_id = int(getattr(current_channel, "id", 0) or 0)
        deleted_ids: set[int] = set()

        if has_target_request:
            for channel in list(getattr(guild, "channels", []) or []):
                if isinstance(channel, discord.CategoryChannel):
                    continue
                if not hasattr(channel, "delete"):
                    continue
                cid = int(getattr(channel, "id", 0) or 0)
                if cid > 0 and cid in deleted_ids:
                    continue
                if not any(self._channel_matches_delete_target(channel, target, current_channel_id) for target in delete_targets):
                    continue
                try:
                    label = f"#{getattr(channel, 'name', cid)}"
                    await channel.delete(reason="Tater Discord admin setup")
                    stats["deleted_channels"].append(label)
                    if cid > 0:
                        deleted_ids.add(cid)
                except Exception as exc:
                    stats["warnings"].append(
                        f"Channel '{getattr(channel, 'name', cid)}' delete skipped: {exc}"
                    )

        if not has_delete_all_request:
            return
        if not _coerce_bool(plan.get("confirm_delete_all_channels"), False):
            stats["warnings"].append(
                "Skipped delete-all-channels action because the request was not explicit enough."
            )
            return

        keep_ids = set(plan_keep_ids)
        keep_names = {name for name in plan_keep_names if name != "__current__"}
        try:
            if current_channel_id > 0:
                keep_ids.add(current_channel_id)
        except Exception:
            pass
        parent = getattr(current_channel, "parent", None)
        try:
            parent_id = int(getattr(parent, "id", 0) or 0)
            if parent_id > 0:
                keep_ids.add(parent_id)
        except Exception:
            pass

        for channel in list(getattr(guild, "channels", []) or []):
            if isinstance(channel, discord.CategoryChannel):
                continue
            cid = int(getattr(channel, "id", 0) or 0)
            if cid > 0 and cid in deleted_ids:
                continue
            if cid > 0 and cid in keep_ids:
                continue
            if self._channel_matches_keep_name(channel, keep_names):
                continue
            if not hasattr(channel, "delete"):
                continue
            try:
                label = f"#{getattr(channel, 'name', cid)}"
                await channel.delete(reason="Tater Discord admin setup")
                stats["deleted_channels"].append(label)
            except Exception as exc:
                stats["warnings"].append(
                    f"Channel '{getattr(channel, 'name', cid)}' delete skipped: {exc}"
                )

    async def _apply_category_deletions(self, message, plan: Dict[str, Any], stats: Dict[str, Any]) -> None:
        guild = getattr(message, "guild", None)
        if guild is None:
            return

        raw_delete_targets = list(plan.get("delete_categories") or [])
        delete_targets: list[Dict[str, Any]] = []
        for target in raw_delete_targets:
            if not isinstance(target, dict):
                continue
            target_id = int(target.get("id") or 0)
            target_name = str(target.get("name") or "").strip()
            if target_id > 0 or target_name:
                delete_targets.append(target)
                continue
            stats["warnings"].append(
                "Skipped broad category delete target because it did not name a specific category."
            )
        plan_keep_ids = {
            int(x)
            for x in (plan.get("delete_all_categories_except_ids") or [])
            if int(x) > 0
        }
        plan_keep_names = {
            str(x).strip().lower()
            for x in (plan.get("delete_all_categories_except_names") or [])
            if str(x).strip()
        }
        has_delete_all_request = bool(plan_keep_ids or plan_keep_names)
        has_target_request = bool(delete_targets)
        if not has_target_request and not has_delete_all_request:
            return

        current_channel = getattr(message, "channel", None)
        current_category = getattr(current_channel, "category", None)
        current_category_id = int(getattr(current_category, "id", 0) or 0)
        deleted_ids: set[int] = set()

        if has_target_request:
            for category in list(getattr(guild, "categories", []) or []):
                if not hasattr(category, "delete"):
                    continue
                cid = int(getattr(category, "id", 0) or 0)
                if cid > 0 and cid in deleted_ids:
                    continue
                if not any(
                    self._category_matches_delete_target(category, target, current_category_id)
                    for target in delete_targets
                ):
                    continue
                try:
                    label = str(getattr(category, "name", cid) or cid)
                    await category.delete(reason="Tater Discord admin setup")
                    stats["deleted_categories"].append(label)
                    if cid > 0:
                        deleted_ids.add(cid)
                except Exception as exc:
                    stats["warnings"].append(
                        f"Category '{getattr(category, 'name', cid)}' delete skipped: {exc}"
                    )

        if not has_delete_all_request:
            return
        if not _coerce_bool(plan.get("confirm_delete_all_categories"), False):
            stats["warnings"].append(
                "Skipped delete-all-categories action because the request was not explicit enough."
            )
            return

        keep_ids = set(plan_keep_ids)
        keep_names = {name for name in plan_keep_names if name != "__current__"}
        try:
            if current_category_id > 0:
                keep_ids.add(current_category_id)
        except Exception:
            pass
        if "__current__" in plan_keep_names and getattr(current_category, "name", None):
            keep_names.add(str(getattr(current_category, "name")).strip().lower())

        for category in list(getattr(guild, "categories", []) or []):
            cid = int(getattr(category, "id", 0) or 0)
            if cid > 0 and cid in deleted_ids:
                continue
            if cid > 0 and cid in keep_ids:
                continue
            if self._category_matches_keep_name(category, keep_names):
                continue
            if not hasattr(category, "delete"):
                continue
            try:
                label = str(getattr(category, "name", cid) or cid)
                await category.delete(reason="Tater Discord admin setup")
                stats["deleted_categories"].append(label)
            except Exception as exc:
                stats["warnings"].append(
                    f"Category '{getattr(category, 'name', cid)}' delete skipped: {exc}"
                )

    async def _apply_plan(self, message, plan: Dict[str, Any], args: Dict[str, Any] | None = None) -> Dict[str, Any]:
        guild = getattr(message, "guild", None)
        payload_args = args if isinstance(args, dict) else {}
        stats: Dict[str, Any] = {
            "created_roles": [],
            "updated_roles": [],
            "skipped_existing_roles": [],
            "created_categories": [],
            "updated_categories": [],
            "deleted_categories": [],
            "skipped_existing_categories": [],
            "created_channels": [],
            "updated_channels": [],
            "deleted_channels": [],
            "skipped_existing_channels": [],
            "updated_server_name": False,
            "updated_icon": False,
            "warnings": [],
        }
        if guild is None:
            stats["warnings"].append("Guild context missing; no setup changes were applied.")
            return stats

        server_name = str(plan.get("server_name") or "").strip()
        if server_name and server_name != str(getattr(guild, "name", "") or ""):
            try:
                await guild.edit(name=server_name, reason="Tater Discord admin setup")
                stats["updated_server_name"] = True
            except Exception as exc:
                stats["warnings"].append(f"Server name update skipped: {exc}")

        for role_spec in list(plan.get("roles") or []):
            try:
                await self._ensure_role(guild, role_spec, stats)
            except Exception as exc:
                stats["warnings"].append(f"Role '{role_spec.get('name', 'unknown')}' skipped: {exc}")

        roles_by_name = self._role_lookup(guild)
        for category_spec in list(plan.get("categories") or []):
            category_name = str(category_spec.get("name") or "").strip()
            if not category_name:
                continue
            try:
                category = await self._ensure_category(guild, category_spec, stats)
            except Exception as exc:
                stats["warnings"].append(f"Category '{category_name}' skipped: {exc}")
                continue
            if category is None:
                continue
            for channel_spec in list(category_spec.get("channels") or []):
                try:
                    await self._ensure_channel(guild, category, channel_spec, roles_by_name, stats)
                except Exception as exc:
                    stats["warnings"].append(f"Channel '{channel_spec.get('name', 'unknown')}' skipped: {exc}")

        try:
            await self._apply_channel_renames(message, plan, stats)
        except Exception as exc:
            stats["warnings"].append(f"Channel rename step skipped: {exc}")

        if self._has_destructive_delete_work(plan):
            stats["warnings"].append(DELETION_DISABLED_TEXT)

        try:
            await self._apply_icon_if_requested(message, payload_args, plan, stats)
        except Exception as exc:
            stats["warnings"].append(f"Guild icon update skipped: {exc}")

        return stats

    @staticmethod
    def _has_setup_work(plan: Dict[str, Any]) -> bool:
        if str(plan.get("server_name") or "").strip():
            return True
        if bool(plan.get("set_guild_icon_from_attachment")):
            return True
        if list(plan.get("roles") or []):
            return True
        if list(plan.get("categories") or []):
            return True
        if list(plan.get("rename_channels") or []):
            return True
        return False

    @staticmethod
    def _summarize_stats(stats: Dict[str, Any]) -> str:
        parts: list[str] = []
        if stats.get("created_roles"):
            parts.append(f"created {len(stats['created_roles'])} role(s)")
        if stats.get("updated_roles"):
            parts.append(f"updated {len(stats['updated_roles'])} role(s)")
        if stats.get("skipped_existing_roles"):
            parts.append(f"left {len(stats['skipped_existing_roles'])} existing role(s) unchanged")
        if stats.get("created_categories"):
            parts.append(f"created {len(stats['created_categories'])} categor(ies)")
        if stats.get("updated_categories"):
            parts.append(f"updated {len(stats['updated_categories'])} categor(ies)")
        if stats.get("deleted_categories"):
            parts.append(f"deleted {len(stats['deleted_categories'])} categor(ies)")
        if stats.get("skipped_existing_categories"):
            parts.append(
                f"left {len(stats['skipped_existing_categories'])} existing categor(ies) unchanged"
            )
        if stats.get("created_channels"):
            parts.append(f"created {len(stats['created_channels'])} channel(s)")
        if stats.get("updated_channels"):
            parts.append(f"updated {len(stats['updated_channels'])} channel(s)")
        if stats.get("deleted_channels"):
            parts.append(f"deleted {len(stats['deleted_channels'])} channel(s)")
        if stats.get("skipped_existing_channels"):
            parts.append(f"left {len(stats['skipped_existing_channels'])} existing channel(s) unchanged")
        if stats.get("updated_server_name"):
            parts.append("updated server name")
        if stats.get("updated_icon"):
            parts.append("updated server icon")
        if not parts:
            parts.append("no structural changes were needed")
        summary = "Discord admin update: " + ", ".join(parts) + "."
        warnings = list(stats.get("warnings") or [])
        if warnings:
            summary += f" Warnings: {len(warnings)}."
        return summary

    async def handle_discord(self, message, args, llm_client):
        request_text = self._extract_request(args, message)
        if not request_text:
            return action_failure(
                code="missing_request",
                message="I need a Discord admin request to act on.",
                needs=["Tell me what to configure (for example: setup this Discord for gaming)."],
                say_hint="Ask for a concrete Discord admin request.",
            )

        if getattr(message, "guild", None) is None:
            return action_failure(
                code="guild_required",
                message="Run this command in a Discord server channel, not in DMs.",
                say_hint="Explain this tool requires a guild channel context.",
            )

        role_summary_prefix = ""
        role_action_applied = False
        route_info = await self._route_request_with_llm(request_text, message, llm_client)
        route = str((route_info or {}).get("route") or "none").strip().lower()
        if route not in {"none", "role", "response_mode", "setup"}:
            route = "none"
        role_action = str((route_info or {}).get("role_action") or "none").strip().lower()
        if role_action not in {"none", "list_roles", "role_me", "role_self"}:
            role_action = "none"
        role_only = _coerce_bool((route_info or {}).get("role_only"), False)
        direct_action = str((route_info or {}).get("response_channel_action") or "none").strip().lower()
        if direct_action not in VALID_RESPONSE_ACTIONS:
            direct_action = "none"
        setup_requested = _coerce_bool((route_info or {}).get("setup_requested"), False)
        setup_mode = str((route_info or {}).get("setup_mode") or "none").strip().lower()
        if setup_mode not in {"none", "exact", "creative"}:
            setup_mode = "none"
        delete_requested = _coerce_bool((route_info or {}).get("delete_requested"), False)

        heuristic_setup = self._infer_setup_route_from_request(request_text)
        if route == "none" and str(heuristic_setup.get("route") or "") == "setup":
            route = "setup"
        if not setup_requested and bool(heuristic_setup.get("setup_requested")):
            setup_requested = True
        heuristic_setup_mode = str(heuristic_setup.get("setup_mode") or "none").strip().lower()
        if setup_mode == "none" and heuristic_setup_mode in {"exact", "creative"}:
            setup_mode = heuristic_setup_mode

        parsed_role_command: Dict[str, Any] = {}
        if role_action != "none":
            parsed_role_command = {
                "action": role_action,
                "role": str((route_info or {}).get("role") or "").strip(),
                "roles": list((route_info or {}).get("roles") or []),
                "role_only": role_only,
                "request_text": request_text,
            }

        if parsed_role_command:
            role_result = await self._handle_direct_role_command(message, parsed_role_command)
            if not bool(role_result.get("ok")):
                return role_result
            role_action_applied = True
            role_summary_prefix = str(role_result.get("summary_for_user") or "").strip()
            if role_only and direct_action == "none" and not setup_requested and not delete_requested:
                return role_result

        args = args or {}
        dry_run = _coerce_bool(args.get("dry_run"), False)
        action_hint = str(args.get("response_channel_action") or "").strip().lower()
        if action_hint in VALID_RESPONSE_ACTIONS:
            direct_action = action_hint

        needs_admin_actions = bool(
            setup_requested or delete_requested or direct_action != "none" or route == "setup"
        )
        allowed, err = self._admin_allowed(message)
        if needs_admin_actions and not allowed:
            if role_action_applied:
                reason = err or "This tool is restricted to the configured Discord admin user."
                summary = role_summary_prefix or "Role command applied."
                return action_success(
                    facts={
                        "role_action_applied": True,
                        "admin_actions_applied": False,
                    },
                    data={"warnings": [reason]},
                    summary_for_user=f"{summary} I could not apply additional Discord admin changes: {reason}",
                    say_hint="Confirm the role command succeeded and explain why additional admin changes were skipped.",
                )
            return action_failure(
                code="admin_only",
                message=err or "This tool is restricted to the configured Discord admin user.",
                say_hint="Explain that this tool is restricted to the configured Discord admin user.",
            )

        if direct_action != "none" and not setup_requested and not delete_requested and route != "setup":
            response_result = await self._apply_response_action(message, direct_action, dry_run=dry_run)
            if not response_result.get("ok"):
                return action_failure(
                    code="response_channel_update_failed",
                    message=str(response_result.get("error") or "Failed to update response channel settings."),
                    say_hint="Explain the response channel update failed and why.",
                )
            response_summary = str(response_result.get("summary") or "Updated response channel settings.").strip()
            if role_summary_prefix:
                response_summary = f"{role_summary_prefix} {response_summary}".strip()
            return action_success(
                facts={
                    "response_channel_action": direct_action,
                    "response_channel_ids": sorted(response_result.get("channels") or []),
                    "guild_id": int(response_result.get("guild_id") or 0),
                    "dry_run": dry_run,
                    "role_action_applied": role_action_applied,
                },
                summary_for_user=response_summary,
                say_hint="Confirm the response channel mode update in plain language.",
            )

        should_plan = bool(setup_requested or delete_requested or route == "setup")
        if should_plan and setup_mode == "none":
            setup_mode = "creative" if route == "setup" else "exact"
        llm_plan = (
            await self._llm_plan(request_text, message.guild, llm_client, setup_mode=setup_mode)
            if should_plan
            else {}
        )
        plan = self._normalize_plan(llm_plan)
        delete_requested = delete_requested or self._has_delete_intent(llm_plan) or self._has_destructive_delete_work(plan)
        if direct_action != "none":
            plan["response_channel_action"] = direct_action

        if (
            delete_requested
            and not self._has_setup_work(plan)
            and str(plan.get("response_channel_action") or "none") == "none"
        ):
            if role_action_applied:
                summary = role_summary_prefix or "Role command applied."
                return action_success(
                    facts={
                        "role_action_applied": True,
                        "admin_actions_applied": False,
                        "delete_requested_but_disabled": True,
                    },
                    data={"warnings": [DELETION_DISABLED_TEXT]},
                    summary_for_user=f"{summary} {DELETION_DISABLED_TEXT}",
                    say_hint="Confirm the role command succeeded and explain deletion is disabled.",
                )
            return action_failure(
                code="deletion_disabled",
                message=DELETION_DISABLED_TEXT,
                needs=[
                    "Delete the channels/categories manually in Discord, then tell me what else you want configured."
                ],
                say_hint="Explain deletion is disabled and ask the user to delete channels/categories manually in Discord.",
            )

        if not self._has_setup_work(plan) and str(plan.get("response_channel_action") or "none") == "none":
            if role_action_applied:
                return action_success(
                    facts={
                        "role_action_applied": True,
                        "admin_actions_applied": False,
                    },
                    summary_for_user=role_summary_prefix or "Role command applied.",
                    say_hint="Confirm the role command and do not claim additional Discord admin changes were applied.",
                )
            return action_failure(
                code="nothing_to_apply",
                message="I could not find a concrete Discord admin change to apply.",
                needs=[
                    "Tell me the theme or exact setup you want (for example: setup this Discord for gaming with LFG channels)."
                ],
                say_hint="Ask for the desired Discord setup details.",
            )

        response_summary = ""
        response_guild_id = int(getattr(getattr(message, "guild", None), "id", 0) or 0)
        if str(plan.get("response_channel_action") or "none") != "none":
            response_result = await self._apply_response_action(
                message, str(plan.get("response_channel_action")), dry_run=dry_run
            )
            if not response_result.get("ok"):
                return action_failure(
                    code="response_channel_update_failed",
                    message=str(response_result.get("error") or "Failed to update response channel settings."),
                    say_hint="Explain the response channel update failed and why.",
                )
            response_guild_id = int(response_result.get("guild_id") or response_guild_id or 0)
            response_summary = str(response_result.get("summary") or "").strip()

        if dry_run:
            planned_roles = len(plan.get("roles") or [])
            planned_categories = len(plan.get("categories") or [])
            planned_channels = sum(len(cat.get("channels") or []) for cat in list(plan.get("categories") or []))
            planned_rename_channels = len(plan.get("rename_channels") or [])
            summary = (
                f"[Dry run] Planned {planned_roles} role(s), {planned_categories} category(ies), "
                f"{planned_channels} channel(s), and {planned_rename_channels} channel rename(s)."
            )
            if delete_requested:
                summary += f" {DELETION_DISABLED_TEXT}"
            if response_summary:
                summary = f"{response_summary} {summary}"
            if role_summary_prefix:
                summary = f"{role_summary_prefix} {summary}"
            return action_success(
                facts={
                    "dry_run": True,
                    "guild_id": response_guild_id,
                    "planned_roles": planned_roles,
                    "planned_categories": planned_categories,
                    "planned_channels": planned_channels,
                    "planned_rename_channels": planned_rename_channels,
                    "delete_requested_but_disabled": delete_requested,
                    "response_channel_action": plan.get("response_channel_action"),
                },
                data={"plan": plan},
                summary_for_user=summary.strip(),
                say_hint="Summarize the dry-run plan and ask if the user wants it applied.",
            )

        stats = await self._apply_plan(message, plan, args=args) if self._has_setup_work(plan) else {
            "created_roles": [],
            "updated_roles": [],
            "skipped_existing_roles": [],
            "created_categories": [],
            "updated_categories": [],
            "deleted_categories": [],
            "skipped_existing_categories": [],
            "created_channels": [],
            "updated_channels": [],
            "deleted_channels": [],
            "skipped_existing_channels": [],
            "updated_server_name": False,
            "updated_icon": False,
            "warnings": [],
        }
        summary = self._summarize_stats(stats)
        if response_summary:
            summary = f"{response_summary} {summary}"
        if role_summary_prefix:
            summary = f"{role_summary_prefix} {summary}"

        return action_success(
            facts={
                "guild_id": response_guild_id,
                "response_channel_action": plan.get("response_channel_action"),
                "created_roles": len(stats.get("created_roles") or []),
                "updated_roles": len(stats.get("updated_roles") or []),
                "created_categories": len(stats.get("created_categories") or []),
                "updated_categories": len(stats.get("updated_categories") or []),
                "deleted_categories": len(stats.get("deleted_categories") or []),
                "created_channels": len(stats.get("created_channels") or []),
                "updated_channels": len(stats.get("updated_channels") or []),
                "deleted_channels": len(stats.get("deleted_channels") or []),
                "updated_server_name": bool(stats.get("updated_server_name")),
                "updated_icon": bool(stats.get("updated_icon")),
                "warnings": len(stats.get("warnings") or []),
            },
            data={"warnings": list(stats.get("warnings") or [])[:10]},
            summary_for_user=summary,
            say_hint="Summarize what Discord admin changes were applied and mention any warnings briefly.",
        )


verba = DiscordAdminRolesPlugin()
