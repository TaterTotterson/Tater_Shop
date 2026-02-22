import json
import logging
import re
from typing import Any, Dict, Iterable, Optional

import discord

from plugin_base import ToolPlugin
from helpers import extract_json, redis_client
from plugin_result import action_failure, action_success

logger = logging.getLogger("discord_admin")
logger.setLevel(logging.INFO)


DISCORD_SETTINGS_KEY = "discord_platform_settings"
RESPONSE_CHANNEL_IDS_BY_GUILD_KEY = "response_channel_ids_by_guild"
MAX_ROLES = 10
MAX_CATEGORIES = 8
MAX_CHANNELS_PER_CATEGORY = 12
ICON_MAX_BYTES = 10 * 1024 * 1024
VALID_ROLE_PERMISSIONS = set(getattr(discord.Permissions, "VALID_FLAGS", {}).keys())
VALID_CHANNEL_PERMISSIONS = set(getattr(discord.Permissions, "VALID_FLAGS", {}).keys())
VALID_RESPONSE_ACTIONS = {"none", "add_current", "remove_current", "set_only_current", "clear_all"}


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


class DiscordAdminPlugin(ToolPlugin):
    name = "discord_admin"
    plugin_name = "Discord Admin"
    pretty_name = "Discord Admin"
    version = "1.0.4"
    min_tater_version = "58.3"
    platforms = ["discord"]

    usage = (
        '{"function":"discord_admin","arguments":{"request":"One Discord admin request in natural language '
        '(for example: setup this discord for gaming, always talk in this room, only respond to ping here)."}}'
    )
    description = (
        "Configure Discord servers from natural language: create themed channel/category layouts, create roles, "
        "set channel permission overwrites, optionally update guild icon from an attached image, and manage which "
        "channels are set to 'always talk here' versus '@ mention only'."
    )
    when_to_use = (
        "Use for Discord server administration requests like setting up a themed server, creating rooms/roles, "
        "editing permissions, setting guild icon, or changing response behavior for the current room."
    )
    plugin_dec = "AI-driven Discord server administration and response-channel control."
    required_args = ["request"]
    optional_args = ["dry_run", "response_channel_action", "theme", "server_name"]
    common_needs = ["A clear Discord admin request, optionally including a theme (gaming, study, etc.)."]
    missing_info_prompts = []

    waiting_prompt_template = (
        "Write one short message telling {mention} you're configuring the Discord server now. "
        "Only output the message."
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

    async def _classify_request_intent_with_llm(self, text: str, llm_client) -> Dict[str, Any]:
        request_text = str(text or "").strip()
        if not request_text or llm_client is None:
            return {"response_channel_action": "none", "setup_requested": False}

        prompt = (
            "Classify this Discord admin request.\n"
            "Return strict JSON only: "
            "{\"response_channel_action\":\"none|add_current|remove_current|set_only_current|clear_all\","
            "\"setup_requested\":true|false}\n\n"
            "Meaning:\n"
            "- add_current: make this room always-response (bot replies to all messages here without ping).\n"
            "- remove_current: remove this room from always-response (ping-only here).\n"
            "- set_only_current: this room should be the only always-response room in this server.\n"
            "- clear_all: remove all always-response rooms in this server.\n"
            "- none: request is not about response-channel behavior.\n\n"
            "- setup_requested: true ONLY when the user asks to change server structure/settings "
            "(create roles/channels/categories, change permissions, set server/guild icon, rename server, "
            "or create a themed server layout).\n\n"
            "Rules:\n"
            "- If the request is mainly server setup/theme/roles/channels/permissions, choose none and setup_requested=true.\n"
            "- If the user asks to respond to all messages here or make this room a response channel, choose add_current.\n"
            "- If the user asks only respond to ping here, choose remove_current.\n"
            "- Handle misspellings and casual phrasing.\n\n"
            f"Request: {request_text}"
        )

        out = {"response_channel_action": "none", "setup_requested": False}
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "You return only strict JSON."},
                    {"role": "user", "content": prompt},
                ]
            )
            raw = str((resp.get("message", {}) or {}).get("content", "") or "").strip()
            blob = extract_json(raw) if raw else None
            candidate = raw
            setup_requested = False
            if blob:
                try:
                    parsed = json.loads(blob)
                except Exception:
                    parsed = None
                if isinstance(parsed, dict):
                    candidate = str(parsed.get("response_channel_action") or "").strip().lower()
                    setup_requested = _coerce_bool(parsed.get("setup_requested"), False)
            candidate = str(candidate or "").strip().lower()
            if candidate in VALID_RESPONSE_ACTIONS:
                out["response_channel_action"] = candidate
                out["setup_requested"] = setup_requested
                return out
            if not (candidate.startswith("{") and candidate.endswith("}")):
                token = re.sub(r"[^a-z_]", "", candidate)
                if token in VALID_RESPONSE_ACTIONS:
                    out["response_channel_action"] = token
                    out["setup_requested"] = setup_requested
                    return out
        except Exception as exc:
            logger.debug(f"[discord_admin] response-action LLM classification failed: {exc}")
        return out

    async def _detect_response_action_with_llm(self, text: str, llm_client) -> str:
        intent = await self._classify_request_intent_with_llm(text, llm_client)
        action = str((intent or {}).get("response_channel_action") or "none").strip().lower()
        return action if action in VALID_RESPONSE_ACTIONS else "none"

    async def _llm_plan(self, request_text: str, guild, llm_client) -> Dict[str, Any]:
        if llm_client is None:
            return {}
        guild_snapshot = {
            "name": str(getattr(guild, "name", "") or ""),
            "id": str(getattr(guild, "id", "") or ""),
            "roles": [str(getattr(role, "name", "") or "") for role in list(getattr(guild, "roles", []) or [])[:40]],
            "categories": [str(getattr(cat, "name", "") or "") for cat in list(getattr(guild, "categories", []) or [])[:30]],
            "channels": [
                {
                    "name": str(getattr(ch, "name", "") or ""),
                    "kind": "voice" if isinstance(ch, discord.VoiceChannel) else "text",
                    "category": str(getattr(getattr(ch, "category", None), "name", "") or ""),
                }
                for ch in list(getattr(guild, "channels", []) or [])[:100]
                if isinstance(ch, (discord.TextChannel, discord.VoiceChannel))
            ],
        }

        allowed_perms = ", ".join(sorted(VALID_CHANNEL_PERMISSIONS))
        prompt = (
            "Build a safe Discord admin execution plan. Return strict JSON only with this schema:\n"
            "{\n"
            '  "response_channel_action":"none|add_current|remove_current|set_only_current|clear_all",\n'
            '  "server_name":"optional server name",\n'
            '  "set_guild_icon_from_attachment":false,\n'
            '  "roles":[{"name":"Role","apply_mode":"create_only|create_or_update","color":"#RRGGBB","mentionable":false,"hoist":false,"permissions":["manage_messages"]}],\n'
            '  "categories":[{"name":"Category","channels":[{"name":"general","kind":"text|voice","topic":"optional",'
            '"slowmode_seconds":0,"nsfw":false,'
            '"apply_mode":"create_only|create_or_update",'
            '"permissions":[{"role":"@everyone","allow":["view_channel"],"deny":["send_messages"]}]}]}],\n'
            '  "notes":"short optional notes"\n'
            "}\n"
            "Rules:\n"
            "- Non-destructive only (never delete existing channels/roles).\n"
            "- Do NOT output boilerplate/default rooms. Generate only changes inferred from the user request.\n"
            "- Choose minimal change set. If no server-structure changes are needed, return empty roles/categories and blank server_name.\n"
            "- Default apply_mode should be create_only. Use create_or_update only when user explicitly asks to modify an existing role/channel.\n"
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

            if normalized_channels:
                categories.append({"name": category_name, "channels": normalized_channels})

        return {
            "response_channel_action": response_action,
            "server_name": server_name,
            "set_guild_icon_from_attachment": set_guild_icon,
            "roles": roles,
            "categories": categories,
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

    async def _ensure_category(self, guild, name: str, stats: Dict[str, Any]):
        for category in list(getattr(guild, "categories", []) or []):
            if str(getattr(category, "name", "")).strip().lower() == name.lower():
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

    async def _apply_icon_if_requested(self, message, plan: Dict[str, Any], stats: Dict[str, Any]) -> None:
        if not plan.get("set_guild_icon_from_attachment"):
            return
        attachments = list(getattr(message, "attachments", []) or [])
        if not attachments:
            stats["warnings"].append("Icon update requested, but no image attachment was provided.")
            return

        image_attachment = None
        for attachment in attachments:
            content_type = str(getattr(attachment, "content_type", "") or "").lower()
            filename = str(getattr(attachment, "filename", "") or "").lower()
            if content_type.startswith("image/") or filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
                image_attachment = attachment
                break
        if image_attachment is None:
            stats["warnings"].append("Icon update requested, but no image attachment was found.")
            return

        raw = await image_attachment.read()
        if not raw:
            stats["warnings"].append("Icon update requested, but the image attachment was empty.")
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

    async def _apply_plan(self, message, plan: Dict[str, Any]) -> Dict[str, Any]:
        guild = getattr(message, "guild", None)
        stats: Dict[str, Any] = {
            "created_roles": [],
            "updated_roles": [],
            "skipped_existing_roles": [],
            "created_categories": [],
            "created_channels": [],
            "updated_channels": [],
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
                category = await self._ensure_category(guild, category_name, stats)
            except Exception as exc:
                stats["warnings"].append(f"Category '{category_name}' skipped: {exc}")
                continue
            for channel_spec in list(category_spec.get("channels") or []):
                try:
                    await self._ensure_channel(guild, category, channel_spec, roles_by_name, stats)
                except Exception as exc:
                    stats["warnings"].append(f"Channel '{channel_spec.get('name', 'unknown')}' skipped: {exc}")

        try:
            await self._apply_icon_if_requested(message, plan, stats)
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
        if stats.get("created_channels"):
            parts.append(f"created {len(stats['created_channels'])} channel(s)")
        if stats.get("updated_channels"):
            parts.append(f"updated {len(stats['updated_channels'])} channel(s)")
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

        allowed, err = self._admin_allowed(message)
        if not allowed:
            return action_failure(
                code="admin_only",
                message=err or "This tool is restricted to the configured Discord admin user.",
                say_hint="Explain that this tool is restricted to the configured Discord admin user.",
            )

        args = args or {}
        dry_run = _coerce_bool(args.get("dry_run"), False)

        intent = await self._classify_request_intent_with_llm(request_text, llm_client)
        direct_action = str((intent or {}).get("response_channel_action") or "none").strip().lower()
        if direct_action not in VALID_RESPONSE_ACTIONS:
            direct_action = "none"
        setup_requested = _coerce_bool((intent or {}).get("setup_requested"), False)
        action_hint = str(args.get("response_channel_action") or "").strip().lower()
        if action_hint in VALID_RESPONSE_ACTIONS:
            direct_action = action_hint

        should_setup = bool(setup_requested)
        if direct_action != "none" and not should_setup:
            response_result = await self._apply_response_action(message, direct_action, dry_run=dry_run)
            if not response_result.get("ok"):
                return action_failure(
                    code="response_channel_update_failed",
                    message=str(response_result.get("error") or "Failed to update response channel settings."),
                    say_hint="Explain the response channel update failed and why.",
                )
            return action_success(
                facts={
                    "response_channel_action": direct_action,
                    "response_channel_ids": sorted(response_result.get("channels") or []),
                    "guild_id": int(response_result.get("guild_id") or 0),
                    "dry_run": dry_run,
                },
                summary_for_user=str(response_result.get("summary") or "Updated response channel settings."),
                say_hint="Confirm the response channel mode update in plain language.",
            )

        llm_plan = await self._llm_plan(request_text, message.guild, llm_client) if should_setup else {}
        plan = self._normalize_plan(llm_plan)
        if direct_action != "none":
            plan["response_channel_action"] = direct_action

        if not self._has_setup_work(plan) and str(plan.get("response_channel_action") or "none") == "none":
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
            summary = (
                f"[Dry run] Planned {planned_roles} role(s), {planned_categories} category(ies), "
                f"and {planned_channels} channel(s)."
            )
            if response_summary:
                summary = f"{response_summary} {summary}"
            return action_success(
                facts={
                    "dry_run": True,
                    "guild_id": response_guild_id,
                    "planned_roles": planned_roles,
                    "planned_categories": planned_categories,
                    "planned_channels": planned_channels,
                    "response_channel_action": plan.get("response_channel_action"),
                },
                data={"plan": plan},
                summary_for_user=summary.strip(),
                say_hint="Summarize the dry-run plan and ask if the user wants it applied.",
            )

        stats = await self._apply_plan(message, plan) if self._has_setup_work(plan) else {
            "created_roles": [],
            "updated_roles": [],
            "skipped_existing_roles": [],
            "created_categories": [],
            "created_channels": [],
            "updated_channels": [],
            "skipped_existing_channels": [],
            "updated_server_name": False,
            "updated_icon": False,
            "warnings": [],
        }
        summary = self._summarize_stats(stats)
        if response_summary:
            summary = f"{response_summary} {summary}"

        return action_success(
            facts={
                "guild_id": response_guild_id,
                "response_channel_action": plan.get("response_channel_action"),
                "created_roles": len(stats.get("created_roles") or []),
                "updated_roles": len(stats.get("updated_roles") or []),
                "created_categories": len(stats.get("created_categories") or []),
                "created_channels": len(stats.get("created_channels") or []),
                "updated_channels": len(stats.get("updated_channels") or []),
                "updated_server_name": bool(stats.get("updated_server_name")),
                "updated_icon": bool(stats.get("updated_icon")),
                "warnings": len(stats.get("warnings") or []),
            },
            data={"warnings": list(stats.get("warnings") or [])[:10]},
            summary_for_user=summary,
            say_hint="Summarize what Discord admin changes were applied and mention any warnings briefly.",
        )


plugin = DiscordAdminPlugin()
