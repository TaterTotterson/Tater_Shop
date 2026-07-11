# verba/broadcast.py
import json
import logging
import re
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from announcement_targets import build_announcement_target_options, normalize_announcement_targets
try:
    from announcement_targets import VOICE_CORE_TARGET_PREFIX, get_voice_core_satellite_target_options
except Exception:
    VOICE_CORE_TARGET_PREFIX = "voice_core:"
    get_voice_core_satellite_target_options = None
from tateros import integration_store as integration_store_module
from helpers import get_tater_name, redis_client
from speech_settings import get_speech_settings
from speech_tts import speak_announcement_targets
from verba_base import ToolVerba
from verba_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("broadcast")
logger.setLevel(logging.INFO)


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> dict:
    module = integration_store_module.integration_module("homeassistant")
    if module is not None:
        return module.load_homeassistant_config(required=required, client=client)
    if required:
        raise ValueError("Home Assistant integration is not enabled.")
    return {"base": "", "token": ""}


class BroadcastPlugin(ToolVerba):
    """Broadcast a spoken announcement to Tater satellites, Sonos, or Home Assistant media-player targets."""

    name = "broadcast"
    verba_name = "Broadcast"
    version = "1.2.0"
    min_tater_version = "59"
    usage = '{"function":"broadcast","arguments":{"text":"<what to announce>","room":"<optional room or satellite>"}}'
    description = (
        "Send a one-time spoken announcement. If the user specifies a room, area, or satellite, send it only there. "
        "If no room is specified, send it everywhere using connected Tater satellites plus configured extra announcement targets. "
        "Use ONLY when the user explicitly asks to broadcast/announce/page an audio message and provides what to say."
    )
    verba_dec = "Send a one-time spoken announcement to all satellites or one specified room."
    pretty_name = "Broadcast Announcement"
    settings_category = "Broadcast"

    required_settings = {
        "TARGETS": {
            "label": "Extra Broadcast Targets",
            "type": "multiselect",
            "default": [],
            "description": "Optional extra Sonos or Home Assistant targets for whole-house broadcasts. Connected Tater satellites are included automatically.",
            "options": [],
        },
    }

    waiting_prompt_template = (
        "Write a short friendly message telling {mention} you’re broadcasting that announcement now. "
        "Only output that message."
    )

    platforms = ['voice_core', 'homeassistant', 'homekit', 'xbmc', 'webui', 'little_spud', 'macos', 'discord', 'telegram', 'matrix', 'irc', 'meshtastic']
    when_to_use = ""
    common_needs = []
    missing_info_prompts = []

    @staticmethod
    def _decode_redis_map(raw: Dict[Any, Any] | None) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
            if isinstance(value, (bytes, bytearray)):
                out[k] = value.decode("utf-8", "ignore")
            else:
                out[k] = str(value or "")
        return out

    def _get_settings(self) -> dict:
        raw = redis_client.hgetall(f"verba_settings:{self.settings_category}") or redis_client.hgetall(
            f"verba_settings: {self.settings_category}"
        )
        return self._decode_redis_map(raw)

    def _targets(self, settings: dict | None) -> list[str]:
        values = settings if isinstance(settings, dict) else {}
        configured = normalize_announcement_targets(values.get("TARGETS"))
        legacy = [
            (values.get("DEVICE_1") or "").strip(),
            (values.get("DEVICE_2") or "").strip(),
            (values.get("DEVICE_3") or "").strip(),
            (values.get("DEVICE_4") or "").strip(),
            (values.get("DEVICE_5") or "").strip(),
        ]
        return self._unique_targets([*configured, *normalize_announcement_targets([item for item in legacy if item])])

    @staticmethod
    def _unique_targets(values: List[str]) -> list[str]:
        rows: list[str] = []
        seen = set()
        for value in values:
            target = str(value or "").strip()
            if not target or target in seen:
                continue
            seen.add(target)
            rows.append(target)
        return rows

    @staticmethod
    def _norm(value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()

    @staticmethod
    def _is_everywhere(value: Any) -> bool:
        return BroadcastPlugin._norm(value) in {
            "",
            "all",
            "all rooms",
            "all satellites",
            "every room",
            "everywhere",
            "house",
            "the house",
            "whole house",
        }

    def _satellite_target_options(self, current_values: Any = None) -> list[dict[str, str]]:
        if not callable(get_voice_core_satellite_target_options):
            return []
        try:
            rows = get_voice_core_satellite_target_options(current_values=current_values)
        except Exception as exc:
            logger.warning("[broadcast] failed to load Tater satellite targets: %s", exc)
            return []
        return [dict(row) for row in list(rows or []) if isinstance(row, dict)]

    def _satellite_targets(self) -> list[str]:
        values = [
            str(row.get("value") or "").strip()
            for row in self._satellite_target_options(current_values=[])
            if str(row.get("value") or "").strip().startswith(VOICE_CORE_TARGET_PREFIX)
        ]
        return self._unique_targets(values)

    def _satellite_aliases(self, option: dict[str, str]) -> list[str]:
        value = str(option.get("value") or "").strip()
        label = str(option.get("label") or "").strip()
        aliases = [label]

        body = label
        if ":" in body:
            body = body.split(":", 1)[1].strip()
        title, _, detail = body.partition("(")
        if title.strip():
            aliases.append(title.strip())
        if detail:
            for part in detail.rstrip(")").split("•"):
                token = part.strip()
                lower = token.lower()
                if token and not lower.startswith(("voice_core:", "native:", "host:")):
                    aliases.append(token)

        if value.startswith(VOICE_CORE_TARGET_PREFIX):
            selector = value[len(VOICE_CORE_TARGET_PREFIX):].strip()
            if selector:
                aliases.append(selector)
                if selector.startswith("native:"):
                    aliases.append(selector.split(":", 1)[1])
                if selector.startswith("host:"):
                    aliases.append(selector.split(":", 1)[1])

        normalized: list[str] = []
        seen = set()
        for alias in aliases:
            token = self._norm(alias)
            if not token or token in seen or token in {"tater", "satellite", "tater satellite"}:
                continue
            seen.add(token)
            normalized.append(token)
        return normalized

    def _room_targets(self, room: Any) -> list[str]:
        query = str(room or "").strip()
        if self._is_everywhere(query):
            return self._satellite_targets()

        lower = query.lower()
        if lower.startswith(VOICE_CORE_TARGET_PREFIX):
            return normalize_announcement_targets([query])
        if lower.startswith(("native:", "host:")):
            return normalize_announcement_targets([f"{VOICE_CORE_TARGET_PREFIX}{query}"])

        query_norm = self._norm(query)
        if not query_norm:
            return []

        matches: list[str] = []
        for option in self._satellite_target_options(current_values=[]):
            value = str(option.get("value") or "").strip()
            if not value.startswith(VOICE_CORE_TARGET_PREFIX):
                continue
            aliases = self._satellite_aliases(option)
            if any(query_norm == alias or query_norm in alias or alias in query_norm for alias in aliases):
                matches.append(value)
        return self._unique_targets(matches)

    def _select_targets(self, settings: dict | None, room: str = "") -> Tuple[list[str], str]:
        configured = self._targets(settings)
        extra_targets = [target for target in configured if not target.startswith(VOICE_CORE_TARGET_PREFIX)]

        if room and not self._is_everywhere(room):
            return self._room_targets(room), "room"

        satellite_targets = self._satellite_targets()
        if satellite_targets:
            return self._unique_targets([*satellite_targets, *extra_targets]), "everywhere"
        return configured, "configured"

    def webui_settings_fields(self, fields, current_settings=None, redis_client=None, notifier_destination_catalog=None):
        rows = [dict(field) if isinstance(field, dict) else field for field in list(fields or [])]
        current = dict(current_settings or {})
        current_targets = self._targets(current)
        ha = load_homeassistant_config(required=False)

        target_options = build_announcement_target_options(
            homeassistant_base_url=ha.get("base", ""),
            homeassistant_token=ha.get("token", ""),
            include_homeassistant=True,
            include_sonos=True,
            include_voice_core=False,
            current_values=current_targets,
        )

        for field in rows:
            if not isinstance(field, dict):
                continue
            if str(field.get("key") or "").strip() != "TARGETS":
                continue
            field["type"] = "multiselect"
            field["options"] = list(target_options)
            field["value"] = list(current_targets)
            field["default"] = []
            break
        return rows

    def webui_prepare_settings_values(self, values=None, redis_client=None):
        out = dict(values or {})
        targets = normalize_announcement_targets(out.get("TARGETS"))
        out["TARGETS"] = json.dumps(targets, ensure_ascii=False)
        for legacy_key in ("DEVICE_1", "DEVICE_2", "DEVICE_3", "DEVICE_4", "DEVICE_5", "TTS_ENTITY"):
            out[legacy_key] = ""
        return out

    def _siri_flatten(self, text: str | None) -> str:
        if not text:
            return "No announcement."
        out = str(text)
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out[:450]

    @staticmethod
    def _extract_announcement_arg(args: Dict[str, Any]) -> str:
        args = args or {}
        for key in ("text", "announcement", "message", "content", "request"):
            value = args.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _extract_room_arg(args: Dict[str, Any]) -> str:
        args = args or {}
        for key in ("room", "area", "target_room", "target", "location", "device", "satellite", "where"):
            value = args.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _split_room_from_text(self, raw_text: str) -> Tuple[str, str]:
        text = str(raw_text or "").strip()
        if not text:
            return "", ""

        body = re.sub(r"^\s*(?:broadcast|announce|page)\b\s*", "", text, flags=re.I).strip()
        options = self._satellite_target_options(current_values=[])
        alias_rows: list[tuple[str, str]] = []
        for option in options:
            for alias in self._satellite_aliases(option):
                if len(alias) >= 3:
                    alias_rows.append((alias, str(option.get("label") or alias).strip()))
        alias_rows.sort(key=lambda item: len(item[0]), reverse=True)

        body_norm = self._norm(body)
        for alias_norm, label in alias_rows:
            phrase = re.escape(alias_norm).replace("\\ ", r"\s+")
            match = re.search(rf"\b(?:to|in|into|for)\s+(?:the\s+)?{phrase}\b", body_norm)
            if not match:
                continue
            start, end = match.span()
            cleaned = f"{body_norm[:start].strip()} {body_norm[end:].strip()}".strip()
            cleaned = re.sub(r"^\s*(?:that|say|saying|tell|announce|broadcast|page)\b\s*", "", cleaned, flags=re.I).strip(" :,-")
            return cleaned or text, label

        match = re.match(
            r"^(?:to|in|into|for)\s+(?:the\s+)?(?P<room>[a-zA-Z0-9 _-]{2,40}?)(?:\s*[:,]\s*|\s+(?:that|say|saying|tell)\s+)(?P<message>.+)$",
            body,
            flags=re.I,
        )
        if match:
            return match.group("message").strip(), match.group("room").strip()

        return body or text, ""

    def _request_from_args(self, args: Dict[str, Any]) -> Tuple[str, str]:
        args = args or {}
        text = self._extract_announcement_arg(args)
        room = self._extract_room_arg(args)
        if not room:
            text, room = self._split_room_from_text(text)
        return text, room

    async def _make_announcement_text(self, raw_text: str, llm_client) -> str:
        raw_text = (raw_text or "").strip()
        if not raw_text:
            return ""

        first, last = get_tater_name()
        max_chars = 220
        prompt = (
            f"Your name is {first} {last}. The user wants you to broadcast an announcement.\n\n"
            f"User said: {raw_text}\n\n"
            "Rewrite that into ONE short, natural-sounding sentence suitable for a whole-house announcement.\n"
            "Rules:\n"
            "- No emojis\n"
            "- No markdown\n"
            "- Keep it friendly and clear\n"
            f"- Under {max_chars} characters\n"
            "Only output the sentence."
        )

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "You rewrite announcements for clear spoken delivery."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=120,
                temperature=0.2,
            )
            txt = (resp.get("message") or {}).get("content", "")
            txt = (txt or "").strip().strip('"').strip()
            if txt:
                return txt[:max_chars]
        except Exception as exc:
            logger.warning("[broadcast] LLM rewrite failed, using raw text: %s", exc)

        return raw_text[:max_chars]

    async def _broadcast(self, raw_text: str, llm_client, room: str = "") -> dict:
        settings = self._get_settings()
        targets, target_scope = self._select_targets(settings, room)
        if not targets:
            if room and not self._is_everywhere(room):
                return action_failure(
                    code="broadcast_room_not_found",
                    message=f"No connected Tater satellite matched {room!r}.",
                    needs=["Try another room name or check that the satellite is connected."],
                    say_hint="Explain that no connected satellite matched that room.",
                )
            return action_failure(
                code="no_broadcast_targets",
                message="No broadcast targets are configured.",
                needs=["Connect a Tater satellite or configure one or more extra broadcast targets."],
                say_hint="Explain there are no connected broadcast targets.",
            )

        announcement = (
            await self._make_announcement_text(raw_text, llm_client)
            if llm_client
            else (raw_text or "").strip()
        )
        if not announcement:
            return action_failure(
                code="missing_announcement_text",
                message="No announcement text was provided.",
                needs=["What should I announce?"],
                say_hint="Ask what announcement text should be broadcast.",
            )

        speech = get_speech_settings()
        announcement_backend = str(speech.get("announcement_tts_backend") or speech.get("tts_backend") or "wyoming")
        announcement_model = str(speech.get("announcement_tts_model") or "")
        announcement_voice = str(speech.get("announcement_tts_voice") or "")
        direct_tts_backend = str(speech.get("tts_backend") or "")
        direct_tts_model = str(speech.get("tts_model") or "")
        direct_tts_voice = str(speech.get("tts_voice") or "")
        ha = load_homeassistant_config(required=False)

        try:
            tts_result = await speak_announcement_targets(
                text=announcement,
                backend=announcement_backend,
                ha_base=str(ha.get("base") or ""),
                token=str(ha.get("token") or ""),
                targets=targets,
                model=announcement_model,
                voice=announcement_voice,
                wyoming_host=str(speech.get("wyoming_tts_host") or ""),
                wyoming_port=speech.get("wyoming_tts_port"),
                wyoming_voice=str(speech.get("wyoming_tts_voice") or ""),
                voice_core_backend=direct_tts_backend,
                voice_core_model=direct_tts_model,
                voice_core_voice=direct_tts_voice,
                voice_core_wyoming_host=str(speech.get("wyoming_tts_host") or ""),
                voice_core_wyoming_port=speech.get("wyoming_tts_port"),
                voice_core_wyoming_voice=str(speech.get("wyoming_tts_voice") or ""),
                default_backend=announcement_backend,
            )
        except Exception as exc:
            logger.error("[broadcast] TTS call failed: %s", exc)
            return action_failure(
                code="broadcast_tts_error",
                message=f"Broadcast failed due to a TTS service call error: {exc}",
                say_hint="Explain the TTS service call failed and suggest retrying.",
            )

        backend_used = str(tts_result.get("backend") or announcement_backend)
        sent_count = int(tts_result.get("sent_count") or 0)
        target_count = int(tts_result.get("target_count") or len(targets))
        warnings = [str(item) for item in list(tts_result.get("warnings") or []) if str(item).strip()]

        if not tts_result.get("ok") or sent_count <= 0:
            detail = str(tts_result.get("error") or "").strip()
            return action_failure(
                code="broadcast_all_targets_failed",
                message=f"Broadcast failed on all target devices.{f' {detail}' if detail else ''}",
                say_hint="Explain that all target devices rejected the broadcast.",
            )

        facts = {
            "announcement": announcement,
            "tts_backend": backend_used,
            "tts_model": announcement_model,
            "tts_voice": announcement_voice,
            "target_count": target_count,
            "ok_count": sent_count,
            "failed_targets": warnings[:10],
            "homeassistant_target_count": int(tts_result.get("homeassistant_target_count") or 0),
            "voice_core_target_count": int(tts_result.get("voice_core_target_count") or 0),
            "homeassistant_sent_count": int(tts_result.get("homeassistant_sent_count") or 0),
            "voice_core_sent_count": int(tts_result.get("voice_core_sent_count") or 0),
            "targets": list(targets),
            "target_scope": target_scope,
        }
        if room:
            facts["requested_room"] = room
        if tts_result.get("media_url"):
            facts["media_url"] = str(tts_result.get("media_url"))

        if warnings:
            return action_success(
                facts=facts,
                summary_for_user=(
                    f"Broadcast sent to {sent_count} of {target_count} target{'s' if target_count != 1 else ''} using {backend_used}. "
                    f"{len(warnings)} targets failed."
                ),
                say_hint="Confirm partial broadcast success and mention failed targets if useful.",
            )

        return action_success(
            facts=facts,
            summary_for_user=f"Broadcast sent to {target_count} target{'s' if target_count != 1 else ''} using {backend_used}.",
            say_hint="Confirm the broadcast was sent successfully.",
        )

    async def _broadcast_from_args(self, args: Dict[str, Any], llm_client) -> dict:
        text, room = self._request_from_args(args or {})
        return await self._broadcast(text, llm_client, room=room)

    async def handle_homeassistant(self, args, llm_client):
        return await self._broadcast_from_args(args or {}, llm_client)

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        try:
            return await self.handle_homeassistant(args=args, llm_client=llm_client, context=context)
        except TypeError:
            try:
                return await self.handle_homeassistant(args=args, llm_client=llm_client)
            except TypeError:
                return await self.handle_homeassistant(args, llm_client)

    async def handle_webui(self, args, llm_client):
        return await self._broadcast_from_args(args or {}, llm_client)

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self.handle_webui(args or {}, llm_client)

    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        return await self._broadcast_from_args(args or {}, llm_client)

    async def handle_xbmc(self, args, llm_client):
        return await self._broadcast_from_args(args or {}, llm_client)

    async def handle_discord(self, message, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        return await self.handle_webui(args or {}, llm_client)


    async def handle_meshtastic(self, args=None, llm_client=None, context=None, **kwargs):
        args = args or {}
        ctx = context if isinstance(context, dict) else {}
        origin = ctx.get("origin") if isinstance(ctx.get("origin"), dict) else {}
        sender = ""
        source_from = origin.get("from")
        if isinstance(source_from, dict):
            sender = str(source_from.get("node_id") or source_from.get("long_name") or source_from.get("short_name") or "").strip()
        channel = str(ctx.get("channel") or origin.get("channel") or origin.get("target") or origin.get("channel_id") or "").strip()
        user = str(ctx.get("user") or origin.get("user") or origin.get("user_id") or sender or "").strip()
        raw_text = str(
            ctx.get("raw_message")
            or ctx.get("raw")
            or ctx.get("request_text")
            or origin.get("text")
            or origin.get("message")
            or origin.get("body")
            or ""
        ).strip()
        call_kwargs = {"args": args, "llm_client": llm_client}
        try:
            sig = __import__("inspect").signature(self.handle_irc)
        except Exception:
            sig = None
        if sig is not None:
            if "bot" in sig.parameters:
                call_kwargs["bot"] = None
            if "channel" in sig.parameters:
                call_kwargs["channel"] = channel
            if "user" in sig.parameters:
                call_kwargs["user"] = user
            if "raw_message" in sig.parameters:
                call_kwargs["raw_message"] = raw_text
            if "raw" in sig.parameters:
                call_kwargs["raw"] = raw_text
            if "context" in sig.parameters:
                call_kwargs["context"] = ctx
        return await self.handle_irc(**call_kwargs)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)


verba = BroadcastPlugin()
