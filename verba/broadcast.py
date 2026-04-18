# verba/broadcast.py
import json
import logging
import re
from typing import Any, Dict, List

from dotenv import load_dotenv

from announcement_targets import build_announcement_target_options, normalize_announcement_targets
from helpers import get_tater_name, redis_client
from speech_settings import get_speech_settings
from speech_tts import speak_announcement_targets
from verba_base import ToolVerba
from verba_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("broadcast")
logger.setLevel(logging.INFO)


class BroadcastPlugin(ToolVerba):
    """Broadcast a spoken announcement to configured Voice Core targets."""

    name = "broadcast"
    verba_name = "Broadcast"
    version = "1.1.8"
    min_tater_version = "59"
    usage = '{"function":"broadcast","arguments":{"text":"<what to announce>"}}'
    description = (
        "Send a one-time whole-house spoken announcement using the global announcement TTS settings on the configured targets. "
        "Use ONLY when the user explicitly asks to broadcast/announce/page an audio message and provides what to say."
    )
    verba_dec = "Send a one-time spoken announcement to your configured broadcast targets."
    pretty_name = "Broadcast Announcement"
    settings_category = "Broadcast"

    required_settings = {
        "TARGETS": {
            "label": "Broadcast Targets",
            "type": "multiselect",
            "default": [],
            "description": "Choose one or more Voice Core satellites.",
            "options": [],
        },
    }

    waiting_prompt_template = (
        "Write a short friendly message telling {mention} you’re broadcasting that announcement now. "
        "Only output that message."
    )

    platforms = ['voice_core', 'homeassistant', 'homekit', 'xbmc', 'webui', 'macos', 'discord', 'telegram', 'matrix', 'irc']
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
        if configured:
            return configured

        legacy = [
            (values.get("DEVICE_1") or "").strip(),
            (values.get("DEVICE_2") or "").strip(),
            (values.get("DEVICE_3") or "").strip(),
            (values.get("DEVICE_4") or "").strip(),
            (values.get("DEVICE_5") or "").strip(),
        ]
        return normalize_announcement_targets([item for item in legacy if item])

    def webui_settings_fields(self, fields, current_settings=None, redis_client=None, notifier_destination_catalog=None):
        rows = [dict(field) if isinstance(field, dict) else field for field in list(fields or [])]
        current = dict(current_settings or {})
        current_targets = self._targets(current)

        target_options = build_announcement_target_options(
            homeassistant_base_url="",
            homeassistant_token="",
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

    async def _broadcast(self, raw_text: str, llm_client) -> dict:
        settings = self._get_settings()
        targets = self._targets(settings)
        if not targets:
            return action_failure(
                code="no_broadcast_targets",
                message="No broadcast targets are configured.",
                needs=["Please configure one or more broadcast targets."],
                say_hint="Explain there are no configured broadcast targets.",
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

        try:
            tts_result = await speak_announcement_targets(
                text=announcement,
                backend=announcement_backend,
                ha_base="",
                token="",
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
        }
        if tts_result.get("media_url"):
            facts["media_url"] = str(tts_result.get("media_url"))

        if warnings:
            return action_success(
                facts=facts,
                summary_for_user=(
                    f"Broadcast sent to {sent_count} of {target_count} targets using {backend_used}. "
                    f"{len(warnings)} targets failed."
                ),
                say_hint="Confirm partial broadcast success and mention failed targets if useful.",
            )

        return action_success(
            facts=facts,
            summary_for_user=f"Broadcast sent to all {target_count} configured targets using {backend_used}.",
            say_hint="Confirm the broadcast was sent successfully.",
        )

    async def handle_homeassistant(self, args, llm_client):
        args = args or {}
        text = self._extract_announcement_arg(args)
        return await self._broadcast(text, llm_client)

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        try:
            return await self.handle_homeassistant(args=args, llm_client=llm_client, context=context)
        except TypeError:
            try:
                return await self.handle_homeassistant(args=args, llm_client=llm_client)
            except TypeError:
                return await self.handle_homeassistant(args, llm_client)

    async def handle_webui(self, args, llm_client):
        args = args or {}
        return await self._broadcast(self._extract_announcement_arg(args), llm_client)

    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        args = args or {}
        return await self._broadcast(self._extract_announcement_arg(args), llm_client)

    async def handle_xbmc(self, args, llm_client):
        args = args or {}
        return await self._broadcast(self._extract_announcement_arg(args), llm_client)

    async def handle_discord(self, message, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        return await self.handle_webui(args or {}, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)


verba = BroadcastPlugin()
