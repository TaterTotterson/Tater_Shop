# verba/find_my_phone.py
import asyncio
import importlib.util
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from verba_base import ToolVerba
from helpers import redis_client
from tateros import integration_store as integration_store_module
from verba_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("find_my_phone")
logger.setLevel(logging.INFO)


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> dict:
    module = integration_store_module.integration_module("homeassistant")
    if module is not None:
        return module.load_homeassistant_config(required=required, client=client)
    if required:
        raise ValueError("Home Assistant integration is not enabled.")
    return {"base": "", "token": ""}


class FindMyPhonePlugin(ToolVerba):
    """
    Make a phone play an alert sound via Home Assistant Companion App notify service.

    Natural asks:
      - "where is my phone?"
      - "find my phone"
      - "ring my phone"
      - "locate my phone"
      - "make my phone beep"
    """

    name = "find_my_phone"
    verba_name = "Find My Phone"
    version = "1.0.11"
    min_tater_version = "59"
    when_to_use = "Use when the user asks to find, ring, locate, or make their phone beep."
    usage = '{"function":"find_my_phone","arguments":{}}'
    description = (
        "Use this when the user asks where their phone is, or asks to find, ring, "
        "locate, or make their phone play a sound."
    )
    verba_dec = "Ping or ring your phone through Home Assistant so you can locate it."
    pretty_name = "Finding Your Phone"
    settings_category = "Find My Phone"
    required_settings = {
        "MOBILE_NOTIFY_SERVICE": {
            "label": "Legacy notify service (optional fallback)",
            "type": "string",
            "default": "",
            "description": "Only used when legacy fallback is enabled. People-specific services are preferred.",
        },
        "ALLOW_LEGACY_FALLBACK": {
            "label": "Allow legacy fallback when no Person matches",
            "type": "checkbox",
            "default": False,
            "description": "Leave off to require Settings > People identity links before ringing a phone.",
        },
        "DEFAULT_TITLE": {
            "label": "Default notification title",
            "type": "string",
            "default": "Find My Phone",
        },
        "DEFAULT_MESSAGE": {
            "label": "Default notification message",
            "type": "string",
            "default": "🔔 Phone alert requested!",
        },
        "ALERT_COUNT": {
            "label": "How many times to send the alert (1-5)",
            "type": "number",
            "default": 2,
        },
        "ALERT_DELAY_SECONDS": {
            "label": "Delay between alerts (seconds)",
            "type": "number",
            "default": 0.6,
        },
        "REQUEST_TIMEOUT_SECONDS": {
            "label": "HTTP timeout (seconds)",
            "type": "number",
            "default": 15,
        },
    }
    waiting_prompt_template = (
        "Write a short, friendly message telling {mention} you're looking for their phone now. "
        "Only output that message."
    )
    platforms = ['webui', 'little_spud', 'macos', 'voice_core', 'homeassistant', 'homekit', 'xbmc', 'discord', 'telegram', 'matrix', 'irc', 'meshtastic']
    common_needs = []
    missing_info_prompts = []

    _PEOPLE_API_MODULE: Any = None
    _PEOPLE_API_UNAVAILABLE = False

    @staticmethod
    def _decode_map(raw: Optional[Dict[Any, Any]]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
            if isinstance(value, (bytes, bytearray)):
                out[k] = value.decode("utf-8", "ignore")
            else:
                out[k] = str(value or "")
        return out

    @staticmethod
    def _text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (bytes, bytearray)):
            try:
                return value.decode("utf-8", "replace").strip()
            except Exception:
                return str(value).strip()
        return str(value).strip()

    @classmethod
    def _as_bool(cls, value: Any, default: bool = False) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        token = cls._text(value).lower()
        if token in {"1", "true", "yes", "on", "enabled", "y"}:
            return True
        if token in {"0", "false", "no", "off", "disabled", "n"}:
            return False
        return bool(default)

    @classmethod
    def _coerce_int(cls, value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(float(str(value).strip()))
        except Exception:
            parsed = int(default)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    @classmethod
    def _coerce_float(cls, value: Any, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(str(value).strip())
        except Exception:
            parsed = float(default)
        if parsed < minimum:
            return float(minimum)
        if parsed > maximum:
            return float(maximum)
        return float(parsed)

    @classmethod
    def _people_api_module(cls) -> Any:
        if cls._PEOPLE_API_MODULE is not None:
            return cls._PEOPLE_API_MODULE
        if cls._PEOPLE_API_UNAVAILABLE:
            return None

        try:
            import people as people_module  # type: ignore

            cls._PEOPLE_API_MODULE = people_module
            return cls._PEOPLE_API_MODULE
        except Exception:
            pass

        try:
            candidate = Path(__file__).resolve().parents[2] / "Tater" / "people.py"
            if candidate.exists():
                spec = importlib.util.spec_from_file_location("tater_people_api", candidate)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    cls._PEOPLE_API_MODULE = module
                    return cls._PEOPLE_API_MODULE
        except Exception:
            pass

        cls._PEOPLE_API_UNAVAILABLE = True
        return None

    @classmethod
    def _people_rows(cls, redis_obj: Any = None) -> List[Dict[str, Any]]:
        module = cls._people_api_module()
        load_store = getattr(module, "load_store", None) if module is not None else None
        if not callable(load_store):
            return []
        try:
            store = load_store(redis_obj if redis_obj is not None else redis_client)
        except Exception:
            return []
        rows = [dict(row) for row in list((store or {}).get("people") or []) if isinstance(row, dict)]
        rows.sort(key=lambda row: (cls._text(row.get("display_name")).lower(), cls._text(row.get("id"))))
        return rows

    @classmethod
    def _person_setting_key(cls, person_id: Any) -> str:
        token = re.sub(r"[^A-Za-z0-9_]+", "_", cls._text(person_id)).strip("_")
        return f"PERSON_NOTIFY_SERVICE__{token}" if token else ""

    @classmethod
    def _person_label(cls, person_id: Any) -> str:
        wanted = cls._text(person_id)
        if not wanted:
            return ""
        for person in cls._people_rows():
            if cls._text(person.get("id")) == wanted:
                return cls._text(person.get("display_name"))
        return ""

    @classmethod
    def webui_settings_fields(
        cls,
        fields: List[Dict[str, Any]],
        current_settings: Optional[Dict[str, Any]] = None,
        redis_client: Any = None,
        **_kwargs,
    ) -> List[Dict[str, Any]]:
        current = current_settings if isinstance(current_settings, dict) else {}
        out = [dict(field) for field in fields if isinstance(field, dict)]
        people = cls._people_rows(redis_client)
        if people:
            for person in people:
                person_id = cls._text(person.get("id"))
                display_name = cls._text(person.get("display_name")) or person_id
                key = cls._person_setting_key(person_id)
                if not key:
                    continue
                out.append(
                    {
                        "key": key,
                        "label": f"{display_name} phone notify service",
                        "type": "string",
                        "value": cls._text(current.get(key)),
                        "default": "",
                        "description": "Example: notify.mobile_app_taters_iphone or mobile_app_taters_iphone.",
                    }
                )
        else:
            out.append(
                {
                    "key": "PEOPLE_SETUP_NOTE",
                    "label": "People setup",
                    "type": "textarea",
                    "value": "Create a master person in Settings > People before configuring per-person phones.",
                    "default": "",
                    "description": "This note is informational; it can be left as-is.",
                }
            )
        return out

    @classmethod
    def webui_prepare_settings_values(
        cls,
        values: Dict[str, Any],
        redis_client: Any = None,
        **_kwargs,
    ) -> Dict[str, Any]:
        out = dict(values or {})
        out.pop("PEOPLE_SETUP_NOTE", None)
        for person in cls._people_rows(redis_client):
            key = cls._person_setting_key(person.get("id"))
            if key in out:
                out[key] = cls._text(out.get(key))
        if "MOBILE_NOTIFY_SERVICE" in out:
            out["MOBILE_NOTIFY_SERVICE"] = cls._text(out.get("MOBILE_NOTIFY_SERVICE"))
        if "ALLOW_LEGACY_FALLBACK" in out:
            out["ALLOW_LEGACY_FALLBACK"] = "1" if cls._as_bool(out.get("ALLOW_LEGACY_FALLBACK"), False) else "0"
        return out

    def _load_settings(self) -> Dict[str, str]:
        raw = redis_client.hgetall(f"verba_settings:{self.settings_category}") or redis_client.hgetall(
            f"verba_settings: {self.settings_category}"
        )
        return self._decode_map(raw)

    def _load_homeassistant_settings(self) -> Dict[str, str]:
        ha = load_homeassistant_config(required=False)
        return {"HA_BASE_URL": ha.get("base", ""), "HA_TOKEN": ha.get("token", "")}

    def _load_default_notify_service(self) -> str:
        settings = self._decode_map(redis_client.hgetall("verba_settings:Home Assistant Notifier") or {})
        return (settings.get("DEFAULT_DEVICE_SERVICE") or "").strip()

    def _normalize_notify_service(self, raw: str) -> str:
        raw = (raw or "").strip()
        raw = raw.replace("/", ".")
        if raw.startswith("notify."):
            raw = raw.split("notify.", 1)[1].strip()
        raw = raw.strip().strip(".")
        if not raw:
            return ""
        if not re.match(r"^[A-Za-z0-9_]+$", raw):
            return ""
        return raw

    def _extract_overrides(self, args: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        args = args or {}
        return {
            "title": args.get("title") or "",
            "message": args.get("message") or args.get("content") or "",
            "count": args.get("count") if args.get("count") is not None else args.get("alert_count"),
        }

    def _origin_from_context(self, platform: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        ctx = context if isinstance(context, dict) else {}
        origin = dict(ctx.get("origin")) if isinstance(ctx.get("origin"), dict) else {}
        args_origin = ctx.get("args_origin") if isinstance(ctx.get("args_origin"), dict) else {}
        if args_origin:
            origin.update(args_origin)
        origin.setdefault("platform", platform)
        for key in (
            "person_id",
            "master_user_id",
            "person_name",
            "people_resolution",
            "speaker_id",
            "speaker_name",
            "user_id",
            "user",
            "username",
            "display_name",
        ):
            if key in ctx and ctx.get(key) is not None:
                origin.setdefault(key, ctx.get(key))
        return origin

    def _origin_from_discord(self, message: Any, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        origin = self._origin_from_context("discord", context)
        author = getattr(message, "author", None)
        author_id = self._text(getattr(author, "id", ""))
        name = self._text(getattr(author, "display_name", "") or getattr(author, "global_name", "") or getattr(author, "name", ""))
        if author_id:
            origin.setdefault("user_id", author_id)
            origin.setdefault("author_id", author_id)
        if name:
            origin.setdefault("display_name", name)
            origin.setdefault("username", name)
            origin.setdefault("user", name)
        return origin

    def _origin_from_telegram(self, update: Any, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        origin = self._origin_from_context("telegram", context)
        sender = (update or {}).get("from") if isinstance(update, dict) else {}
        sender = sender if isinstance(sender, dict) else {}
        user_id = self._text(sender.get("id") or sender.get("user_id"))
        username = self._text(sender.get("username") or sender.get("first_name") or sender.get("last_name"))
        if user_id:
            origin.setdefault("user_id", user_id)
            origin.setdefault("from_id", user_id)
        if username:
            origin.setdefault("username", username)
            origin.setdefault("display_name", username)
            origin.setdefault("user", username)
        return origin

    def _origin_from_matrix(self, sender: Any, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        origin = self._origin_from_context("matrix", context)
        user_id = self._text(sender)
        if user_id:
            origin.setdefault("user_id", user_id)
            origin.setdefault("sender", user_id)
            origin.setdefault("user", user_id)
            origin.setdefault("display_name", user_id)
        return origin

    def _origin_from_irc(self, user: Any, context: Optional[Dict[str, Any]] = None, *, platform: str = "irc") -> Dict[str, Any]:
        origin = self._origin_from_context(platform, context)
        user_name = self._text(user)
        if user_name:
            origin.setdefault("user", user_name)
            origin.setdefault("username", user_name)
            origin.setdefault("display_name", user_name)
        return origin

    def _resolve_person_for_request(self, platform: str, origin: Dict[str, Any]) -> Dict[str, str]:
        source = origin if isinstance(origin, dict) else {}
        module = self._people_api_module()
        resolver = getattr(module, "resolve_person", None) if module is not None else None
        if callable(resolver):
            try:
                resolved = resolver(platform=platform, origin=source, redis_client=redis_client)
            except Exception:
                resolved = {}
            if bool((resolved or {}).get("matched")):
                person_id = self._text((resolved or {}).get("person_id") or (resolved or {}).get("master_user_id"))
                return {
                    "person_id": person_id,
                    "person_name": self._text((resolved or {}).get("display_name")) or self._person_label(person_id),
                    "match_type": self._text((resolved or {}).get("match_type")),
                }

        return {"person_id": "", "person_name": "", "match_type": ""}

    def _notify_service_for_person(self, settings: Dict[str, str], person_id: str) -> str:
        key = self._person_setting_key(person_id)
        if key:
            configured = self._text(settings.get(key))
            if configured:
                return configured

        raw_json = self._text(settings.get("PEOPLE_NOTIFY_SERVICES_JSON") or settings.get("PERSON_NOTIFY_SERVICES_JSON"))
        if raw_json:
            try:
                parsed = json.loads(raw_json)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                for candidate in (person_id, self._person_label(person_id)):
                    value = self._text(parsed.get(candidate))
                    if value:
                        return value
        return ""

    def _ha_post_service(
        self,
        base_url: str,
        token: str,
        domain: str,
        service: str,
        payload: Dict[str, Any],
        timeout_seconds: float,
    ) -> tuple[int, str]:
        base_url = (base_url or "").rstrip("/")
        url = f"{base_url}/api/services/{domain}/{service}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_seconds)
        return resp.status_code, resp.text

    def _trigger_phone_alert(
        self,
        overrides: Optional[Dict[str, Any]] = None,
        *,
        platform: str = "webui",
        origin: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """
        Returns a dict:
          - ok: bool
          - count: int (how many alerts we attempted)
          - error: str
        """
        overrides = overrides or {}
        settings = self._load_settings()
        person = self._resolve_person_for_request(platform, origin or {})
        person_id = self._text(person.get("person_id"))
        person_name = self._text(person.get("person_name"))
        legacy_fallback = self._as_bool(settings.get("ALLOW_LEGACY_FALLBACK"), False)

        if person_id:
            notify_service_raw = self._notify_service_for_person(settings, person_id)
        elif legacy_fallback:
            notify_service_raw = (settings.get("MOBILE_NOTIFY_SERVICE") or "").strip() or self._load_default_notify_service()
        else:
            return {
                "ok": False,
                "count": 0,
                "code": "person_phone_missing",
                "error": "I do not have a phone set up for you yet. Link this user to a Person in Settings > People, then set that person's phone notify service in Find My Phone settings.",
            }

        if person_id and not notify_service_raw:
            owner = person_name or "this person"
            return {
                "ok": False,
                "count": 0,
                "code": "person_phone_missing",
                "person_id": person_id,
                "person_name": person_name,
                "error": f"No phone is set up for {owner}. Add a notify service for {owner} in Find My Phone settings.",
            }

        ha_settings = self._load_homeassistant_settings()
        ha_base = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        ha_token = (ha_settings.get("HA_TOKEN") or "").strip()

        title = ((overrides.get("title") or "").strip() or (settings.get("DEFAULT_TITLE", "") or "Find My Phone")).strip()
        message = ((overrides.get("message") or "").strip() or (settings.get("DEFAULT_MESSAGE", "") or "Phone alert requested.")).strip()

        if not ha_token:
            return {
                "ok": False,
                "count": 0,
                "code": "ha_token_missing",
                "error": (
                    "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                    "and add a Long-Lived Access Token."
                ),
            }

        if not notify_service_raw:
            return {
                "ok": False,
                "count": 0,
                "code": "notify_service_missing",
                "error": "Find My Phone is not configured. Please set a notify service in plugin settings.",
            }

        service = self._normalize_notify_service(notify_service_raw)
        if not service:
            return {
                "ok": False,
                "count": 0,
                "code": "notify_service_invalid",
                "error": (
                    "The notify service is invalid. Use notify.mobile_app_<device> or mobile_app_<device> "
                    "(letters/numbers/underscore)."
                ),
            }

        # Parse count
        raw_count = overrides.get("count") if overrides.get("count") is not None else settings.get("ALERT_COUNT", 2)
        count = self._coerce_int(raw_count, default=2, minimum=1, maximum=5)
        delay_seconds = self._coerce_float(settings.get("ALERT_DELAY_SECONDS", 0.6), default=0.6, minimum=0.0, maximum=10.0)
        timeout_seconds = self._coerce_float(settings.get("REQUEST_TIMEOUT_SECONDS", 15), default=15.0, minimum=3.0, maximum=120.0)

        # Companion app notify payload
        payload = {
            "title": title,
            "message": message,
            "data": {
                # common “be loud / immediate” knobs
                "ttl": 0,
                "priority": "high",

                # Android hint (best-effort; device/OS varies)
                "channel": "alarm_stream",

                # iOS hint (best-effort; critical alerts require proper setup/permissions)
                "push": {
                    "sound": {
                        "name": "default",
                        "critical": 1,
                        "volume": 1.0
                    }
                },

                # best-effort vibration hints for some clients
                "vibrationPattern": [100, 1000, 100, 1000, 100],
            },
        }

        last_status, last_text = 0, ""
        for i in range(count):
            last_status, last_text = self._ha_post_service(
                base_url=ha_base,
                token=ha_token,
                domain="notify",
                service=service,
                payload=payload,
                timeout_seconds=timeout_seconds,
            )

            if last_status not in (200, 201):
                logger.error(f"[find_my_phone] HA notify failed HTTP {last_status}: {last_text}")
                return {
                    "ok": False,
                    "count": i + 1,
                    "code": "phone_alert_failed",
                    "error": f"Home Assistant notify failed (HTTP {last_status}). Check HA URL/token/service name.",
                }

            # Small pause so multiple alerts are less likely to collapse into one
            if i < count - 1:
                time.sleep(delay_seconds)

        return {"ok": True, "count": count, "error": "", "person_id": person_id, "person_name": person_name}

    def _siri_flatten(self, text: Optional[str]) -> str:
        if not text:
            return "No answer available."
        out = str(text)
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out[:280]

    async def _run(
        self,
        args: Optional[Dict[str, Any]],
        llm_client,
        mention: str = "you",
        *,
        platform: str = "webui",
        origin: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        overrides = self._extract_overrides(args)
        result = await asyncio.to_thread(self._trigger_phone_alert, overrides, platform=platform, origin=origin or {})

        if not result.get("ok"):
            err_msg = (result.get("error") or "Something went wrong triggering the phone alert.").strip()
            code = (result.get("code") or "phone_alert_failed").strip()
            return action_failure(
                code=code,
                message=err_msg,
                say_hint="Tell the user they do not have a phone set up yet, or explain the setting that needs attention.",
            )

        count = int(result.get("count", 1) or 1)
        person_name = self._text(result.get("person_name"))
        return action_success(
            facts={"alert_count": count, "mention": mention, "person_name": person_name},
            summary_for_user=f"Sent the phone alert {count} time(s).",
            say_hint="Confirm that the alert was sent and suggest listening for the phone.",
        )

    # ---- platform handlers ----
    async def handle_webui(self, args, llm_client, context=None):
        return await self._run(args or {}, llm_client, mention="you", platform="webui", origin=self._origin_from_context("webui", context))

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self.handle_webui(args or {}, llm_client, context=context)


    async def handle_macos(self, args, llm_client, context=None):
        return await self._run(args or {}, llm_client, mention="you", platform="macos", origin=self._origin_from_context("macos", context))

    async def handle_homeassistant(self, args, llm_client, context=None):
        return await self._run(args or {}, llm_client, mention="you", platform="homeassistant", origin=self._origin_from_context("homeassistant", context))

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._run(args or {}, llm_client, mention="you", platform="voice_core", origin=self._origin_from_context("voice_core", context))


    async def handle_homekit(self, args, llm_client, context=None):
        return await self._run(args or {}, llm_client, mention="you", platform="homekit", origin=self._origin_from_context("homekit", context))

    async def handle_xbmc(self, args, llm_client, context=None):
        return await self._run(args or {}, llm_client, mention="you", platform="xbmc", origin=self._origin_from_context("xbmc", context))

    async def handle_discord(self, message, args, llm_client, context=None):
        mention = getattr(getattr(message, "author", None), "mention", None) or "you"
        return await self._run(args or {}, llm_client, mention=mention, platform="discord", origin=self._origin_from_discord(message, context))

    async def handle_telegram(self, update, args, llm_client, context=None):
        sender = (update or {}).get("from") or {}
        username = (sender.get("username") or sender.get("first_name") or "you").strip()
        mention = f"@{username}" if username and username != "you" and not username.startswith("@") else (username or "you")
        return await self._run(args or {}, llm_client, mention=mention, platform="telegram", origin=self._origin_from_telegram(update, context))

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, context=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        mention = str(sender or "you").strip() or "you"
        return await self._run(args or {}, llm_client, mention=mention, platform="matrix", origin=self._origin_from_matrix(sender, context))


    async def handle_meshtastic(self, args=None, llm_client=None, context=None, **kwargs):
        args = args or {}
        ctx = context if isinstance(context, dict) else {}
        ctx["platform"] = "meshtastic"
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

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client, context=None):
        mention = str(user or "you").strip() or "you"
        ctx = context if isinstance(context, dict) else {}
        platform = "meshtastic" if str(ctx.get("platform") or "").strip().lower() == "meshtastic" else "irc"
        return await self._run(args or {}, llm_client, mention=mention, platform=platform, origin=self._origin_from_irc(user, ctx, platform=platform))


verba = FindMyPhonePlugin()
