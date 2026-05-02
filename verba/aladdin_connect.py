import base64
import hashlib
import hmac
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests

from helpers import redis_client
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("aladdin_connect")
logger.setLevel(logging.INFO)

SETTINGS_CATEGORY = "Aladdin Connect"
SHARED_SETTINGS_KEY = "aladdin_connect_settings"
DEFAULT_API_BASE_URL = "https://api.smartgarage.systems"
DEFAULT_COGNITO_REGION = "us-east-2"
DEFAULT_CLIENT_ID = "27iic8c3bvslqngl3hso83t74b"
DEFAULT_CLIENT_SECRET = "7bokto0ep96055k42fnrmuth84k7jdcjablestb7j53o8lp63v5"
DEFAULT_APP_VERSION = "6.21"
DEFAULT_TIMEOUT_SECONDS = 30

SETTINGS_KEYS = (
    SHARED_SETTINGS_KEY,
    f"verba_settings:{SETTINGS_CATEGORY}",
    f"verba_settings: {SETTINGS_CATEGORY}",
)


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


def _decode_map(raw: Optional[dict]) -> dict:
    out: dict = {}
    for key, value in (raw or {}).items():
        k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
        if isinstance(value, (bytes, bytearray)):
            out[k] = value.decode("utf-8", "ignore")
        elif value is None:
            out[k] = ""
        else:
            out[k] = str(value)
    return out


def _read_settings() -> dict:
    merged: dict = {}
    for key in SETTINGS_KEYS:
        try:
            decoded = _decode_map(redis_client.hgetall(key) or {})
        except Exception:
            continue
        for field, value in decoded.items():
            if _coerce_text(value) or field not in merged:
                merged[field] = value
    return merged


def _setting(settings: dict, key: str, default: str = "") -> str:
    aliases = {
        "ALADDIN_EMAIL": ("ALADDIN_EMAIL", "EMAIL", "USERNAME", "USER_EMAIL"),
        "ALADDIN_PASSWORD": ("ALADDIN_PASSWORD", "PASSWORD"),
        "ALADDIN_API_BASE_URL": ("ALADDIN_API_BASE_URL", "API_BASE_URL", "BASE_URL"),
        "ALADDIN_COGNITO_REGION": ("ALADDIN_COGNITO_REGION", "COGNITO_REGION"),
        "ALADDIN_CLIENT_ID": ("ALADDIN_CLIENT_ID", "CLIENT_ID"),
        "ALADDIN_CLIENT_SECRET": ("ALADDIN_CLIENT_SECRET", "CLIENT_SECRET"),
        "ALADDIN_APP_VERSION": ("ALADDIN_APP_VERSION", "APP_VERSION"),
        "ALADDIN_TIMEOUT_SECONDS": ("ALADDIN_TIMEOUT_SECONDS", "TIMEOUT_SECONDS"),
    }.get(key, (key,))
    for name in aliases:
        value = _coerce_text(settings.get(name))
        if value:
            return value
    return default


def _normalize_api_base(raw_url: Any) -> str:
    text = _coerce_text(raw_url) or DEFAULT_API_BASE_URL
    if "://" not in text:
        text = f"https://{text}"
    parsed = urlparse(text)
    if not parsed.netloc and parsed.path:
        parsed = urlparse(f"https://{text}")
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or parsed.path
    root = urlunparse((scheme, netloc, "", "", "", "")).rstrip("/")
    return root or DEFAULT_API_BASE_URL


def _as_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        number = int(float(str(value).strip()))
    except Exception:
        number = int(default)
    return max(int(minimum), min(int(maximum), number))


class AladdinClient:
    DOOR_STATUS = {
        0: "unknown",
        1: "open",
        2: "opening",
        3: "unknown",
        4: "closed",
        5: "closing",
        6: "unknown",
        7: "unknown",
    }
    DOOR_LINK_STATUS = {
        0: "Unknown",
        1: "NotConfigured",
        2: "Paired",
        3: "Connected",
    }

    def __init__(self):
        settings = _read_settings()
        self.email = _setting(settings, "ALADDIN_EMAIL", "")
        self.password = _setting(settings, "ALADDIN_PASSWORD", "")
        self.api_base_url = _normalize_api_base(_setting(settings, "ALADDIN_API_BASE_URL", DEFAULT_API_BASE_URL))
        self.cognito_region = _setting(settings, "ALADDIN_COGNITO_REGION", DEFAULT_COGNITO_REGION)
        self.client_id = _setting(settings, "ALADDIN_CLIENT_ID", DEFAULT_CLIENT_ID)
        self.client_secret = _setting(settings, "ALADDIN_CLIENT_SECRET", DEFAULT_CLIENT_SECRET)
        self.app_version = _setting(settings, "ALADDIN_APP_VERSION", DEFAULT_APP_VERSION)
        self.timeout = _as_int(
            _setting(settings, "ALADDIN_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS)),
            DEFAULT_TIMEOUT_SECONDS,
            minimum=5,
            maximum=120,
        )
        self.session = requests.Session()
        self.access_token = ""
        if not self.email or not self.password:
            raise ValueError("Aladdin Connect email and password are missing in Tater Settings > Integrations.")

    def _secret_hash(self) -> str:
        message = f"{self.email}{self.client_id}".encode("utf-8")
        digest = hmac.new(self.client_secret.encode("utf-8"), msg=message, digestmod=hashlib.sha256).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _headers(self, *, json_body: bool = False) -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": "okhttp/4.10.0",
            "app_version": self.app_version,
            "Content-Type": "application/json" if json_body else "application/x-www-form-urlencoded",
        }
        return headers

    def login(self) -> bool:
        url = f"https://cognito-idp.{self.cognito_region}.amazonaws.com/"
        payload = {
            "AuthFlow": "USER_PASSWORD_AUTH",
            "AuthParameters": {
                "SECRET_HASH": self._secret_hash(),
                "USERNAME": self.email,
                "PASSWORD": self.password,
            },
            "ClientId": self.client_id,
        }
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
        }
        response = self.session.post(url, headers=headers, json=payload, timeout=self.timeout)
        if response.status_code >= 400:
            raise RuntimeError(self._error_message(response, "Aladdin Connect login failed"))
        data = response.json()
        auth = data.get("AuthenticationResult") if isinstance(data, dict) else {}
        token = _coerce_text((auth or {}).get("AccessToken"))
        if not token:
            raise RuntimeError("Aladdin Connect login failed: no access token returned.")
        self.access_token = token
        return True

    def _error_message(self, response: requests.Response, prefix: str) -> str:
        detail = _coerce_text(response.text)
        try:
            payload = response.json()
            if isinstance(payload, dict):
                detail = _coerce_text(payload.get("message") or payload.get("error") or payload.get("__type") or detail)
        except Exception:
            pass
        return f"{prefix}: HTTP {response.status_code}{(': ' + detail) if detail else ''}"

    def _request(self, method: str, path: str, *, json_body: Optional[dict] = None) -> Any:
        if not self.access_token:
            self.login()
        url = f"{self.api_base_url}{path}"
        response = self.session.request(
            method,
            url,
            headers=self._headers(json_body=json_body is not None),
            json=json_body,
            timeout=self.timeout,
        )
        if response.status_code in {401, 403}:
            self.login()
            response = self.session.request(
                method,
                url,
                headers=self._headers(json_body=json_body is not None),
                json=json_body,
                timeout=self.timeout,
            )
        if response.status_code >= 400:
            raise RuntimeError(self._error_message(response, f"Aladdin Connect API call failed for {path}"))
        if not _coerce_text(response.text):
            return None
        try:
            return response.json()
        except Exception:
            return response.text

    def list_doors(self) -> List[dict]:
        data = self._request("GET", "/devices")
        rows: List[dict] = []
        for device in (data or {}).get("devices", []) if isinstance(data, dict) else []:
            if not isinstance(device, dict):
                continue
            device_id = device.get("id")
            serial = _coerce_text(device.get("serial_number") or device.get("serial"))
            for door in device.get("doors", []) if isinstance(device.get("doors"), list) else []:
                if not isinstance(door, dict):
                    continue
                status_code = door.get("status", 0)
                link_code = door.get("link_status", 0)
                rows.append(
                    {
                        "device_id": device_id,
                        "door_number": door.get("door_index"),
                        "name": _coerce_text(door.get("name")) or f"Garage Door {door.get('door_index')}",
                        "status": self.DOOR_STATUS.get(status_code, "unknown"),
                        "status_code": status_code,
                        "link_status": self.DOOR_LINK_STATUS.get(link_code, "Unknown"),
                        "link_status_code": link_code,
                        "battery_level": door.get("battery_level", 0),
                        "rssi": device.get("rssi", 0),
                        "serial": serial,
                        "vendor": _coerce_text(device.get("vendor")),
                        "model": _coerce_text(device.get("model")),
                        "ble_strength": door.get("ble_strength", 0),
                    }
                )
        return rows

    def command_door(self, device_id: Any, door_number: Any, action: str) -> bool:
        normalized = _coerce_text(action).lower()
        if normalized not in {"open", "close"}:
            raise ValueError(f"Unsupported Aladdin Connect door action: {action}")
        command = "OPEN_DOOR" if normalized == "open" else "CLOSE_DOOR"
        try:
            self._request("POST", f"/command/devices/{device_id}/doors/{door_number}", json_body={"command": command})
        except RuntimeError as exc:
            text = str(exc).lower()
            if f"already {normalized}" in text:
                return True
            raise
        return True


class AladdinConnectPlugin(ToolVerba):
    name = "aladdin_connect"
    verba_name = "Aladdin Connect"
    version = "1.0.0"
    min_tater_version = "59"
    pretty_name = "Aladdin Connect"
    settings_category = "Aladdin Connect"
    platforms = ["voice_core", "homeassistant", "webui", "macos", "xbmc", "homekit", "discord", "telegram", "matrix", "irc", "meshtastic"]
    tags = ["aladdin", "genie", "garage", "garage-door"]
    routing_keywords = ["aladdin", "aladdin connect", "genie", "garage door", "garage"]
    forced_route = "garage_door"
    forced_domain_hint = "aladdin connect garage door"

    usage = '{"function":"aladdin_connect","arguments":{"query":"is the garage door closed?"}}'
    description = "Control Aladdin Connect garage doors and read open/closed status."
    verba_dec = "Control Aladdin Connect garage doors."
    when_to_use = "Use for Aladdin Connect or Genie garage door status checks, open commands, and close commands."
    how_to_use = "Pass a natural-language garage door request with an action and optional door name."
    common_needs = ["Garage door action: open, close, or status."]
    missing_info_prompts = ["Which garage door should I control?"]
    example_calls = [
        '{"function":"aladdin_connect","arguments":{"query":"is the garage door closed?"}}',
        '{"function":"aladdin_connect","arguments":{"query":"open the garage door"}}',
        '{"function":"aladdin_connect","arguments":{"query":"close the left garage door"}}',
    ]

    required_settings = {}

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are checking or controlling the garage door now. "
        "Only output that message."
    )

    @staticmethod
    def _coerce_text(value: Any) -> str:
        return _coerce_text(value)

    def _normalize_handler_args(self, args: Any) -> Dict[str, Any]:
        if isinstance(args, dict):
            payload = dict(args)
            nested = payload.get("arguments")
            if isinstance(nested, dict):
                merged = dict(nested)
                for key, value in payload.items():
                    if key != "arguments":
                        merged[key] = value
                return merged
            return payload
        if isinstance(args, str):
            text = self._coerce_text(args)
            if not text:
                return {}
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    nested = parsed.get("arguments")
                    if isinstance(nested, dict):
                        merged = dict(nested)
                        for key, value in parsed.items():
                            if key != "arguments":
                                merged[key] = value
                        return merged
                    return dict(parsed)
            except Exception:
                pass
            return {"query": text}
        return {}

    def _diagnosis(self) -> dict:
        settings = _read_settings()
        email = _setting(settings, "ALADDIN_EMAIL", "")
        password = _setting(settings, "ALADDIN_PASSWORD", "")
        api_base = _normalize_api_base(_setting(settings, "ALADDIN_API_BASE_URL", DEFAULT_API_BASE_URL))
        parsed = urlparse(api_base)
        return {
            "aladdin_email": "set" if "@" in email else "missing",
            "aladdin_password": "set" if len(password) >= 1 else "missing",
            "aladdin_api_base_url": "set" if parsed.scheme in {"http", "https"} and bool(parsed.netloc) else "invalid",
        }

    def _get_client(self) -> Optional[AladdinClient]:
        try:
            return AladdinClient()
        except Exception as exc:
            logger.error("[%s] Failed to initialize Aladdin client: %s", self.name, exc)
            return None

    def _normalize_action(self, value: Any, query: str) -> str:
        explicit = self._coerce_text(value).lower()
        if explicit in {"status", "state", "get_state", "check"}:
            return "status"
        if explicit in {"open", "open_door"}:
            return "open"
        if explicit in {"close", "closed", "shut", "shut_door"}:
            return "close"

        text = f" {self._coerce_text(query).lower()} "
        status_markers = (
            " status " in text
            or " state " in text
            or " check " in text
            or " is " in text
            or " are " in text
            or " what " in text
            or " whether " in text
        )
        command_open = bool(re.search(r"\b(open|raise|lift)\b", text)) and not bool(re.search(r"\b(is|are|status|state|check|whether)\b.*\bopen\b", text))
        command_close = bool(re.search(r"\b(close|shut|lower)\b", text)) and not bool(re.search(r"\b(is|are|status|state|check|whether)\b.*\bclosed?\b", text))
        if command_close:
            return "close"
        if command_open:
            return "open"
        if status_markers or " open or closed " in text or " opened or closed " in text:
            return "status"
        return ""

    def _target_text(self, args: dict, query: str) -> str:
        target = self._coerce_text(args.get("target") or args.get("door") or args.get("door_name"))
        if target:
            return target
        text = self._coerce_text(query).lower()
        text = re.sub(r"\b(aladdin|connect|genie|garage|door|doors|please|can you|would you|the|my|a|an)\b", " ", text)
        text = re.sub(r"\b(open|close|closed|shut|lower|raise|lift|status|state|check|is|are|what|whether|currently|right now)\b", " ", text)
        return " ".join(re.findall(r"[a-z0-9]+", text))

    def _door_key(self, door: dict) -> Tuple[str, str]:
        return (str(door.get("device_id")), str(door.get("door_number")))

    def _find_door(self, doors: List[dict], args: dict, query: str) -> Tuple[Optional[dict], List[str]]:
        device_id = self._coerce_text(args.get("device_id"))
        door_number = self._coerce_text(args.get("door_number"))
        if device_id and door_number:
            for door in doors:
                if self._coerce_text(door.get("device_id")) == device_id and self._coerce_text(door.get("door_number")) == door_number:
                    return door, []
            return None, [f"No Aladdin Connect door matched device_id={device_id}, door_number={door_number}."]

        if not doors:
            return None, ["No Aladdin Connect garage doors were returned for this account."]
        if len(doors) == 1:
            return doors[0], []

        target = self._target_text(args, query).lower()
        if not target:
            choices = [f"{door.get('name')} ({door.get('status')})" for door in doors]
            return None, ["Which garage door? Available doors: " + ", ".join(choices)]

        target_tokens = {token for token in re.split(r"[^a-z0-9]+", target) if token}
        scored: List[Tuple[int, dict]] = []
        for door in doors:
            name = self._coerce_text(door.get("name")).lower()
            if target and target in name:
                scored.append((100 + len(target), door))
                continue
            name_tokens = {token for token in re.split(r"[^a-z0-9]+", name) if token}
            score = len(target_tokens & name_tokens)
            if score:
                scored.append((score, door))
        if not scored:
            choices = [f"{door.get('name')} ({door.get('status')})" for door in doors]
            return None, [f"I could not match '{target}' to an Aladdin Connect door. Available doors: " + ", ".join(choices)]
        scored.sort(key=lambda item: item[0], reverse=True)
        if len(scored) > 1 and scored[0][0] == scored[1][0]:
            tied = [door.get("name") for score, door in scored if score == scored[0][0]]
            return None, ["That matched multiple garage doors: " + ", ".join(self._coerce_text(name) for name in tied if name)]
        return scored[0][1], []

    def _door_summary(self, door: dict) -> str:
        name = self._coerce_text(door.get("name")) or "Garage door"
        status = self._coerce_text(door.get("status")) or "unknown"
        link = self._coerce_text(door.get("link_status"))
        if link and link != "Connected":
            return f"{name} is {status}; link status {link}."
        return f"{name} is {status}."

    async def _handle(self, args, llm_client=None):
        payload = self._normalize_handler_args(args)
        query = self._coerce_text(payload.get("query") or payload.get("text") or payload.get("prompt"))
        action = self._normalize_action(payload.get("action"), query)
        if not action:
            return action_failure(
                code="missing_action",
                message="Please ask whether the garage door is open/closed, or ask to open/close it.",
                needs=["Use open, close, or status for the Aladdin Connect garage door."],
                say_hint="Ask which garage door action the user wants.",
            )

        client = self._get_client()
        if not client:
            return action_failure(
                code="aladdin_not_configured",
                message="Aladdin Connect is not configured.",
                diagnosis=self._diagnosis(),
                needs=[
                    "Set Aladdin Connect email and password in Tater Settings > Integrations > Aladdin Connect.",
                ],
                say_hint="Explain Aladdin Connect settings are missing and ask to configure them.",
            )

        try:
            doors = client.list_doors()
        except Exception as exc:
            return action_failure(
                code="aladdin_door_lookup_failed",
                message=f"Could not read Aladdin Connect garage doors: {exc}",
                diagnosis=self._diagnosis(),
                say_hint="Explain Aladdin Connect door lookup failed and suggest checking credentials.",
            )

        door, needs = self._find_door(doors, payload, query)
        if not door:
            return action_failure(
                code="door_selection_failed",
                message="Could not select an Aladdin Connect garage door.",
                needs=needs,
                say_hint="Ask which garage door to use.",
            )

        if action == "status":
            return action_success(
                facts={
                    "action": "status",
                    "device_id": door.get("device_id"),
                    "door_number": door.get("door_number"),
                    "door_name": door.get("name"),
                    "status": door.get("status"),
                },
                data={"door": door, "doors": doors},
                summary_for_user=self._door_summary(door),
                say_hint="Report the garage door status briefly.",
            )

        desired = "open" if action == "open" else "closed"
        if self._coerce_text(door.get("status")) == desired:
            return action_success(
                facts={
                    "action": action,
                    "device_id": door.get("device_id"),
                    "door_number": door.get("door_number"),
                    "door_name": door.get("name"),
                    "status": door.get("status"),
                    "already_target_state": True,
                },
                data={"door": door},
                summary_for_user=f"{self._coerce_text(door.get('name')) or 'Garage door'} is already {desired}.",
                say_hint="Tell the user the garage door is already in the requested state.",
            )

        try:
            client.command_door(door.get("device_id"), door.get("door_number"), action)
            updated_doors = client.list_doors()
            updated, _needs = self._find_door(
                updated_doors,
                {"device_id": door.get("device_id"), "door_number": door.get("door_number")},
                query,
            )
            if isinstance(updated, dict):
                door = updated
        except Exception as exc:
            return action_failure(
                code="aladdin_command_failed",
                message=f"Aladdin Connect failed to {action} {door.get('name')}: {exc}",
                diagnosis=self._diagnosis(),
                needs=["Check the Aladdin Connect app and try again."],
                say_hint="Explain the garage door command failed and suggest checking the Aladdin Connect app.",
            )

        name = self._coerce_text(door.get("name")) or "Garage door"
        status = self._coerce_text(door.get("status")) or ("opening" if action == "open" else "closing")
        return action_success(
            facts={
                "action": action,
                "device_id": door.get("device_id"),
                "door_number": door.get("door_number"),
                "door_name": door.get("name"),
                "status": status,
            },
            data={"door": door},
            summary_for_user=f"{name} is {status}.",
            say_hint="Reply briefly with the garage door command result.",
        )

    async def handle_homeassistant(self, args=None, llm_client=None, *unused_args, **unused_kwargs):
        payload = self._normalize_handler_args(args)
        if not self._coerce_text(payload.get("query")):
            payload_query = self._coerce_text(unused_kwargs.get("query"))
            if payload_query:
                payload["query"] = payload_query
        return await self._handle(payload, llm_client)

    async def handle_voice_core(self, args=None, llm_client=None, *unused_args, **unused_kwargs):
        payload = self._normalize_handler_args(args)
        if not self._coerce_text(payload.get("query")):
            payload_query = self._coerce_text(unused_kwargs.get("query"))
            if payload_query:
                payload["query"] = payload_query
        return await self._handle(payload, llm_client)

    async def handle_webui(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_macos(self, args, llm_client, context=None):
        return await self._handle(args, llm_client)

    async def handle_xbmc(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_discord(self, message, args, llm_client):
        payload = dict(args or {})
        if not self._coerce_text(payload.get("query")):
            content = getattr(message, "content", None)
            if isinstance(content, str) and content.strip():
                payload["query"] = content.strip()
        return await self._handle(payload, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        payload = dict(args or {})
        if not self._coerce_text(payload.get("query")):
            try:
                if isinstance(update, dict):
                    msg = update.get("message") or {}
                    text = msg.get("text") or msg.get("caption") or ""
                    if isinstance(text, str) and text.strip():
                        payload["query"] = text.strip()
            except Exception:
                pass
        return await self._handle(payload, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        payload = dict(args or {})
        if not self._coerce_text(payload.get("query")) and isinstance(body, str) and body.strip():
            payload["query"] = body.strip()
        return await self._handle(payload, llm_client)

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
        return await self.handle_irc(None, channel, user, raw_text, args, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        payload = dict(args or {})
        if not self._coerce_text(payload.get("query")) and isinstance(raw_message, str) and raw_message.strip():
            payload["query"] = raw_message.strip()
        return await self._handle(payload, llm_client)


verba = AladdinConnectPlugin()
