import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from tateros import integration_store as integration_store_module
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("aladdin_connect")
logger.setLevel(logging.INFO)

ALADDIN_DEFAULT_API_BASE_URL = "https://api.smartgarage.systems"
SETTINGS_CATEGORY = "Aladdin Connect"


def _aladdin_module(*, required: bool = True):
    module = integration_store_module.integration_module("aladdin")
    if module is None and required:
        raise RuntimeError("Aladdin Connect integration is not enabled.")
    return module


def normalize_aladdin_api_base(*args, **kwargs) -> str:
    module = _aladdin_module(required=False)
    if module is not None:
        return module.normalize_aladdin_api_base(*args, **kwargs)
    value = args[0] if args else kwargs.get("value")
    text = _coerce_text(value) or ALADDIN_DEFAULT_API_BASE_URL
    if "://" not in text:
        text = f"https://{text}"
    parsed = urlparse(text)
    netloc = parsed.netloc or parsed.path
    if not netloc:
        return ALADDIN_DEFAULT_API_BASE_URL
    return f"{parsed.scheme or 'https'}://{netloc}".rstrip("/")


def read_aladdin_settings(*args, **kwargs) -> Dict[str, str]:
    module = _aladdin_module(required=False)
    if module is not None:
        return module.read_aladdin_settings(*args, **kwargs)
    return {
        "ALADDIN_USERNAME": "",
        "ALADDIN_PASSWORD": "",
        "ALADDIN_API_BASE_URL": ALADDIN_DEFAULT_API_BASE_URL,
        "ALADDIN_TIMEOUT_SECONDS": "5",
    }


def AladdinConnectClient(*args, **kwargs):
    return _aladdin_module().AladdinConnectClient(*args, **kwargs)


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


class AladdinConnectPlugin(ToolVerba):
    name = "aladdin_connect"
    verba_name = "Aladdin Connect"
    version = "1.0.5"
    min_tater_version = "59"
    pretty_name = "Aladdin Connect"
    settings_category = "Aladdin Connect"
    platforms = [
        "voice_core",
        "homeassistant",
        "webui", "little_spud",
        "macos",
        "xbmc",
        "homekit",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    tags = ["aladdin", "genie", "garage", "garage-door"]
    routing_keywords = ["aladdin", "aladdin connect", "genie", "garage door", "garage"]
    forced_route = "garage_door"
    forced_domain_hint = "aladdin connect garage door"

    usage = '{"function":"aladdin_connect","arguments":{"query":"is the garage door closed?"}}'
    description = "Control Aladdin Connect garage doors directly with the Aladdin Connect cloud API."
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
        settings = read_aladdin_settings()
        username = _coerce_text(settings.get("ALADDIN_USERNAME"))
        password = _coerce_text(settings.get("ALADDIN_PASSWORD"))
        api_base = normalize_aladdin_api_base(settings.get("ALADDIN_API_BASE_URL") or ALADDIN_DEFAULT_API_BASE_URL)
        parsed = urlparse(api_base)
        return {
            "aladdin_username": "set" if username else "missing",
            "aladdin_password": "set" if password else "missing",
            "aladdin_api_base_url": "set" if parsed.scheme in {"http", "https"} and bool(parsed.netloc) else "invalid",
        }

    def _get_client(self) -> Optional[Any]:
        try:
            return AladdinConnectClient()
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
        command_open = bool(re.search(r"\b(open|raise|lift)\b", text)) and not bool(
            re.search(r"\b(is|are|status|state|check|whether)\b.*\bopen\b", text)
        )
        command_close = bool(re.search(r"\b(close|shut|lower)\b", text)) and not bool(
            re.search(r"\b(is|are|status|state|check|whether)\b.*\bclosed?\b", text)
        )
        if command_close:
            return "close"
        if command_open:
            return "open"
        if status_markers or " open or closed " in text or " opened or closed " in text:
            return "status"
        return ""

    @staticmethod
    def _json_object_from_text(text: str) -> Dict[str, Any]:
        clean = str(text or "").strip()
        clean = re.sub(r"^```(?:json)?\s*", "", clean, flags=re.I)
        clean = re.sub(r"\s*```$", "", clean).strip()
        try:
            parsed = json.loads(clean)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass
        match = re.search(r"\{.*\}", clean, flags=re.S)
        if not match:
            return {}
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    async def _ai_pick_action(self, llm_client, query: str) -> Dict[str, str]:
        if not llm_client:
            return {}
        text = self._coerce_text(query)
        if not text:
            return {}
        prompt = (
            "Choose the best Aladdin Connect garage door action for the user request.\n"
            "Allowed actions: open, close, status.\n"
            "Rules:\n"
            "1) Use status for questions asking whether a door is open, closed, or what its state is.\n"
            "2) Use open for commands to open, raise, or lift a garage door.\n"
            "3) Use close for commands to close, shut, or lower a garage door.\n"
            "4) Include target when the user names a specific door such as left, right, main, shop, or garage name.\n"
            "Respond with JSON: {\"action\":\"<open|close|status or empty string>\",\"target\":\"<door target or empty string>\"}\n\n"
            f'User request: "{text}"\n'
        )
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            raw = ((resp or {}).get("message") or {}).get("content", "")
            data = self._json_object_from_text(raw)
            action = self._normalize_action(data.get("action"), "")
            return {"action": action, "target": self._coerce_text(data.get("target"))}
        except Exception as exc:
            logger.warning("[%s] _ai_pick_action failed: %s", self.name, exc)
            return {}

    def _target_text(self, args: dict, query: str) -> str:
        target = self._coerce_text(args.get("target") or args.get("door") or args.get("door_name"))
        if target:
            return target
        text = self._coerce_text(query).lower()
        text = re.sub(r"\b(aladdin|connect|genie|garage|door|doors|please|can you|would you|the|my|a|an)\b", " ", text)
        text = re.sub(
            r"\b(open|close|closed|shut|lower|raise|lift|status|state|check|is|are|what|whether|currently|right now)\b",
            " ",
            text,
        )
        return " ".join(re.findall(r"[a-z0-9]+", text))

    def _find_door(self, doors: List[dict], args: dict, query: str) -> Tuple[Optional[dict], List[str]]:
        device_id = self._coerce_text(args.get("device_id"))
        door_number = self._coerce_text(args.get("door_number") or args.get("door_index"))
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
            ai_payload = await self._ai_pick_action(llm_client, query)
            action = self._normalize_action(ai_payload.get("action"), "")
            if ai_payload.get("target") and not any(
                self._coerce_text(payload.get(key)) for key in ("target", "door", "door_name")
            ):
                payload["target"] = ai_payload.get("target")
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
                needs=["Set Aladdin Connect username and password in Tater Settings > Integrations > Aladdin Connect."],
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

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self.handle_webui(args or {}, llm_client)

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
