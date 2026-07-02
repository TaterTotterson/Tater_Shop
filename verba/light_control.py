import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from helpers import extract_json
from integration_registry import get_integration_devices_by_capability, run_integration_device_action
from verba_base import ToolVerba
from verba_result import action_failure, action_success


logger = logging.getLogger("light_control")
logger.setLevel(logging.INFO)


class LightControlPlugin(ToolVerba):
    name = "light_control"
    verba_name = "Light Control"
    pretty_name = "Light Control"
    version = "1.0.0"
    min_tater_version = "59"
    settings_category = "Device Control"
    platforms = [
        "voice_core",
        "homeassistant",
        "webui",
        "little_spud",
        "macos",
        "xbmc",
        "homekit",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    tags = ["light", "device", "integration"]
    routing_keywords = ["light", "lights", "lamp", "lamps", "bulb", "bulbs", "dimmer", "brightness"]
    forced_route = "light"
    forced_domain_hint = "light"

    usage = '{"function":"light_control","arguments":{"query":"turn off the office lights"}}'
    description = "Control lights across all enabled integrations using the shared device registry."
    verba_dec = "Control lights from Home Assistant, Philips Hue, Shelly, and any integration that exposes light devices."
    when_to_use = "Use for light status checks, inventory, on/off, brightness, and color requests regardless of which integration owns the light."
    how_to_use = "Pass one natural-language light request in query. The verba resolves the room or device across enabled integrations."
    common_needs = ["Light room/device target and action, such as office lights + turn off."]
    missing_info_prompts = ["Which light or room should I control?"]
    example_calls = [
        '{"function":"light_control","arguments":{"query":"turn off the office lights"}}',
        '{"function":"light_control","arguments":{"query":"set kitchen lights to 30 percent"}}',
        '{"function":"light_control","arguments":{"query":"are the porch lights on?"}}',
    ]

    required_settings = {
        "LIGHT_MAX_CANDIDATES": {
            "label": "Max Light Candidates",
            "type": "number",
            "default": 160,
            "description": "Maximum light candidates sent to chooser LLM calls.",
        },
    }

    allowed_actions = {"list", "status", "turn_on", "turn_off", "toggle", "set_brightness", "set_color"}
    control_actions = {"turn_on", "turn_off", "toggle", "set_brightness", "set_color"}
    ignored_target_words = {
        "light",
        "lights",
        "lamp",
        "lamps",
        "bulb",
        "bulbs",
        "dimmer",
        "dimmers",
        "the",
        "my",
        "all",
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are checking or controlling lights now. "
        "Only output that message."
    )

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _normalize_handler_args(args: Any) -> Dict[str, Any]:
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
            text = str(args or "").strip()
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
                    return parsed
            except Exception:
                pass
            return {"query": text}
        return {}

    @staticmethod
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

    def _get_plugin_settings(self) -> dict:
        try:
            from helpers import redis_client

            merged = {}
            for key in ("verba_settings:Device Control", "verba_settings: Device Control"):
                merged.update(self._decode_map(redis_client.hgetall(key) or {}))
            return merged
        except Exception:
            return {}

    def _get_int_setting(self, key: str, default: int, minimum: int, maximum: int) -> int:
        raw = self._get_plugin_settings().get(key)
        try:
            value = int(float(str(raw).strip()))
        except Exception:
            value = int(default)
        return max(minimum, min(maximum, value))

    def _json_object_from_text(self, text: str) -> Dict[str, Any]:
        clean = self._text(text)
        clean = re.sub(r"^```(?:json)?\s*", "", clean, flags=re.I)
        clean = re.sub(r"\s*```$", "", clean).strip()
        try:
            parsed = json.loads(clean)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass
        try:
            blob = extract_json(clean) or ""
            if blob:
                parsed = json.loads(blob)
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

    async def _llm_json(self, *, llm_client, system: str, user_payload: dict, max_tokens: int = 260) -> dict:
        if llm_client is None:
            return {}
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                max_tokens=max_tokens,
                temperature=0.0,
            )
            return self._json_object_from_text(self._text((resp.get("message", {}) or {}).get("content", "")))
        except Exception as exc:
            logger.debug("[light_control] llm_json failed: %s", exc)
            return {}

    def _normalize_int(self, value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(round(float(value)))
        except Exception:
            return None

    def _percent_from_text(self, text: Any) -> Optional[int]:
        raw = self._text(text).lower()
        for pattern in (r"\b(\d{1,3})\s*%", r"\b(?:to|at|brightness|level)\s+(\d{1,3})\b"):
            match = re.search(pattern, raw)
            if not match:
                continue
            try:
                return max(0, min(100, int(round(float(match.group(1))))))
            except Exception:
                continue
        return None

    def _normalize_rgb(self, value: Any) -> Optional[List[int]]:
        if not isinstance(value, (list, tuple)) or len(value) != 3:
            return None
        out: List[int] = []
        for item in value:
            parsed = self._normalize_int(item)
            if parsed is None:
                return None
            out.append(max(0, min(255, parsed)))
        return out

    def _extract_rgb_from_text(self, text: str) -> Optional[List[int]]:
        for pattern in (
            r"\brgb\s*\(\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\s*\)",
            r"\brgb\s*[:=]?\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\b",
        ):
            match = re.search(pattern, self._text(text), flags=re.I)
            if not match:
                continue
            try:
                return [max(0, min(255, int(match.group(i)))) for i in (1, 2, 3)]
            except Exception:
                continue
        return None

    def _extract_hex_rgb_from_text(self, text: str) -> Optional[List[int]]:
        match = re.search(r"#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})\b", self._text(text))
        if not match:
            return None
        token = match.group(1).lower()
        if len(token) == 3:
            token = "".join(ch * 2 for ch in token)
        try:
            return [int(token[0:2], 16), int(token[2:4], 16), int(token[4:6], 16)]
        except Exception:
            return None

    def _normalize_action(self, value: Any, query: str) -> str:
        explicit = self._text(value).lower().replace("-", "_").replace(" ", "_")
        aliases = {
            "on": "turn_on",
            "off": "turn_off",
            "power_on": "turn_on",
            "power_off": "turn_off",
            "state": "status",
            "get_state": "status",
            "check": "status",
            "inventory": "list",
            "devices": "list",
            "brightness": "set_brightness",
            "color": "set_color",
        }
        action = aliases.get(explicit, explicit)
        if action in self.allowed_actions:
            return action

        text = f" {self._text(query).lower()} "
        compact = re.sub(r"\s+", " ", text)
        if re.search(r"\b(list|show)\b.*\b(lights|lamps|bulbs)\b", compact):
            return "list"
        if re.search(r"\b(toggle|flip)\b", compact):
            return "toggle"
        if re.search(r"\b(turn|switch|power|shut|set)\s+(?:[\w -]+?\s+)?off\b", compact) or re.search(r"\b(disable|deactivate)\b", compact):
            return "turn_off"
        has_brightness_percent = self._percent_from_text(compact) is not None
        if has_brightness_percent and (
            "%" in compact
            or re.search(r"\b(brightness|brighten|dim|level|percent)\b", compact)
            or re.search(r"\b(turn|switch|power|set)\b.*\b(to|at)\s+\d{1,3}\s*(?:%|percent)?\b", compact)
        ):
            return "set_brightness"
        if re.search(r"\b(turn|switch|power|set)\s+(?:[\w -]+?\s+)?on\b", compact) or re.search(r"\b(enable|activate)\b", compact):
            return "turn_on"
        if self._extract_rgb_from_text(compact) or self._extract_hex_rgb_from_text(compact):
            return "set_color"
        if re.search(r"\b(red|orange|amber|yellow|green|blue|cyan|teal|purple|violet|magenta|pink|white|daylight)\b", compact):
            return "set_color"
        if re.search(r"\b(status|state|check|is|are|what|whether|show me|how)\b", compact) or "?" in compact:
            return "status"
        return ""

    def _clean_target(self, value: Any) -> str:
        text = self._text(value).lower()
        text = re.sub(r"\b(turn|switch|power|set|shut|enable|disable|activate|deactivate|toggle|flip|status|state|check|is|are|what|whether|show|list|on|off|to|at|brightness|brighten|dim|level|percent|currently|right|now|please|can|you|could|would)\b", " ", text)
        ignored = [re.escape(item) for item in sorted(self.ignored_target_words, key=len, reverse=True)]
        if ignored:
            text = re.sub(r"\b(" + "|".join(ignored) + r")\b", " ", text)
        text = re.sub(r"\d{1,3}\s*(?:%|percent)?", " ", text)
        text = re.sub(r"#[0-9a-fA-F]{3,6}\b", " ", text)
        text = re.sub(r"\brgb\s*\([^)]*\)", " ", text, flags=re.I)
        return " ".join(re.findall(r"[a-z0-9]+", text))

    async def _interpret_query(self, payload: dict, query: str, llm_client) -> dict:
        action = self._normalize_action(payload.get("action"), query)
        brightness = self._normalize_int(
            payload.get("brightness_pct", payload.get("brightness", payload.get("level")))
        )
        if brightness is None:
            brightness = self._percent_from_text(query)
        if brightness is not None:
            brightness = max(0, min(100, brightness))
        rgb = self._normalize_rgb(payload.get("rgb_color")) or self._extract_rgb_from_text(query) or self._extract_hex_rgb_from_text(query)
        color_name = self._text(payload.get("color_name")).lower()
        target = self._text(payload.get("target") or payload.get("room") or payload.get("device") or payload.get("name"))

        if not action or not target:
            system = (
                "Interpret one smart-light request across multiple home integrations.\n"
                "Return STRICT JSON only with keys: action, target, brightness_pct, color_name, rgb_color.\n"
                "Allowed action values: list, status, turn_on, turn_off, toggle, set_brightness, set_color, or empty string.\n"
                "Rules:\n"
                "- target is the named room or specific light, without action words.\n"
                "- Use set_brightness for brightness or dimming percentages.\n"
                "- Use set_color for named colors, hex colors, rgb values, or vague color requests.\n"
                "- brightness_pct is an integer 0-100 or null.\n"
                "- rgb_color is [r,g,b] or null.\n"
            )
            ai = await self._llm_json(llm_client=llm_client, system=system, user_payload={"query": query}, max_tokens=220)
            if ai:
                action = action or self._normalize_action(ai.get("action"), query)
                target = target or self._text(ai.get("target"))
                if brightness is None:
                    brightness = self._normalize_int(ai.get("brightness_pct"))
                    if brightness is not None:
                        brightness = max(0, min(100, brightness))
                if rgb is None:
                    rgb = self._normalize_rgb(ai.get("rgb_color"))
                if not color_name:
                    color_name = self._text(ai.get("color_name")).lower()

        if not target:
            target = self._clean_target(query)
        return {
            "action": action,
            "target": target,
            "brightness_pct": brightness,
            "color_name": color_name,
            "rgb_color": rgb,
        }

    def _device_actions(self, device: dict) -> set:
        return {self._text(item).lower() for item in (device.get("actions") or []) if self._text(item)}

    def _device_caps(self, device: dict) -> set:
        return {self._text(item).lower() for item in (device.get("capabilities") or []) if self._text(item)}

    def _action_supported(self, device: dict, action: str) -> bool:
        if action in {"list", "status"}:
            return True
        actions = self._device_actions(device)
        caps = self._device_caps(device)
        if action in actions:
            return True
        if action in {"turn_on", "turn_off"} and "light" in caps:
            return True
        if action == "set_brightness" and "dimmable" in caps:
            return True
        if action == "set_color" and "color" in caps:
            return True
        return False

    def _device_alias_text(self, device: dict) -> str:
        details = device.get("details") if isinstance(device.get("details"), dict) else {}
        bits = [
            device.get("id"),
            device.get("ref"),
            device.get("name"),
            device.get("room"),
            device.get("area"),
            device.get("integration_name"),
            device.get("integration_id"),
            device.get("type"),
            details.get("friendly_name"),
            details.get("alias"),
            details.get("device_id"),
            details.get("model"),
        ]
        return " ".join(self._text(bit).lower() for bit in bits if self._text(bit))

    def _tokens(self, value: Any) -> List[str]:
        return [token for token in re.split(r"[^a-z0-9]+", self._text(value).lower()) if token]

    def _score_device(self, target: str, device: dict) -> int:
        target_text = self._clean_target(target)
        if not target_text:
            return 0
        name = self._text(device.get("name")).lower()
        room = self._text(device.get("room") or device.get("area")).lower()
        alias = self._device_alias_text(device)
        if target_text == name:
            return 1000
        score = 0
        if target_text in name:
            score += 520 + len(target_text)
        if target_text and target_text == self._clean_target(room):
            score += 300
        elif target_text in alias:
            score += 220 + len(target_text)
        target_tokens = self._tokens(target_text)
        alias_tokens = set(self._tokens(alias))
        for token in target_tokens:
            if token in alias_tokens:
                score += 75
            elif len(token) >= 4 and any(part.startswith(token) for part in alias_tokens):
                score += 30
        return score

    def _room_matches(self, target: str, devices: List[dict]) -> List[dict]:
        clean_target = self._clean_target(target)
        if not clean_target:
            return []
        matches: List[dict] = []
        for device in devices:
            room = self._clean_target(device.get("room") or device.get("area"))
            if room and clean_target == room:
                matches.append(device)
        return matches

    async def _ai_choose_device(self, *, query: str, intent: dict, candidates: List[dict], llm_client) -> str:
        if llm_client is None or not candidates:
            return ""
        limit = self._get_int_setting("LIGHT_MAX_CANDIDATES", 160, 5, 800)
        shortlist = candidates[:limit]
        compact = [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "room": row.get("room") or row.get("area"),
                "integration": row.get("integration_name") or row.get("integration_id"),
                "state": row.get("state") or row.get("status"),
                "actions": row.get("actions") or [],
            }
            for row in shortlist
        ]
        valid_ids = {self._text(row.get("id")) for row in shortlist}
        system = (
            "Choose the best light for this request.\n"
            "Return STRICT JSON only: {\"device_id\":\"<id from candidates or empty>\"}.\n"
            "Pick exactly one id only if the request clearly names a single light. Do not invent ids."
        )
        payload = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={"query": query, "intent": intent, "candidates": compact},
            max_tokens=220,
        )
        picked = self._text(payload.get("device_id"))
        return picked if picked in valid_ids else ""

    async def _select_devices(self, *, devices: List[dict], payload: dict, query: str, intent: dict, llm_client) -> Tuple[List[dict], List[str]]:
        action = self._text(intent.get("action"))
        candidates = [device for device in devices if self._action_supported(device, action)]
        if not candidates:
            return [], [f"No lights support {action}."]

        explicit_id = self._text(payload.get("device_id") or payload.get("id") or payload.get("ref"))
        if explicit_id:
            for device in candidates:
                if self._text(device.get("id")) == explicit_id or self._text(device.get("ref")) == explicit_id:
                    return [device], []
            return [], [f"No light matched id {explicit_id}."]

        target = self._text(intent.get("target")) or self._clean_target(query)
        if not target:
            return [], ["Choose a room or light name."]

        room_matches = self._room_matches(target, candidates)
        if room_matches:
            return room_matches, []

        scored = [(self._score_device(target, device), device) for device in candidates]
        scored = [(score, device) for score, device in scored if score > 0]
        scored.sort(key=lambda item: (item[0], self._text(item[1].get("name")).lower()), reverse=True)
        if scored and (len(scored) == 1 or scored[0][0] > scored[1][0]):
            return [scored[0][1]], []

        picked_id = await self._ai_choose_device(query=query, intent=intent, candidates=candidates, llm_client=llm_client)
        if picked_id:
            for device in candidates:
                if self._text(device.get("id")) == picked_id:
                    return [device], []

        if scored and len(scored) > 1 and scored[0][0] == scored[1][0]:
            tied = [device for score, device in scored if score == scored[0][0]]
            return [], ["That matched multiple lights: " + self._format_device_choices(tied[:10])]
        return [], [f"I could not match '{target}' to a light. Available matches: {self._format_device_choices(candidates[:12])}"]

    def _format_device_choices(self, devices: List[dict]) -> str:
        parts: List[str] = []
        for device in devices:
            name = self._text(device.get("name")) or self._text(device.get("id")) or "light"
            room = self._text(device.get("room") or device.get("area"))
            state = self._text(device.get("state") or device.get("status"))
            provider = self._text(device.get("integration_name") or device.get("integration_id"))
            meta = ", ".join(part for part in (room, state, provider) if part)
            parts.append(f"{name} ({meta})" if meta else name)
        return ", ".join(parts) if parts else "none"

    def _list_summary(self, devices: List[dict]) -> str:
        if not devices:
            return "No lights were found."
        by_room: Dict[str, int] = {}
        for device in devices:
            room = self._text(device.get("room") or device.get("area")) or "Unassigned"
            by_room[room] = by_room.get(room, 0) + 1
        rooms = ", ".join(f"{room} ({count})" for room, count in sorted(by_room.items())[:8])
        suffix = f", and {len(by_room) - 8} more rooms" if len(by_room) > 8 else ""
        return f"Found {len(devices)} lights: {rooms}{suffix}."

    def _status_summary(self, devices: List[dict]) -> str:
        if len(devices) == 1:
            device = devices[0]
            name = self._text(device.get("name")) or "Light"
            state = self._text(device.get("state") or device.get("status")) or "unknown"
            room = self._text(device.get("room") or device.get("area"))
            return f"{name}{f' in {room}' if room else ''} is {state}."
        shown = ", ".join(
            f"{self._text(device.get('name')) or 'Light'} ({self._text(device.get('state') or device.get('status') or 'unknown')})"
            for device in devices[:10]
        )
        suffix = f", and {len(devices) - 10} more" if len(devices) > 10 else ""
        return f"{shown}{suffix}."

    def _action_payload(self, intent: dict) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        brightness = intent.get("brightness_pct")
        if brightness is not None:
            payload["brightness_pct"] = brightness
            payload["brightness"] = brightness
        color_name = self._text(intent.get("color_name"))
        rgb = self._normalize_rgb(intent.get("rgb_color"))
        if color_name:
            payload["color_name"] = color_name
        if rgb is not None:
            payload["rgb_color"] = rgb
        return payload

    async def _handle(self, args, llm_client=None):
        payload = self._normalize_handler_args(args)
        query = self._text(payload.get("query") or payload.get("text") or payload.get("prompt"))
        if not query and self._text(payload.get("action")):
            query = self._text(payload.get("action"))
        if not query:
            return action_failure(
                code="missing_query",
                message="Please provide a light request in query.",
                needs=["Ask for a light action such as list, status, turn on, turn off, or set brightness."],
                say_hint="Ask what light action the user wants.",
            )

        intent = await self._interpret_query(payload, query, llm_client)
        action = self._text(intent.get("action")).lower()
        if action not in self.allowed_actions:
            return action_failure(
                code="missing_action",
                message="Please ask for a supported light action.",
                needs=["Use list, status, turn_on, turn_off, toggle, set_brightness, or set_color."],
                say_hint="Ask which light action the user wants.",
            )
        if action == "set_brightness" and intent.get("brightness_pct") is None:
            return action_failure(
                code="missing_brightness",
                message="Light brightness requires a percentage.",
                needs=["Include a brightness percentage from 0 to 100."],
                say_hint="Ask for the brightness percentage.",
            )
        if action == "set_color" and not (self._text(intent.get("color_name")) or self._normalize_rgb(intent.get("rgb_color"))):
            return action_failure(
                code="missing_color",
                message="Light color requires a color name or RGB value.",
                needs=["Include a color such as blue, warm white, #33ccff, or rgb(51,204,255)."],
                say_hint="Ask what color to use.",
            )

        try:
            devices = [dict(row) for row in get_integration_devices_by_capability("light") if isinstance(row, dict)]
        except Exception as exc:
            return action_failure(
                code="light_inventory_failed",
                message=f"Could not read integrated lights: {exc}",
                needs=["Check enabled integrations and their settings."],
                say_hint="Explain that Tater could not read the light inventory.",
            )

        if action == "list":
            return action_success(
                facts={"action": "list", "device_count": len(devices)},
                data={"devices": devices},
                summary_for_user=self._list_summary(devices),
                say_hint="Briefly summarize available lights by room.",
            )

        if not devices:
            return action_failure(
                code="no_lights",
                message="No lights were found in enabled integrations.",
                needs=["Enable an integration that exposes light devices, then refresh devices."],
                say_hint="Explain that no lights are currently available.",
            )

        selected, needs = await self._select_devices(
            devices=devices,
            payload=payload,
            query=query,
            intent=intent,
            llm_client=llm_client,
        )
        if not selected:
            return action_failure(
                code="light_selection_failed",
                message="Could not select a light for this request.",
                needs=needs,
                say_hint="Ask which light or room to use.",
            )

        if action == "status":
            return action_success(
                facts={"action": "status", "device_count": len(selected)},
                data={"devices": selected, "intent": intent},
                summary_for_user=self._status_summary(selected),
                say_hint="Report the light status briefly.",
            )

        action_payload = self._action_payload(intent)
        results: List[dict] = []
        failures: List[str] = []
        for device in selected:
            integration_id = self._text(device.get("integration_id"))
            device_id = self._text(device.get("id") or device.get("ref"))
            if not integration_id or not device_id:
                failures.append(f"{self._text(device.get('name')) or 'Light'} is missing integration or device id.")
                continue
            try:
                result = run_integration_device_action(integration_id, action, device_id, action_payload)
                results.append(
                    {
                        "integration_id": integration_id,
                        "device_id": device_id,
                        "device_name": device.get("name"),
                        "result": result,
                    }
                )
            except Exception as exc:
                failures.append(f"{self._text(device.get('name')) or device_id}: {exc}")

        if failures and not results:
            return action_failure(
                code="light_action_failed",
                message="Light action failed: " + "; ".join(failures[:4]),
                needs=["Check the owning integration settings and whether the light supports this action."],
                data={"devices": selected, "intent": intent, "failures": failures},
                say_hint="Explain that the light action failed.",
            )

        label = {
            "turn_on": "turned on",
            "turn_off": "turned off",
            "toggle": "toggled",
            "set_brightness": "updated brightness for",
            "set_color": "updated color for",
        }.get(action, "updated")
        target_label = (
            self._text(selected[0].get("room") or selected[0].get("area"))
            if len(selected) > 1 and len({self._text(row.get("room") or row.get("area")) for row in selected}) == 1
            else self._format_device_choices(selected[:5])
        )
        summary = f"{label.capitalize()} {len(results)} light{'s' if len(results) != 1 else ''}"
        if target_label:
            summary += f" for {target_label}"
        summary += "."
        if failures:
            summary += f" {len(failures)} light{'s' if len(failures) != 1 else ''} failed."

        return action_success(
            facts={
                "action": action,
                "requested_count": len(selected),
                "success_count": len(results),
                "failure_count": len(failures),
            },
            data={"devices": selected, "intent": intent, "results": results, "failures": failures},
            summary_for_user=summary,
            say_hint="Confirm the light action briefly and mention failures if any.",
        )

    async def handle_webui(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_voice_core(self, args, llm_client, context=None):
        return await self._handle(args, llm_client)

    async def handle_macos(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_little_spud(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_xbmc(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_discord(self, message, args, llm_client):
        payload = self._normalize_handler_args(args or {})
        if not payload.get("query"):
            content = getattr(message, "content", "")
            if content:
                payload["query"] = content
        return await self._handle(payload, llm_client)

    async def handle_telegram(self, update, context, args, llm_client):
        payload = self._normalize_handler_args(args or {})
        if not payload.get("query"):
            message = getattr(update, "message", None)
            text = self._text(getattr(message, "text", ""))
            if text:
                payload["query"] = text
        return await self._handle(payload, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        payload = self._normalize_handler_args(args or {})
        if not payload.get("query") and body:
            payload["query"] = body
        return await self._handle(payload, llm_client)

    async def handle_irc(self, bot, channel, user, message, args, llm_client):
        payload = self._normalize_handler_args(args or {})
        if not payload.get("query") and message:
            payload["query"] = message
        return await self._handle(payload, llm_client)

    async def handle_meshtastic(self, packet, args, llm_client):
        return await self._handle(args or {}, llm_client)


verba = LightControlPlugin()
