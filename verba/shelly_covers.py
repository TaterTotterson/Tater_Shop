import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from helpers import extract_json, redis_client
from tateros import integration_store as integration_store_module
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("shelly_covers")
logger.setLevel(logging.INFO)


def _shelly_module(*, required: bool = True):
    module = integration_store_module.integration_module("shelly")
    if module is None and required:
        raise RuntimeError("Shelly integration is not enabled.")
    return module


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


class ShellyCoversPlugin(ToolVerba):
    name = "shelly_covers"
    verba_name = "Shelly Covers"
    pretty_name = "Shelly Covers"
    version = "1.0.0"
    min_tater_version = "59"
    settings_category = "Shelly Control"
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
    tags = ["shelly", "cover", "roller", "shade", "blind"]
    routing_keywords = [
        "cover",
        "roller",
        "shade",
        "blind",
        "curtain",
    ]
    forced_route = "cover"
    forced_domain_hint = "cover"

    usage = '{"function":"shelly_covers","arguments":{"query":"close the office shade"}}'
    description = "Open, close, stop, set position, and check covers like blinds, shades, curtains, and rollers."
    verba_dec = "Control covers, rollers, shades, blinds, and curtains."
    when_to_use = (
        "Use for opening, closing, stopping, positioning, or checking blinds, shades, curtains, rollers, and other cover entities."
    )
    how_to_use = "Pass one natural-language cover request in query. The verba resolves the cover resource and action."
    common_needs = ["Cover target and action, such as office shade + close or patio roller + set to 40 percent."]
    missing_info_prompts = ["Which cover, blind, shade, curtain, or roller should I control?"]
    example_calls = [
        '{"function":"shelly_covers","arguments":{"query":"open the office shade"}}',
        '{"function":"shelly_covers","arguments":{"query":"close the patio cover"}}',
        '{"function":"shelly_covers","arguments":{"query":"set the roller to 30%"}}',
    ]

    required_settings = {
        "SHELLY_MAX_CANDIDATES": {
            "label": "Max Device Candidates",
            "type": "number",
            "default": 120,
            "description": "Maximum Shelly device candidates sent to chooser LLM calls.",
        },
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are checking or controlling Shelly devices now. "
        "Only output that message."
    )

    allowed_actions = {
        "discover",
        "list",
        "status",
        "open",
        "close",
        "stop",
        "set_position",
    }
    control_actions = {
        "open",
        "close",
        "stop",
        "set_position",
    }
    action_labels = {
        "open": "open",
        "close": "close",
        "stop": "stop",
        "set_position": "set position",
    }
    device_types = {"cover"}
    domain_label = "cover"
    domain_label_plural = "covers"
    ignored_target_words = {
        "device",
        "devices",
        "cover",
        "covers",
        "roller",
        "rollers",
        "shade",
        "shades",
        "blind",
        "blinds",
        "curtain",
        "curtains",
    }

    @staticmethod
    def _coerce_text(value: Any) -> str:
        return _coerce_text(value)

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
        merged: dict = {}
        for key in ("verba_settings:Shelly Control", "verba_settings: Shelly Control"):
            try:
                merged.update(self._decode_map(redis_client.hgetall(key) or {}))
            except Exception:
                continue
        return merged

    def _get_int_setting(self, key: str, default: int, minimum: int, maximum: int) -> int:
        raw = self._get_plugin_settings().get(key)
        try:
            val = int(float(str(raw).strip()))
        except Exception:
            val = int(default)
        return max(minimum, min(maximum, val))

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
        try:
            module = _shelly_module(required=False)
            if module is None:
                return {"shelly_integration": "missing"}
            settings_fn = getattr(module, "read_integration_settings", None)
            if not callable(settings_fn):
                return {"shelly_integration": "missing_settings_reader"}
            settings = settings_fn()
            return {
                "shelly_integration": "set",
                "shelly_enabled": "set" if bool(settings.get("shelly_enabled")) else "missing",
                "shelly_device_hosts": "set" if self._coerce_text(settings.get("shelly_device_hosts")) else "optional",
            }
        except Exception as exc:
            return {"shelly_integration": f"error: {exc}"}

    def _get_module(self) -> Optional[Any]:
        try:
            return _shelly_module()
        except Exception as exc:
            logger.error("[%s] Failed to initialize Shelly integration: %s", self.name, exc)
            return None

    def _json_object_from_text(self, text: str) -> Dict[str, Any]:
        clean = self._coerce_text(text)
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

    async def _llm_json(
        self,
        *,
        llm_client,
        system: str,
        user_payload: dict,
        max_tokens: int = 260,
        temperature: float = 0.0,
    ) -> dict:
        if llm_client is None:
            return {}
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                max_tokens=max_tokens,
                temperature=temperature,
            )
            content = self._coerce_text((resp.get("message", {}) or {}).get("content", ""))
            return self._json_object_from_text(content)
        except Exception as exc:
            logger.debug("[%s] llm_json failed: %s", self.name, exc)
            return {}

    def _normalize_action(self, value: Any, query: str) -> str:
        explicit = self._coerce_text(value).lower().replace("-", "_").replace(" ", "_")
        aliases = {
            "on": "turn_on",
            "switch_on": "turn_on",
            "power_on": "turn_on",
            "enable": "turn_on",
            "off": "turn_off",
            "switch_off": "turn_off",
            "power_off": "turn_off",
            "disable": "turn_off",
            "state": "status",
            "get_state": "status",
            "check": "status",
            "devices": "list",
            "inventory": "list",
            "brightness": "set_brightness",
            "position": "set_position",
        }
        action = aliases.get(explicit, explicit)
        if action in self.allowed_actions:
            return action

        text = f" {self._coerce_text(query).lower()} "
        compact = re.sub(r"\s+", " ", text)
        if not compact.strip():
            return ""

        if re.search(r"\b(discover|scan|refresh)\b", compact) and re.search(r"\bshelly|device|plug|switch|light|cover\b", compact):
            return "discover"
        if re.search(r"\b(list|show)\b.*\b(shelly|devices|plugs|switches|lights|covers|sensors)\b", compact):
            return "list"
        if re.search(
            r"\bwhat\b.*\b(shelly\s+)?(devices|plugs|switches|lights|covers|sensors)\b.*\b(have|available|configured|found)\b",
            compact,
        ):
            return "list"
        if re.search(
            r"\bwhat\b.*\b(have|available|configured|found)\b.*\b(shelly|devices|plugs|switches|lights|covers|sensors)\b",
            compact,
        ):
            return "list"

        status_markers = bool(
            re.search(r"\b(status|state|check|is|are|what|whether|tell me|show me|how)\b", compact)
            or "?" in compact
        )
        if re.search(r"\b(toggle|flip)\b", compact):
            return "toggle"
        if re.search(r"\b(turn|switch|power|set)\s+(?:the\s+|my\s+|that\s+|it\s+)?(?:[\w -]+?\s+)?on\b", compact):
            return "turn_on"
        if re.search(r"\b(enable|activate)\b", compact):
            return "turn_on"
        if re.search(r"\b(turn|switch|power|shut|set)\s+(?:the\s+|my\s+|that\s+|it\s+)?(?:[\w -]+?\s+)?off\b", compact):
            return "turn_off"
        if re.search(r"\b(disable|deactivate)\b", compact):
            return "turn_off"
        if re.search(r"\b(brightness|dim|level)\b", compact) and self._percent_from_text(compact) is not None:
            return "set_brightness"
        if re.search(r"\b(to|at)\s+\d{1,3}\s*%", compact) and re.search(r"\b(light|lamp|dimmer)\b", compact):
            return "set_brightness"
        if re.search(r"\b(position|percent|roller_pos|cover_pos)\b", compact) and self._percent_from_text(compact) is not None:
            return "set_position"
        if re.search(r"\b(to|at)\s+\d{1,3}\s*%", compact) and re.search(r"\b(cover|shade|blind|curtain|roller)\b", compact):
            return "set_position"
        if not status_markers and re.search(r"\b(open|raise)\b", compact):
            return "open"
        if not status_markers and re.search(r"\b(close|lower)\b", compact):
            return "close"
        if not status_markers and re.search(r"\bstop\b", compact):
            return "stop"
        if status_markers:
            return "status"
        return ""

    def _percent_from_text(self, text: Any) -> Optional[int]:
        raw = self._coerce_text(text).lower()
        patterns = [
            r"\b(\d{1,3})\s*%",
            r"\b(?:to|at|brightness|level|position|pos)\s+(\d{1,3})\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, raw)
            if not match:
                continue
            try:
                value = int(float(match.group(1)))
            except Exception:
                continue
            return max(0, min(100, value))
        return None

    def _number_payload_value(self, payload: dict, keys: Tuple[str, ...], query: str) -> Optional[int]:
        for key in keys:
            value = (payload or {}).get(key)
            if value is None or isinstance(value, bool):
                continue
            try:
                return max(0, min(100, int(round(float(value)))))
            except Exception:
                continue
        return self._percent_from_text(query)

    def _target_text(self, payload: dict, query: str, intent: Optional[dict] = None) -> str:
        for key in ("target", "device", "device_name", "name"):
            value = self._coerce_text((payload or {}).get(key))
            if value:
                return value
        if isinstance(intent, dict):
            value = self._coerce_text(intent.get("target"))
            if value:
                return value

        text = self._coerce_text(query).lower()
        text = re.sub(r"\b(shelly|please|can you|could you|would you|the|my|a|an|that|it)\b", " ", text)
        text = re.sub(
            r"\b(turn|switch|power|set|shut|enable|disable|activate|deactivate|toggle|flip|open|close|closed|"
            r"raise|lower|stop|status|state|check|is|are|what|whether|show|list|discover|scan|refresh|"
            r"on|off|to|at|brightness|level|position|percent|currently|right|now)\b",
            " ",
            text,
        )
        ignored = {self._coerce_text(item).lower() for item in self.ignored_target_words if self._coerce_text(item)}
        if ignored:
            pattern = r"\b(" + "|".join(re.escape(item) for item in sorted(ignored, key=len, reverse=True)) + r")\b"
            text = re.sub(pattern, " ", text)
        text = re.sub(r"\d{1,3}\s*%", " ", text)
        return " ".join(re.findall(r"[a-z0-9]+", text))

    def _normalize_intent(self, payload: dict, query: str) -> dict:
        action = self._normalize_action(payload.get("action"), query)
        if action not in self.allowed_actions:
            return {}
        brightness = self._number_payload_value(payload, ("brightness", "level", "percent", "brightness_pct"), query)
        position = self._number_payload_value(payload, ("position", "pos", "cover_pos", "roller_pos"), query)
        target = self._coerce_text(payload.get("target"))
        return {
            "action": action,
            "target": target,
            "brightness": brightness,
            "position": position,
        }

    async def _ai_interpret(self, llm_client, query: str) -> dict:
        if llm_client is None or not self._coerce_text(query):
            return {}
        system = (
            f"Interpret one {self.domain_label} request.\n"
            "Return STRICT JSON only with keys: action, target, brightness, position.\n"
            f"Allowed action values: {', '.join(sorted(self.allowed_actions))}, or empty string.\n"
            "Rules:\n"
            "- Use open/close/stop for cover movement requests.\n"
            "- Use set_position when the user asks for a cover, shade, blind, curtain, or roller percentage.\n"
            "- Use status for questions asking whether something is open, closed, moving, or what its state is.\n"
            "- Use list for inventory questions.\n"
            "- target is the short device phrase the user named, without action words.\n"
            "- brightness and position must be numbers 0-100 or null.\n"
        )
        payload = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={"query": self._coerce_text(query)},
            max_tokens=220,
            temperature=0.0,
        )
        if not payload:
            return {}
        action = self._normalize_action(payload.get("action"), "")
        if action not in self.allowed_actions:
            return {}
        out = dict(payload)
        out["action"] = action
        for key in ("brightness", "position"):
            value = out.get(key)
            if value is None or value == "":
                out[key] = None
                continue
            try:
                out[key] = max(0, min(100, int(round(float(value)))))
            except Exception:
                out[key] = None
        out["target"] = self._coerce_text(out.get("target"))
        return out

    async def _interpret_query(self, payload: dict, query: str, llm_client) -> dict:
        intent = self._normalize_intent(payload, query)
        if not intent or not intent.get("target"):
            ai_intent = await self._ai_interpret(llm_client, query)
            if ai_intent:
                if not intent:
                    intent = ai_intent
                else:
                    for key, value in ai_intent.items():
                        if intent.get(key) in (None, "") and value not in (None, ""):
                            intent[key] = value
        if not intent:
            return {}
        if not intent.get("target"):
            target = self._target_text(payload, query, intent)
            if target:
                intent["target"] = target
        if intent.get("action") == "set_brightness" and intent.get("brightness") is None:
            intent["brightness"] = self._percent_from_text(query)
        if intent.get("action") == "set_position" and intent.get("position") is None:
            intent["position"] = self._percent_from_text(query)
        return intent

    def _domain_device(self, device: dict) -> bool:
        if not isinstance(device, dict):
            return False
        dtype = self._coerce_text(device.get("type")).lower()
        if self.device_types and dtype not in set(self.device_types):
            return False
        return True

    def _filter_domain_devices(self, devices: List[dict]) -> List[dict]:
        return [row for row in devices or [] if self._domain_device(row)]

    def _integration_devices(self, module: Any) -> Tuple[List[Dict[str, Any]], str, List[str]]:
        devices_fn = getattr(module, "integration_devices", None)
        if not callable(devices_fn):
            raise RuntimeError("Installed Shelly integration does not expose device inventory.")
        payload = devices_fn()
        if not isinstance(payload, dict):
            return [], "", []
        rows = [dict(item) for item in payload.get("devices") or [] if isinstance(item, dict)]
        rows = self._filter_domain_devices(rows)
        warnings = [self._coerce_text(item) for item in payload.get("warnings") or [] if self._coerce_text(item)]
        return rows, self._coerce_text(payload.get("message")), warnings

    def _device_actions(self, device: dict) -> set:
        return {self._coerce_text(item).lower() for item in (device.get("actions") or []) if self._coerce_text(item)}

    def _device_caps(self, device: dict) -> set:
        return {self._coerce_text(item).lower() for item in (device.get("capabilities") or []) if self._coerce_text(item)}

    def _action_supported(self, device: dict, action: str) -> bool:
        if action == "status":
            return True
        actions = self._device_actions(device)
        if action in actions:
            return True
        if action in {"turn_on", "turn_off", "toggle"} and self._device_caps(device) & {"switch", "light"}:
            return action in actions
        return False

    def _candidate_devices(self, devices: List[dict], action: str) -> List[dict]:
        if action == "status":
            return list(devices)
        return [row for row in devices if self._action_supported(row, action)]

    def _device_alias_text(self, device: dict) -> str:
        details = device.get("details") if isinstance(device.get("details"), dict) else {}
        component = details.get("component") if isinstance(details.get("component"), dict) else {}
        root_host = ""
        root_url = self._coerce_text(details.get("root_url"))
        if root_url:
            root_host = self._coerce_text(urlparse(root_url).hostname)
        bits = [
            device.get("id"),
            device.get("name"),
            device.get("type"),
            device.get("status"),
            details.get("device_id"),
            details.get("mac"),
            details.get("model"),
            details.get("profile"),
            root_host,
            component.get("type"),
            component.get("id"),
            component.get("key"),
        ]
        return " ".join(self._coerce_text(bit).lower() for bit in bits if self._coerce_text(bit))

    def _tokenize(self, value: Any) -> List[str]:
        return [token for token in re.split(r"[^a-z0-9]+", self._coerce_text(value).lower()) if token]

    def _score_device(self, target: str, device: dict) -> int:
        target_text = self._coerce_text(target).lower()
        if not target_text:
            return 0
        alias_text = self._device_alias_text(device)
        device_id = self._coerce_text(device.get("id")).lower()
        name = self._coerce_text(device.get("name")).lower()
        if target_text == device_id:
            return 1000
        if target_text == name:
            return 900
        score = 0
        if target_text in name:
            score += 500 + len(target_text)
        elif target_text in alias_text:
            score += 250 + len(target_text)
        target_tokens = self._tokenize(target_text)
        alias_tokens = set(self._tokenize(alias_text))
        for token in target_tokens:
            if token in alias_tokens:
                score += 80
            elif len(token) >= 4 and any(part.startswith(token) for part in alias_tokens):
                score += 35
        device_type = self._coerce_text(device.get("type")).lower()
        caps = self._device_caps(device)
        if any(token in {"plug", "outlet", "relay"} for token in target_tokens) and ("switch" in caps or device_type == "switch"):
            score += 25
        if "light" in target_tokens and ("light" in caps or device_type == "light"):
            score += 25
        if any(token in {"cover", "blind", "shade", "curtain", "roller"} for token in target_tokens) and (
            "cover" in caps or device_type == "cover"
        ):
            score += 25
        return score

    async def _ai_choose_device(self, *, query: str, intent: dict, candidates: List[dict], llm_client) -> str:
        if llm_client is None or not candidates:
            return ""
        max_candidates = self._get_int_setting("SHELLY_MAX_CANDIDATES", 120, 5, 500)
        shortlist = candidates[:max_candidates]
        compact = [
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "type": row.get("type"),
                "state": row.get("state") or row.get("status"),
                "capabilities": row.get("capabilities") or [],
                "actions": row.get("actions") or [],
            }
            for row in shortlist
        ]
        valid_ids = {self._coerce_text(row.get("id")) for row in shortlist}
        system = (
            "Choose the best cover for this request.\n"
            "Return STRICT JSON only: {\"device_id\":\"<id from candidates or empty>\"}.\n"
            "Rules:\n"
            "- Pick exactly one id from candidates when there is a clear match.\n"
            "- Use the target phrase, action, device type, capabilities, and current state.\n"
            "- Do not invent device ids.\n"
            "- Return an empty device_id if the request is ambiguous."
        )
        payload = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={"query": self._coerce_text(query), "intent": intent, "candidates": compact},
            max_tokens=220,
            temperature=0.0,
        )
        picked = self._coerce_text(payload.get("device_id"))
        return picked if picked in valid_ids else ""

    async def _find_device(self, *, devices: List[dict], payload: dict, query: str, intent: dict, llm_client) -> Tuple[Optional[dict], List[str]]:
        action = self._coerce_text(intent.get("action"))
        candidates = self._candidate_devices(devices, action)
        if not candidates:
            return None, [f"No {self.domain_label} supports {self.action_labels.get(action, action) or action}."]

        explicit_id = self._coerce_text(payload.get("device_id") or payload.get("id") or payload.get("ref"))
        if explicit_id:
            for device in candidates:
                if self._coerce_text(device.get("id")) == explicit_id or self._coerce_text(device.get("ref")) == explicit_id:
                    return device, []
            return None, [f"No {self.domain_label} matched id {explicit_id} for this action."]

        if len(candidates) == 1:
            return candidates[0], []

        target = self._target_text(payload, query, intent)
        if not target:
            choices = self._format_device_choices(candidates[:10])
            return None, [f"Which {self.domain_label}? Available matches: {choices}"]

        scored = [(self._score_device(target, device), device) for device in candidates]
        scored = [(score, device) for score, device in scored if score > 0]
        scored.sort(key=lambda item: (item[0], self._coerce_text(item[1].get("name")).lower()), reverse=True)
        if scored and (len(scored) == 1 or scored[0][0] > scored[1][0]):
            return scored[0][1], []

        picked_id = await self._ai_choose_device(query=query, intent=intent, candidates=candidates, llm_client=llm_client)
        if picked_id:
            for device in candidates:
                if self._coerce_text(device.get("id")) == picked_id:
                    return device, []

        if scored and len(scored) > 1 and scored[0][0] == scored[1][0]:
            tied = [device for score, device in scored if score == scored[0][0]]
            return None, [f"That matched multiple {self.domain_label_plural}: " + self._format_device_choices(tied[:10])]
        return None, [f"I could not match '{target}' to a {self.domain_label}. Available matches: {self._format_device_choices(candidates[:10])}"]

    def _format_device_choices(self, devices: List[dict]) -> str:
        parts: List[str] = []
        for device in devices:
            name = self._coerce_text(device.get("name")) or self._coerce_text(device.get("id")) or self.domain_label
            state = self._coerce_text(device.get("state") or device.get("status"))
            dtype = self._coerce_text(device.get("type"))
            label = name
            if dtype:
                label += f" ({dtype}"
                if state:
                    label += f", {state}"
                label += ")"
            elif state:
                label += f" ({state})"
            parts.append(label)
        return ", ".join(parts) if parts else "none"

    def _device_summary(self, device: dict) -> str:
        name = self._coerce_text(device.get("name")) or self._coerce_text(device.get("id")) or self.domain_label
        state = self._coerce_text(device.get("state") or device.get("status")) or "unknown"
        details = device.get("details") if isinstance(device.get("details"), dict) else {}
        extras: List[str] = []
        metric_labels = {
            "apower": "power",
            "power": "power",
            "total_act_power": "power",
            "voltage": "voltage",
            "current": "current",
            "temperature": "temperature",
            "humidity": "humidity",
            "brightness": "brightness",
        }
        for key, label in metric_labels.items():
            value = details.get(key)
            if value not in (None, "", [], {}):
                extras.append(f"{label} {value}")
        if extras:
            return f"{name} is {state}; " + ", ".join(extras[:3]) + "."
        return f"{name} is {state}."

    def _list_summary(self, devices: List[dict], message: str, warnings: List[str]) -> str:
        if not devices:
            return f"No {self.domain_label_plural} were found."
        shown = ", ".join(
            f"{self._coerce_text(row.get('name')) or row.get('id')} ({self._coerce_text(row.get('state') or row.get('status') or 'unknown')})"
            for row in devices[:12]
        )
        suffix = ""
        if len(devices) > 12:
            suffix = f", and {len(devices) - 12} more"
        warn = f" {len(warnings)} host warning{'s' if len(warnings) != 1 else ''}." if warnings else ""
        return f"Found {len(devices)} {self.domain_label_plural}: {shown}{suffix}.{warn}"

    def _action_payload(self, action: str, intent: dict, device: dict) -> dict:
        details = device.get("details") if isinstance(device.get("details"), dict) else {}
        component = details.get("component") if isinstance(details.get("component"), dict) else {}
        payload = {
            "root_url": details.get("root_url"),
            "component_type": component.get("type") or device.get("type"),
            "component_id": component.get("id") or 0,
        }
        if action == "set_brightness" and intent.get("brightness") is not None:
            payload["brightness"] = intent.get("brightness")
        if action == "set_position" and intent.get("position") is not None:
            payload["position"] = intent.get("position")
        return {key: value for key, value in payload.items() if value not in (None, "")}

    def _already_in_target_state(self, action: str, device: dict) -> bool:
        state = self._coerce_text(device.get("state") or device.get("status")).lower()
        if action == "turn_on":
            return state == "on"
        if action == "turn_off":
            return state == "off"
        if action == "open":
            return state == "open"
        if action == "close":
            return state in {"closed", "close"}
        return False

    async def _handle(self, args, llm_client=None):
        payload = self._normalize_handler_args(args)
        query = self._coerce_text(payload.get("query") or payload.get("text") or payload.get("prompt"))
        if not query and self._coerce_text(payload.get("action")):
            query = self._coerce_text(payload.get("action"))
        if not query:
            return action_failure(
                code="missing_query",
                message=f"Please provide a {self.domain_label} request in query.",
                needs=[f"Ask for a {self.domain_label} action such as turn on, turn off, status, or list."],
                say_hint=f"Ask what {self.domain_label} action the user wants.",
            )

        intent = await self._interpret_query(payload, query, llm_client)
        action = self._coerce_text(intent.get("action")).lower()
        if action not in self.allowed_actions:
            return action_failure(
                code="missing_action",
                message=f"Please ask for a supported {self.domain_label} action.",
                needs=[f"Use a supported {self.domain_label} action and include the target."],
                say_hint=f"Ask which {self.domain_label} action the user wants.",
            )
        if action == "set_brightness" and intent.get("brightness") is None:
            return action_failure(
                code="missing_brightness",
                message="Shelly light brightness requires a percentage.",
                needs=["Include a brightness percentage from 0 to 100."],
                say_hint="Ask for the brightness percentage.",
            )
        if action == "set_position" and intent.get("position") is None:
            return action_failure(
                code="missing_position",
                message="Shelly cover position requires a percentage.",
                needs=["Include a cover position percentage from 0 to 100."],
                say_hint="Ask for the cover position percentage.",
            )

        module = self._get_module()
        if module is None:
            return action_failure(
                code="shelly_not_available",
                message="Shelly integration is not installed or enabled.",
                diagnosis=self._diagnosis(),
                needs=["Enable the Shelly integration in Tater Settings > Integrations."],
                say_hint="Explain the Shelly integration needs to be enabled before controlling devices.",
            )

        if action == "discover":
            action_fn = getattr(module, "run_integration_action", None)
            if not callable(action_fn):
                return action_failure(
                    code="shelly_discovery_unavailable",
                    message="The installed Shelly integration does not expose discovery.",
                    diagnosis=self._diagnosis(),
                    say_hint="Explain the Shelly integration needs to be updated.",
                )
            try:
                result = action_fn("discover", {})
            except Exception as exc:
                return action_failure(
                    code="shelly_discovery_failed",
                    message=f"Shelly discovery failed: {exc}",
                    diagnosis=self._diagnosis(),
                    needs=["Check Shelly integration settings, manual hosts, and local network access."],
                    say_hint="Explain Shelly discovery failed and suggest checking settings.",
                )
            return action_success(
                facts={"action": "discover", "device_count": result.get("device_count"), "host_count": result.get("host_count")},
                data=result,
                summary_for_user=self._coerce_text(result.get("message")) or "Shelly discovery finished.",
                say_hint="Report the Shelly discovery result briefly.",
            )

        try:
            devices, inventory_message, warnings = self._integration_devices(module)
        except Exception as exc:
            return action_failure(
                code="shelly_inventory_failed",
                message=f"Could not read Shelly devices: {exc}",
                diagnosis=self._diagnosis(),
                needs=["Check Shelly integration settings and device network reachability."],
                say_hint="Explain Shelly device lookup failed and suggest checking settings.",
            )

        if action == "list":
            return action_success(
                facts={"action": "list", "device_count": len(devices), "warnings": warnings},
                data={"devices": devices, "message": inventory_message, "warnings": warnings},
                summary_for_user=self._list_summary(devices, inventory_message, warnings),
                say_hint="List Shelly devices with their current states.",
            )

        if not devices:
            return action_failure(
                code="no_shelly_devices",
                message=f"No {self.domain_label_plural} were found.",
                diagnosis=self._diagnosis(),
                needs=["Add Shelly manual hosts or run discovery in Tater Settings > Integrations > Shelly."],
                say_hint=f"Explain no {self.domain_label_plural} were found and ask to configure or discover them.",
            )

        selected, needs = await self._find_device(
            devices=devices,
            payload=payload,
            query=query,
            intent=intent,
            llm_client=llm_client,
        )
        if not selected:
            return action_failure(
                code="device_selection_failed",
                message=f"Could not select a {self.domain_label} for this request.",
                needs=needs,
                say_hint=f"Ask which {self.domain_label} to use.",
            )

        if action == "status":
            return action_success(
                facts={
                    "action": "status",
                    "device_id": selected.get("id"),
                    "device_name": selected.get("name"),
                    "device_type": selected.get("type"),
                    "state": selected.get("state") or selected.get("status"),
                },
                data={"device": selected, "intent": intent},
                summary_for_user=self._device_summary(selected),
                say_hint="Report the Shelly device status briefly.",
            )

        if not self._action_supported(selected, action):
            return action_failure(
                code="unsupported_device_action",
                message=f"{selected.get('name') or selected.get('id')} does not support {self.action_labels.get(action, action)}.",
                needs=["Choose a Shelly device that supports this action."],
                say_hint="Explain that the selected Shelly device does not support the requested action.",
            )

        if self._already_in_target_state(action, selected):
            return action_success(
                facts={
                    "action": action,
                    "device_id": selected.get("id"),
                    "device_name": selected.get("name"),
                    "state": selected.get("state") or selected.get("status"),
                    "already_target_state": True,
                },
                data={"device": selected, "intent": intent},
                summary_for_user=f"{self._coerce_text(selected.get('name')) or self.domain_label} is already {self._coerce_text(selected.get('state') or selected.get('status'))}.",
                say_hint=f"Tell the user the {self.domain_label} is already in the requested state.",
            )

        action_fn = getattr(module, "run_integration_device_action", None)
        if not callable(action_fn):
            return action_failure(
                code="shelly_action_unavailable",
                message=f"The installed Shelly integration does not expose {self.domain_label} control.",
                diagnosis=self._diagnosis(),
                needs=["Update the Shelly integration."],
                say_hint="Explain the Shelly integration needs to be updated before control works.",
            )

        action_payload = self._action_payload(action, intent, selected)
        try:
            result = action_fn(action, self._coerce_text(selected.get("id")), action_payload)
        except Exception as exc:
            return action_failure(
                code="shelly_action_failed",
                message=f"Shelly failed to {self.action_labels.get(action, action)} {selected.get('name') or selected.get('id')}: {exc}",
                diagnosis=self._diagnosis(),
                needs=["Check that the Shelly device is online and retry."],
                say_hint="Explain the Shelly command failed and suggest checking the device.",
            )

        updated_device = None
        try:
            updated_devices, _message, _warnings = self._integration_devices(module)
            selected_id = self._coerce_text(selected.get("id"))
            updated_device = next(
                (row for row in updated_devices if self._coerce_text(row.get("id")) == selected_id),
                None,
            )
        except Exception:
            updated_device = None

        summary_device = updated_device if isinstance(updated_device, dict) else selected
        summary = self._device_summary(summary_device)
        if not updated_device:
            summary = f"Sent {self.action_labels.get(action, action)} to {self._coerce_text(selected.get('name')) or selected.get('id')}."

        return action_success(
            facts={
                "action": action,
                "device_id": selected.get("id"),
                "device_name": selected.get("name"),
                "device_type": selected.get("type"),
                "state": summary_device.get("state") or summary_device.get("status"),
            },
            data={
                "device": selected,
                "updated_device": updated_device,
                "intent": intent,
                "payload": action_payload,
                "result": result,
            },
            summary_for_user=summary,
            say_hint="Confirm the Shelly command and include the resulting state when available.",
        )

    async def handle_homeassistant(self, args=None, llm_client=None, *unused_args, **unused_kwargs):
        payload = self._normalize_handler_args(args)
        if not self._coerce_text(payload.get("query")):
            payload_query = self._coerce_text(unused_kwargs.get("query"))
            if payload_query:
                payload["query"] = payload_query
        return await self._handle(payload, llm_client)

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
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
        payload = self._normalize_handler_args(args)
        if not self._coerce_text(payload.get("query")):
            content = getattr(message, "content", None)
            if isinstance(content, str) and content.strip():
                payload["query"] = content.strip()
        return await self._handle(payload, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        payload = self._normalize_handler_args(args)
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

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        payload = self._normalize_handler_args(args)
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
        payload = self._normalize_handler_args(args)
        if not self._coerce_text(payload.get("query")) and isinstance(raw_message, str) and raw_message.strip():
            payload["query"] = raw_message.strip()
        return await self._handle(payload, llm_client)


verba = ShellyCoversPlugin()
