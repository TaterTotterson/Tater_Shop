import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

from helpers import extract_json, redis_client
from verba_base import ToolVerba
from verba_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from verba_result import action_failure, action_success

logger = logging.getLogger('ha_lights')
logger.setLevel(logging.INFO)


class HAClient:
    """Simple Home Assistant REST API helper (settings from Redis)."""

    def __init__(self):
        settings = redis_client.hgetall("homeassistant_settings") or {}
        self.base_url = (settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        self.token = (settings.get("HA_TOKEN") or "").strip()
        if not self.token:
            raise ValueError(
                "Home Assistant token is not set. Open WebUI -> Settings -> Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _req(self, method: str, path: str, json_body: Optional[dict] = None, timeout: int = 20):
        url = f"{self.base_url}{path}"
        resp = requests.request(method, url, headers=self.headers, json=json_body, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
        try:
            return resp.json()
        except Exception:
            return resp.text

    def list_states(self):
        return self._req("GET", "/api/states") or []

    def get_state(self, entity_id: str):
        return self._req("GET", f"/api/states/{entity_id}")

    def call_service(self, domain: str, service: str, payload: dict):
        return self._req("POST", f"/api/services/{domain}/{service}", json_body=payload)


class HALightsPlugin(ToolVerba):
    name = 'ha_lights'
    verba_name = 'Home Assistant Lights'
    version = '2.1.2'
    min_tater_version = "59"
    pretty_name = 'Home Assistant Lights'
    settings_category = "Home Assistant Control"
    platforms = ['homeassistant', 'webui', 'macos', 'xbmc', 'homekit', 'discord', 'telegram', 'matrix', 'irc']
    tags = ['homeassistant', 'light']

    forced_route = 'light'
    forced_domain_hint = 'light'

    usage = '{"function":"ha_lights","arguments":{"query":"turn off office lights"}}'
    description = 'Control one light or one area at a time: turn lights on/off and adjust brightness or color.'
    verba_dec = 'Control Home Assistant lights.'
    when_to_use = 'Use for light state checks, power control, brightness changes, and color changes.'
    how_to_use = 'Pass a natural-language light request with target plus action.'
    common_needs = ['Light/device and action (for example: office lights + turn off).']
    missing_info_prompts = ['Which light should I control?']
    example_calls = ['{"function":"ha_lights","arguments":{"query":"turn off office lights"}}', '{"function":"ha_lights","arguments":{"query":"set kitchen lights to 30 percent"}}', '{"function":"ha_lights","arguments":{"query":"set living room lights to blue"}}']

    entity_domains: List[str] = ['light']
    interpret_actions: List[str] = ['get_state', 'turn_on', 'turn_off', 'set_brightness', 'set_color']
    service_map: Dict[str, Optional[str]] = {'get_state': None, 'turn_on': 'turn_on', 'turn_off': 'turn_off', 'set_brightness': 'turn_on', 'set_color': 'turn_on'}
    required_action_params: Dict[str, List[str]] = {'set_brightness': ['brightness_pct']}
    optional_action_params: Dict[str, List[str]] = {
        'turn_on': ['brightness_pct', 'color_name', 'rgb_color', 'xy_color'],
        'set_color': ['color_name', 'rgb_color', 'xy_color'],
    }
    param_payload_keys: Dict[str, str] = {}
    summary_attribute_keys: List[str] = ['brightness', 'color_name', 'rgb_color', 'color_mode']
    interpret_focus: str = (
        "- Use set_brightness when user asks for percent brightness.\n"
        "- Use set_color when user asks for any color.\n"
        "- Preserve named colors as color_name (examples: crimson, chartreuse, warm white, deep sky blue).\n"
        "- For explicit rgb(...) or hex colors, set params.rgb_color as [r,g,b].\n"
    )
    interpret_examples: List[str] = [
        'turn off office lights -> action=turn_off, target=office lights, read_target=none',
        'set kitchen lights to 30 percent -> action=set_brightness, target=kitchen lights, params.brightness_pct=30, read_target=none',
        'set living room lights to blue -> action=set_color, target=living room lights, params.color_name=blue, read_target=none',
        'set bedroom lights to rgb(255, 120, 0) -> action=set_color, target=bedroom lights, params.rgb_color=[255,120,0], read_target=none',
        'set patio lights to #33ccff -> action=set_color, target=patio lights, params.rgb_color=[51,204,255], read_target=none',
    ]

    domain_read_only: bool = False
    include_binary_sensor: bool = False
    temperature_only: bool = False
    exclude_temperature: bool = False

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are controlling Home Assistant devices now. "
        "Only output that message."
    )

    required_settings = {
        "HA_MAX_CANDIDATES": {
            "label": "Max Entity Candidates",
            "type": "number",
            "default": 150,
            "description": "Maximum entity candidates sent to chooser LLM call.",
        },
    }

    @staticmethod
    def _coerce_text(value: Any) -> str:
        return str(value or "").strip()

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
        for key in ("verba_settings: Home Assistant Control", "verba_settings:Home Assistant Control"):
            merged.update(self._decode_map(redis_client.hgetall(key) or {}))
        return merged

    def _get_int_setting(self, key: str, default: int, minimum: int, maximum: int) -> int:
        raw = self._get_plugin_settings().get(key)
        try:
            val = int(float(str(raw).strip()))
        except Exception:
            val = int(default)
        return max(minimum, min(maximum, val))

    def _ha_diagnosis(self) -> dict:
        hash_diag = diagnose_hash_fields(
            "homeassistant_settings",
            fields={"ha_base_url": "HA_BASE_URL", "ha_token": "HA_TOKEN"},
            validators={
                "ha_base_url": lambda v: str(v or "").startswith("http://") or str(v or "").startswith("https://"),
                "ha_token": lambda v: len(str(v or "").strip()) >= 10,
            },
        )
        key_diag = diagnose_redis_keys(
            keys={"ha_base_url": "tater:homeassistant:base_url", "ha_token": "tater:homeassistant:token"},
            validators={
                "ha_base_url": lambda v: str(v or "").startswith("http://") or str(v or "").startswith("https://"),
                "ha_token": lambda v: len(str(v or "").strip()) >= 10,
            },
        )
        merged = dict(hash_diag)
        for k, v in (key_diag or {}).items():
            if merged.get(k) == "missing":
                merged[k] = v
        return combine_diagnosis(merged)

    def _get_client(self) -> Optional[HAClient]:
        try:
            return HAClient()
        except Exception as exc:
            logger.error("[%s] Failed to initialize HA client: %s", self.name, exc)
            return None

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
            blob = extract_json(content) or ""
            if not blob:
                return {}
            parsed = json.loads(blob)
            return parsed if isinstance(parsed, dict) else {}
        except Exception as exc:
            logger.debug("[%s] llm_json failed: %s", self.name, exc)
            return {}

    def _normalize_number(self, value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        try:
            return float(value)
        except Exception:
            text = self._coerce_text(value)
            if not text:
                return None
            try:
                return float(text)
            except Exception:
                return None

    def _normalize_int(self, value: Any) -> Optional[int]:
        n = self._normalize_number(value)
        if n is None:
            return None
        try:
            return int(round(n))
        except Exception:
            return None

    def _normalize_rgb_triplet(self, value: Any) -> Optional[List[int]]:
        if isinstance(value, (list, tuple)) and len(value) == 3:
            out: List[int] = []
            for item in value:
                num = self._normalize_int(item)
                if num is None:
                    return None
                out.append(max(0, min(255, int(num))))
            return out
        return None

    def _normalize_hex_color(self, value: Any) -> str:
        text = self._coerce_text(value).lower()
        if not text:
            return ""
        if text.startswith("hex "):
            text = text[4:].strip()
        if text.startswith("#"):
            text = text[1:]
        if len(text) == 3 and all(ch in "0123456789abcdef" for ch in text):
            text = "".join(ch * 2 for ch in text)
        if len(text) == 6 and all(ch in "0123456789abcdef" for ch in text):
            return text
        return ""

    def _hex_to_rgb(self, hex_color: str) -> Optional[List[int]]:
        token = self._normalize_hex_color(hex_color)
        if not token:
            return None
        try:
            return [int(token[0:2], 16), int(token[2:4], 16), int(token[4:6], 16)]
        except Exception:
            return None

    def _extract_rgb_from_text(self, text: str) -> Optional[List[int]]:
        source = self._coerce_text(text)
        if not source:
            return None
        patterns = [
            r"\brgb\s*\(\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\s*\)",
            r"\brgb\s*[:=]?\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\s*[, ]\s*(\d{1,3})\b",
        ]
        for pattern in patterns:
            match = re.search(pattern, source, flags=re.IGNORECASE)
            if not match:
                continue
            try:
                values = [max(0, min(255, int(match.group(i)))) for i in (1, 2, 3)]
                return values
            except Exception:
                continue
        return None

    def _extract_hex_from_text(self, text: str) -> str:
        source = self._coerce_text(text)
        if not source:
            return ""
        match = re.search(r"#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})\b", source)
        if not match:
            return ""
        return self._normalize_hex_color(match.group(1))

    def _normalize_color_name(self, value: Any) -> str:
        text = self._coerce_text(value).lower()
        if not text:
            return ""
        text = " ".join(text.split())
        return text

    def _normalize_display_color_label(self, value: Any) -> str:
        text = self._coerce_text(value).lower()
        if not text:
            return ""
        text = text.replace("_", " ").replace("/", " ")
        cleaned = "".join(ch for ch in text if ch.isalpha() or ch in {" ", "-"})
        cleaned = " ".join(cleaned.split())
        if not cleaned:
            return ""
        words = cleaned.split()
        if len(words) > 4:
            cleaned = " ".join(words[:4])
        return cleaned

    def _normalize_xy_pair(self, value: Any) -> Optional[List[float]]:
        if isinstance(value, (list, tuple)) and len(value) == 2:
            out: List[float] = []
            for item in value:
                n = self._normalize_number(item)
                if n is None:
                    return None
                out.append(max(0.0, min(1.0, float(n))))
            return [round(out[0], 6), round(out[1], 6)]
        return None

    async def _resolve_color_spec_llm(self, *, query: str, params: dict, llm_client) -> dict:
        if llm_client is None:
            return {}

        source_color_name = self._normalize_color_name((params or {}).get("color_name"))
        source_rgb = self._normalize_rgb_triplet((params or {}).get("rgb_color"))
        source_xy = self._normalize_xy_pair((params or {}).get("xy_color"))
        source_hex = self._normalize_hex_color((params or {}).get("hex_color"))
        if source_rgb is None and source_hex:
            source_rgb = self._hex_to_rgb(source_hex)

        system = (
            "Resolve a requested smart-light color for Home Assistant.\n"
            "Return STRICT JSON only:\n"
            "{\n"
            '  "color_name": "<short lowercase display name; may coin a blended name>",\n'
            '  "rgb_color": [<int 0-255>, <int 0-255>, <int 0-255>] or null,\n'
            '  "xy_color": [<float 0-1>, <float 0-1>] or null\n'
            "}\n"
            "Rules:\n"
            "- Resolve the user-requested color intent, including mixed phrases (example: bluish purple).\n"
            "- color_name is for user-facing confirmation and can be a coined blend (example: blurple).\n"
            "- If the user requested a plain canonical color name, keep it (example: purple -> purple).\n"
            "- Do not echo vague descriptor phrases (for example: bluish green, greenish blue, reddish purple).\n"
            "- Convert vague descriptors to a proper named hue when possible (examples: bluish purple -> violet, bluish green -> teal).\n"
            "- If no standard hue name fits well, create a short creative blend name.\n"
            "- Always return at least one of rgb_color or xy_color.\n"
            "- color_name must be 1-3 words and should not contain words ending in 'ish'.\n"
            "- Do not include prose.\n"
        )
        payload = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={
                "query": self._coerce_text(query),
                "source": {
                    "color_name": source_color_name,
                    "rgb_color": source_rgb,
                    "xy_color": source_xy,
                    "hex_color": source_hex,
                },
            },
            max_tokens=140,
            temperature=0.0,
        )
        if not isinstance(payload, dict):
            return {}

        color_name = self._normalize_display_color_label(payload.get("color_name"))
        rgb_color = self._normalize_rgb_triplet(payload.get("rgb_color"))
        xy_color = self._normalize_xy_pair(payload.get("xy_color"))

        if rgb_color is None:
            rgb_color = source_rgb
        if xy_color is None:
            xy_color = source_xy
        if not color_name:
            color_name = self._normalize_display_color_label(source_color_name)

        if rgb_color is None and xy_color is None:
            return {}

        return {
            "color_name": color_name,
            "rgb_color": rgb_color,
            "xy_color": xy_color,
        }

    def _is_temperature_row(self, row: dict) -> bool:
        if not isinstance(row, dict):
            return False
        device_class = self._coerce_text(row.get("device_class")).lower()
        unit = self._coerce_text(row.get("unit")).lower()
        if device_class == "temperature":
            return True
        if "°f" in unit or "°c" in unit:
            return True
        return False

    def _build_catalog(self, states: List[dict]) -> List[dict]:
        domains = {d.strip().lower() for d in (self.entity_domains or []) if str(d).strip()}
        if self.include_binary_sensor:
            domains.add("binary_sensor")

        rows: List[dict] = []
        seen: set[str] = set()
        for st in states or []:
            if not isinstance(st, dict):
                continue
            entity_id = self._coerce_text(st.get("entity_id")).lower()
            if not entity_id or entity_id in seen or "." not in entity_id:
                continue

            domain = entity_id.split(".", 1)[0]
            if domains and domain not in domains:
                continue

            attrs = st.get("attributes") if isinstance(st.get("attributes"), dict) else {}
            if not isinstance(attrs, dict):
                attrs = {}

            row = {
                "entity_id": entity_id,
                "domain": domain,
                "name": self._coerce_text(attrs.get("friendly_name")) or entity_id,
                "state": self._coerce_text(st.get("state")) or "unknown",
                "device_class": self._coerce_text(attrs.get("device_class")),
                "unit": self._coerce_text(attrs.get("unit_of_measurement")),
            }

            if self.temperature_only and not self._is_temperature_row(row):
                continue
            if self.exclude_temperature and self._is_temperature_row(row):
                continue

            rows.append(row)
            seen.add(entity_id)

        rows.sort(key=lambda r: (r.get("name") or "").lower())
        return rows

    def _interpret_system_prompt(self) -> str:
        actions = "|".join(self.interpret_actions or ["get_state"])
        examples_blob = "\n".join([f"- {x}" for x in (self.interpret_examples or []) if str(x).strip()])
        focus = self._coerce_text(self.interpret_focus)
        if focus:
            focus += "\n"

        return (
            f"You interpret Home Assistant requests for verba '{self.name}'.\n"
            "Return STRICT JSON only:\n"
            "{\n"
            f'  "action": "{actions}",\n'
            '  "target": "<short room/device phrase or empty>",\n'
            '  "read_target": "state|target_temperature|current_temperature|none",\n'
            '  "params": {\n'
            '    "temperature": <number or null>,\n'
            '    "brightness_pct": <int 0-100 or null>,\n'
            '    "color_name": "<string or empty>",\n'
            '    "rgb_color": [<int 0-255>, <int 0-255>, <int 0-255>] or null,\n'
            '    "xy_color": [<float 0-1>, <float 0-1>] or null,\n'
            '    "hex_color": "#RRGGBB" or "#RGB" or empty,\n'
            '    "percentage": <int 0-100 or null>,\n'
            '    "command": "<string or empty>",\n'
            '    "hvac_mode": "<string or empty>"\n'
            "  }\n"
            "}\n"
            "Rules:\n"
            f"- action must be one of: {actions}.\n"
            "- target is the user-mentioned room/device phrase when present.\n"
            "- Use read_target=state for status checks unless a specific climate target/current temperature read is requested.\n"
            "- For control actions use read_target=none.\n"
            f"{focus}"
            "Examples:\n"
            f"{examples_blob if examples_blob else '- none'}\n"
        )

    def _normalize_interpret_result(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            return {}

        action = self._coerce_text(payload.get("action")).lower()
        if action not in set(self.interpret_actions or []):
            return {}

        target = self._coerce_text(payload.get("target"))
        read_target = self._coerce_text(payload.get("read_target")).lower() or "state"
        params_in = payload.get("params") if isinstance(payload.get("params"), dict) else {}

        params: Dict[str, Any] = {}
        params["temperature"] = self._normalize_number(params_in.get("temperature"))

        brightness = self._normalize_int(params_in.get("brightness_pct"))
        if brightness is not None:
            brightness = max(0, min(100, brightness))
        params["brightness_pct"] = brightness

        percentage = self._normalize_int(params_in.get("percentage"))
        if percentage is not None:
            percentage = max(0, min(100, percentage))
        params["percentage"] = percentage

        params["color_name"] = self._normalize_color_name(params_in.get("color_name"))
        params["rgb_color"] = self._normalize_rgb_triplet(params_in.get("rgb_color"))
        params["xy_color"] = self._normalize_xy_pair(params_in.get("xy_color"))
        params["hex_color"] = self._normalize_hex_color(params_in.get("hex_color"))

        if params["rgb_color"] is None and params["hex_color"]:
            params["rgb_color"] = self._hex_to_rgb(params["hex_color"])

        if params["rgb_color"] is None:
            rgb_from_color_name = self._extract_rgb_from_text(params["color_name"])
            if rgb_from_color_name is not None:
                params["rgb_color"] = rgb_from_color_name
                params["color_name"] = ""
            else:
                hex_from_color_name = self._extract_hex_from_text(params["color_name"])
                if hex_from_color_name:
                    params["rgb_color"] = self._hex_to_rgb(hex_from_color_name)
                    params["hex_color"] = hex_from_color_name
                    params["color_name"] = ""

        params["command"] = self._coerce_text(params_in.get("command"))
        params["hvac_mode"] = self._coerce_text(params_in.get("hvac_mode")).lower()

        return {
            "action": action,
            "target": target,
            "read_target": read_target,
            "params": params,
        }

    async def _interpret_query(self, query: str, llm_client) -> dict:
        payload = await self._llm_json(
            llm_client=llm_client,
            system=self._interpret_system_prompt(),
            user_payload={"query": self._coerce_text(query)},
            max_tokens=260,
            temperature=0.0,
        )
        return self._normalize_interpret_result(payload)

    async def _choose_entity(self, *, query: str, intent: dict, catalog: List[dict], llm_client) -> str:
        if not catalog:
            return ""
        if len(catalog) == 1:
            return self._coerce_text(catalog[0].get("entity_id")).lower()

        max_candidates = self._get_int_setting("HA_MAX_CANDIDATES", 150, 5, 800)
        shortlist = catalog[:max_candidates]
        compact = [
            {
                "entity_id": row.get("entity_id"),
                "name": row.get("name"),
                "domain": row.get("domain"),
                "state": row.get("state"),
                "device_class": row.get("device_class"),
                "unit": row.get("unit"),
            }
            for row in shortlist
        ]
        valid_ids = {self._coerce_text(row.get("entity_id")).lower() for row in shortlist}

        system = (
            f"Choose the best Home Assistant entity for verba '{self.name}'.\n"
            "Return STRICT JSON only: {\"entity_id\":\"domain.name\"}.\n"
            "Rules:\n"
            "- Pick exactly one entity_id from candidates.\n"
            "- Use target phrase and action to pick the best match.\n"
            "- Do not invent entities.\n"
        )

        payload = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={"query": self._coerce_text(query), "intent": intent, "candidates": compact},
            max_tokens=220,
            temperature=0.0,
        )
        picked = self._coerce_text(payload.get("entity_id")).lower()
        if picked in valid_ids:
            return picked
        return ""

    def _action_param_value(self, params: dict, key: str) -> Any:
        if not isinstance(params, dict):
            return None
        return params.get(key)

    def _build_service_payload(self, *, action: str, params: dict) -> Tuple[Optional[str], dict, Optional[str]]:
        if action not in self.service_map:
            return None, {}, f"Unsupported action '{action}' for {self.name}."

        service = self.service_map.get(action)
        if service is None:
            return None, {}, None

        payload: Dict[str, Any] = {}
        for key in self.required_action_params.get(action, []):
            value = self._action_param_value(params, key)
            if value is None or (isinstance(value, str) and not value.strip()):
                return None, {}, f"Action '{action}' requires parameter '{key}'."
            payload_key = self.param_payload_keys.get(key, key)
            payload[payload_key] = value

        for key in self.optional_action_params.get(action, []):
            value = self._action_param_value(params, key)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            payload_key = self.param_payload_keys.get(key, key)
            payload[payload_key] = value

        if action == "set_color":
            color_name = self._coerce_text(payload.get("color_name"))
            rgb_color = payload.get("rgb_color")
            xy_color = payload.get("xy_color")
            has_rgb = isinstance(rgb_color, list) and len(rgb_color) == 3
            has_xy = isinstance(xy_color, list) and len(xy_color) == 2
            if not color_name and not has_rgb and not has_xy:
                return None, {}, "Action 'set_color' requires color_name, rgb_color, or xy_color."

        return service, payload, None

    def _build_service_payload_attempts(self, *, action: str, params: dict, payload: dict) -> List[dict]:
        base = dict(payload or {})
        if action not in {"set_color", "turn_on"}:
            return [base]

        rgb_color = self._normalize_rgb_triplet(base.get("rgb_color"))
        if rgb_color is None:
            rgb_color = self._normalize_rgb_triplet((params or {}).get("rgb_color"))

        xy_color = self._normalize_xy_pair(base.get("xy_color"))
        if xy_color is None:
            xy_color = self._normalize_xy_pair((params or {}).get("xy_color"))

        color_name = self._normalize_color_name(base.get("color_name"))
        if not color_name:
            color_name = self._normalize_color_name((params or {}).get("color_name"))

        core = {k: v for k, v in base.items() if k not in {"rgb_color", "xy_color", "color_name"}}
        attempts: List[dict] = []

        if isinstance(rgb_color, list) and len(rgb_color) == 3:
            p = dict(core)
            p["rgb_color"] = rgb_color
            attempts.append(p)
        if isinstance(xy_color, list) and len(xy_color) == 2:
            p = dict(core)
            p["xy_color"] = xy_color
            attempts.append(p)
        if color_name:
            p = dict(core)
            p["color_name"] = color_name
            attempts.append(p)

        if not attempts:
            return [base]

        unique: List[dict] = []
        seen: set[str] = set()
        for item in attempts:
            key = json.dumps(item, sort_keys=True, ensure_ascii=False)
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique

    def _state_summary(
        self,
        *,
        entity_id: str,
        state_payload: dict,
        read_target: str,
        color_label: str = "",
    ) -> str:
        st = state_payload if isinstance(state_payload, dict) else {}
        attrs = st.get("attributes") if isinstance(st.get("attributes"), dict) else {}
        if not isinstance(attrs, dict):
            attrs = {}

        name = self._coerce_text(attrs.get("friendly_name")) or entity_id
        state = self._coerce_text(st.get("state")) or "unknown"
        unit = self._coerce_text(attrs.get("unit_of_measurement"))

        read_target = self._coerce_text(read_target).lower()
        if read_target == "target_temperature":
            value = attrs.get("temperature")
            if value is not None:
                return f"{name} target temperature is {value}{(' ' + unit) if unit else ''}."
        if read_target == "current_temperature":
            value = attrs.get("current_temperature")
            if value is not None:
                return f"{name} current temperature is {value}{(' ' + unit) if unit else ''}."

        if self.temperature_only:
            return f"{name} is {state}{(' ' + unit) if unit else ''}."

        extras: List[str] = []
        brightness_raw = self._normalize_int(attrs.get("brightness"))
        if brightness_raw is not None:
            brightness_raw = max(0, min(255, int(brightness_raw)))
            pct = int(round((brightness_raw / 255.0) * 100.0))
            extras.append(f"brightness {pct}%")

        llm_label = self._coerce_text(color_label).lower()
        if llm_label:
            extras.append(f"color {llm_label}")

        if extras:
            return f"{name} is {state}; " + ", ".join(extras) + "."
        return f"{name} is {state}."

    def _sanitize_state_for_hydra(
        self,
        *,
        entity_id: str,
        state_payload: dict,
        color_label: str = "",
    ) -> dict:
        st = state_payload if isinstance(state_payload, dict) else {}
        attrs = st.get("attributes") if isinstance(st.get("attributes"), dict) else {}
        if not isinstance(attrs, dict):
            attrs = {}

        safe_attrs: Dict[str, Any] = {}
        friendly = self._coerce_text(attrs.get("friendly_name"))
        if friendly:
            safe_attrs["friendly_name"] = friendly

        brightness_raw = self._normalize_int(attrs.get("brightness"))
        if brightness_raw is not None:
            brightness_raw = max(0, min(255, int(brightness_raw)))
            safe_attrs["brightness_pct"] = int(round((brightness_raw / 255.0) * 100.0))

        llm_label = self._coerce_text(color_label).lower()
        if llm_label:
            safe_attrs["color"] = llm_label

        color_mode = self._coerce_text(attrs.get("color_mode"))
        if color_mode:
            safe_attrs["color_mode"] = color_mode

        return {
            "entity_id": self._coerce_text(st.get("entity_id")) or entity_id,
            "state": self._coerce_text(st.get("state")) or "unknown",
            "attributes": safe_attrs,
        }

    def _ensure_llm_available(self, llm_client) -> Optional[dict]:
        if llm_client is not None:
            return None
        return action_failure(
            code="llm_unavailable",
            message="This Home Assistant verba requires LLM parsing for natural-language requests.",
            needs=["Try again when the model is reachable."],
            say_hint="Explain the model was unavailable and retry once model connectivity returns.",
        )

    async def _handle(self, args, llm_client):
        args = args or {}
        query = self._coerce_text(args.get("query"))
        if not query:
            return action_failure(
                code="missing_query",
                message=f"Please provide a {self.pretty_name or self.name} request in query.",
                needs=[f"What should I do with {self.pretty_name or self.name}?"],
                say_hint="Ask for the Home Assistant request in natural language.",
            )

        llm_missing = self._ensure_llm_available(llm_client)
        if llm_missing:
            return llm_missing

        client = self._get_client()
        if not client:
            diagnosis = self._ha_diagnosis()
            needs = needs_from_diagnosis(
                diagnosis,
                {
                    "ha_base_url": "Please set your Home Assistant base URL in settings.",
                    "ha_token": "Please set your Home Assistant Long-Lived Access Token.",
                },
            )
            return action_failure(
                code="ha_not_configured",
                message="Home Assistant connection is not configured.",
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain Home Assistant settings are missing and ask to configure them.",
            )

        try:
            states = client.list_states()
        except Exception as exc:
            return action_failure(
                code="ha_states_failed",
                message=f"Could not read Home Assistant states: {exc}",
                diagnosis=self._ha_diagnosis(),
                say_hint="Explain Home Assistant state fetch failed and suggest retrying.",
            )

        catalog = self._build_catalog(states)
        if not catalog:
            domains = ", ".join(self.entity_domains or [])
            return action_failure(
                code="no_entities",
                message=f"No Home Assistant entities were found for {domains or self.name}.",
                needs=["Expose matching entities in Home Assistant and try again."],
                say_hint="Explain no matching entities were available for this verba.",
            )

        intent = await self._interpret_query(query, llm_client)
        if not intent:
            return action_failure(
                code="interpret_failed",
                message="Could not interpret this request for this Home Assistant verba.",
                needs=["Try rephrasing with a clearer action and room/device target."],
                say_hint="Ask for clearer wording and keep focus on this verba's domain.",
            )

        action = self._coerce_text(intent.get("action")).lower()
        if self.domain_read_only and action != "get_state":
            return action_failure(
                code="read_only",
                message=f"{self.pretty_name or self.name} is read-only and only supports state checks.",
                needs=["Ask for a state check instead of a control action."],
                say_hint="Explain this verba is read-only and suggest a state-check phrasing.",
            )

        entity_id = self._coerce_text(args.get("entity_id")).lower()
        valid_ids = {self._coerce_text(row.get("entity_id")).lower() for row in catalog}
        if entity_id:
            if entity_id not in valid_ids:
                return action_failure(
                    code="invalid_entity_id",
                    message=f"Entity '{entity_id}' is not available for {self.name}.",
                    needs=["Use a valid entity_id in this Home Assistant domain."],
                    say_hint="Explain provided entity_id is invalid for this verba.",
                )
        else:
            entity_id = await self._choose_entity(query=query, intent=intent, catalog=catalog, llm_client=llm_client)
            if not entity_id:
                return action_failure(
                    code="entity_selection_failed",
                    message="Could not select an entity for this request.",
                    needs=["Specify the room/device more clearly in your request."],
                    say_hint="Ask for a clearer device target.",
                )

        selected = next((row for row in catalog if self._coerce_text(row.get("entity_id")).lower() == entity_id), None)
        if not isinstance(selected, dict):
            return action_failure(
                code="entity_not_found",
                message=f"Selected entity '{entity_id}' was not found.",
                say_hint="Explain entity lookup failed and suggest retrying.",
            )

        read_target = self._coerce_text(intent.get("read_target")).lower() or "state"
        params = intent.get("params") if isinstance(intent.get("params"), dict) else {}
        wants_color_resolution = (
            action == "set_color"
            or bool(self._coerce_text(params.get("color_name")))
            or self._normalize_rgb_triplet(params.get("rgb_color")) is not None
            or self._normalize_xy_pair(params.get("xy_color")) is not None
            or bool(self._normalize_hex_color(params.get("hex_color")))
        )
        if wants_color_resolution:
            resolved_color = await self._resolve_color_spec_llm(query=query, params=params, llm_client=llm_client)
            if not resolved_color and action == "set_color":
                return action_failure(
                    code="color_resolution_failed",
                    message="Could not resolve a concrete light color (rgb/xy) from this request.",
                    needs=["Rephrase the desired color (for example: violet, blurple, rgb(120,80,255), or #7f5cff)."],
                    say_hint="Ask for a clearer color target because color resolution failed.",
                )
            if resolved_color:
                params["color_name"] = self._normalize_display_color_label(resolved_color.get("color_name"))
                params["rgb_color"] = self._normalize_rgb_triplet(resolved_color.get("rgb_color"))
                params["xy_color"] = self._normalize_xy_pair(resolved_color.get("xy_color"))

        requested_color_label = self._normalize_display_color_label(params.get("color_name"))

        service, service_payload, payload_error = self._build_service_payload(action=action, params=params)
        if payload_error:
            return action_failure(
                code="invalid_action_parameters",
                message=payload_error,
                needs=["Retry and include the missing parameter in your request."],
                say_hint="Explain which required parameter is missing.",
            )

        if service is None:
            try:
                state = client.get_state(entity_id)
                state_dict = state if isinstance(state, dict) else {}
                attrs = state_dict.get("attributes") if isinstance(state_dict.get("attributes"), dict) else {}
                color_label = self._normalize_display_color_label(attrs.get("color_name")) if isinstance(attrs, dict) else ""
                summary = self._state_summary(
                    entity_id=entity_id,
                    state_payload=state_dict,
                    read_target=read_target,
                    color_label=color_label,
                )
                return action_success(
                    facts={
                        "action": "get_state",
                        "entity_id": entity_id,
                        "domain": selected.get("domain"),
                        "read_target": read_target,
                    },
                    data={
                        "entity_id": entity_id,
                        "state": state,
                        "intent": intent,
                    },
                    summary_for_user=summary,
                    say_hint="Report the Home Assistant state from returned data.",
                )
            except Exception as exc:
                return action_failure(
                    code="state_read_failed",
                    message=f"Could not read state for {entity_id}: {exc}",
                    diagnosis=self._ha_diagnosis(),
                    say_hint="Explain state read failed and suggest retrying.",
                )

        payload = {"entity_id": entity_id}
        if isinstance(service_payload, dict):
            payload.update(service_payload)

        domain = self._coerce_text(selected.get("domain"))
        call_error: Optional[Exception] = None
        attempts = self._build_service_payload_attempts(action=action, params=params, payload=payload)
        for candidate in attempts:
            try:
                client.call_service(domain, service, candidate)
                payload = candidate
                call_error = None
                break
            except Exception as exc:
                call_error = exc

        if call_error is not None:
            return action_failure(
                code="service_call_failed",
                message=f"Home Assistant {domain}.{service} failed for {entity_id}: {call_error}",
                diagnosis=self._ha_diagnosis(),
                needs=["Retry or choose a different entity target."],
                say_hint="Explain service call failed and ask whether to retry.",
            )

        display_name = self._coerce_text(selected.get("name")) or entity_id
        summary = f"Completed light update for {display_name}."
        if requested_color_label:
            summary = f"Completed light update for {display_name}; color {requested_color_label}."
        state_after: Dict[str, Any] = {}
        try:
            raw_state = client.get_state(entity_id)
            if isinstance(raw_state, dict):
                color_label = requested_color_label
                if not color_label:
                    attrs = raw_state.get("attributes") if isinstance(raw_state.get("attributes"), dict) else {}
                    color_label = self._normalize_display_color_label(attrs.get("color_name")) if isinstance(attrs, dict) else ""
                state_after = self._sanitize_state_for_hydra(
                    entity_id=entity_id,
                    state_payload=raw_state,
                    color_label=color_label,
                )
        except Exception:
            pass

        return action_success(
            facts={
                "action": action,
                "service": service,
                "entity_id": entity_id,
                "domain": selected.get("domain"),
            },
            data={
                "entity_id": entity_id,
                "service": service,
                "domain": selected.get("domain"),
                "result": "completed",
                "state_after": state_after,
            },
            summary_for_user=summary,
            say_hint="Reply briefly that the light command is complete. Do not include rgb/xy/hs/internal attribute dumps.",
        )

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)

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

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        payload = dict(args or {})
        if not self._coerce_text(payload.get("query")) and isinstance(raw_message, str) and raw_message.strip():
            payload["query"] = raw_message.strip()
        return await self._handle(payload, llm_client)


verba = HALightsPlugin()
