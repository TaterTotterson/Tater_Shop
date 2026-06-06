import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from helpers import extract_json, redis_client
from tateros import integration_store as integration_store_module
from verba_base import ToolVerba
from verba_diagnostics import needs_from_diagnosis
from verba_result import action_failure, action_success

logger = logging.getLogger("hue_lights")
logger.setLevel(logging.INFO)

HUE_DEFAULT_BRIDGE_HOST = "http://philips-hue.local"
HUE_DEFAULT_DEVICE_TYPE = "tater_shop#tater"
HUE_DEFAULT_TIMEOUT_SECONDS = 10
PLUGIN_SETTINGS_KEYS = (
    "verba_settings:Philips Hue Control",
    "verba_settings: Philips Hue Control",
)


def _hue_module(*, required: bool = True):
    module = integration_store_module.integration_module("hue")
    if module is None and required:
        raise RuntimeError("Philips Hue integration is not enabled.")
    return module


def normalize_hue_bridge_root(raw_host: Any) -> str:
    module = _hue_module(required=False)
    if module is not None:
        return module.normalize_hue_bridge_root(raw_host)
    text = _coerce_text(raw_host) or HUE_DEFAULT_BRIDGE_HOST
    if "://" not in text:
        text = f"http://{text}"
    parsed = urlparse(text)
    if not parsed.netloc and parsed.path:
        parsed = urlparse(f"http://{text}")
    scheme = parsed.scheme or "http"
    netloc = parsed.netloc or parsed.path
    root = f"{scheme}://{netloc}".rstrip("/")
    return root or HUE_DEFAULT_BRIDGE_HOST


def read_hue_settings(*args, **kwargs) -> Dict[str, Any]:
    module = _hue_module(required=False)
    if module is not None:
        return module.read_hue_settings(*args, **kwargs)
    return {
        "HUE_BRIDGE_HOST": HUE_DEFAULT_BRIDGE_HOST,
        "HUE_APP_KEY": "",
        "HUE_DEVICE_TYPE": HUE_DEFAULT_DEVICE_TYPE,
        "HUE_TIMEOUT_SECONDS": str(HUE_DEFAULT_TIMEOUT_SECONDS),
    }


def pair_hue_bridge(*args, **kwargs):
    return _hue_module().pair_hue_bridge(*args, **kwargs)


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


def _read_plugin_settings() -> dict:
    merged: dict = read_hue_settings()
    for key in PLUGIN_SETTINGS_KEYS:
        try:
            decoded = _decode_map(redis_client.hgetall(key) or {})
        except Exception:
            continue
        for field, value in decoded.items():
            if field != "HUE_MAX_CANDIDATES":
                continue
            if _coerce_text(value) or field not in merged:
                merged[field] = value
    return merged


def _as_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
    try:
        number = int(float(str(value).strip()))
    except Exception:
        number = int(default)
    return max(int(minimum), min(int(maximum), number))


def _normalize_rgb(value: Any) -> Optional[List[int]]:
    if not isinstance(value, (list, tuple)) or len(value) != 3:
        return None
    out: List[int] = []
    for item in value:
        try:
            out.append(max(0, min(255, int(round(float(item))))))
        except Exception:
            return None
    return out


def _normalize_xy(value: Any) -> Optional[List[float]]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        return None
    out: List[float] = []
    for item in value:
        try:
            out.append(max(0.0, min(1.0, float(item))))
        except Exception:
            return None
    return [round(out[0], 4), round(out[1], 4)]


def _rgb_to_xy(rgb: List[int]) -> Optional[List[float]]:
    normalized = _normalize_rgb(rgb)
    if normalized is None:
        return None

    def gamma(channel: int) -> float:
        c = channel / 255.0
        if c > 0.04045:
            return ((c + 0.055) / 1.055) ** 2.4
        return c / 12.92

    r, g, b = [gamma(v) for v in normalized]
    x_val = r * 0.664511 + g * 0.154324 + b * 0.162028
    y_val = r * 0.283881 + g * 0.668433 + b * 0.047685
    z_val = r * 0.000088 + g * 0.072310 + b * 0.986039
    total = x_val + y_val + z_val
    if total <= 0:
        return None
    return [round(x_val / total, 4), round(y_val / total, 4)]


COLOR_NAME_RGB: Dict[str, List[int]] = {
    "red": [255, 0, 0],
    "orange": [255, 128, 0],
    "amber": [255, 191, 0],
    "yellow": [255, 255, 0],
    "green": [0, 180, 0],
    "lime": [128, 255, 0],
    "blue": [0, 70, 255],
    "cyan": [0, 255, 255],
    "teal": [0, 128, 128],
    "purple": [128, 0, 255],
    "violet": [143, 0, 255],
    "magenta": [255, 0, 255],
    "pink": [255, 105, 180],
    "rose": [255, 64, 129],
    "white": [255, 244, 229],
    "warm white": [255, 197, 143],
    "soft white": [255, 214, 170],
    "cool white": [214, 225, 255],
    "daylight": [220, 235, 255],
    "gold": [255, 184, 28],
    "crimson": [220, 20, 60],
    "chartreuse": [127, 255, 0],
}


def _color_name_to_xy(color_name: Any) -> Optional[List[float]]:
    text = " ".join(_coerce_text(color_name).lower().replace("_", " ").split())
    if not text:
        return None
    if text in COLOR_NAME_RGB:
        return _rgb_to_xy(COLOR_NAME_RGB[text])
    compact = re.sub(r"[^a-z0-9]+", " ", text).strip()
    if compact in COLOR_NAME_RGB:
        return _rgb_to_xy(COLOR_NAME_RGB[compact])
    for name, rgb in COLOR_NAME_RGB.items():
        if name in text:
            return _rgb_to_xy(rgb)
    return None


class HueClient:
    """Philips Hue Bridge v1 API adapter shaped like the HA light client."""

    def __init__(self, *, require_app_key: bool = True):
        settings = read_hue_settings()
        self.base_url = normalize_hue_bridge_root(settings.get("HUE_BRIDGE_HOST") or HUE_DEFAULT_BRIDGE_HOST)
        self.app_key = _coerce_text(settings.get("HUE_APP_KEY"))
        self.device_type = (
            _coerce_text(settings.get("HUE_DEVICE_TYPE")) or HUE_DEFAULT_DEVICE_TYPE
        )[:40]
        self.timeout = _as_int(
            settings.get("HUE_TIMEOUT_SECONDS"),
            HUE_DEFAULT_TIMEOUT_SECONDS,
            minimum=2,
            maximum=60,
        )
        if require_app_key and not self.app_key:
            raise ValueError("Philips Hue app key is missing. Press the bridge link button, then click Link Hue Bridge.")

    @classmethod
    def pair_bridge(cls) -> str:
        client = cls(require_app_key=False)
        result = pair_hue_bridge(
            bridge_host=client.base_url,
            device_type=client.device_type,
            timeout_seconds=client.timeout,
        )
        return _coerce_text(result.get("message")) or "Hue Bridge pairing finished."

    def _raise_hue_errors(self, payload: Any) -> None:
        errors: List[str] = []
        for item in payload if isinstance(payload, list) else [payload]:
            if isinstance(item, dict) and isinstance(item.get("error"), dict):
                err = item["error"]
                desc = _coerce_text(err.get("description")) or json.dumps(err, ensure_ascii=False)
                errors.append(desc)
        if errors:
            raise RuntimeError("; ".join(errors))

    def _req(self, method: str, path: str, json_body: Optional[dict] = None):
        url = f"{self.base_url}{path}"
        response = requests.request(method, url, json=json_body, timeout=self.timeout)
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
        try:
            payload = response.json()
        except Exception:
            payload = response.text
        self._raise_hue_errors(payload)
        return payload

    @staticmethod
    def _bri_to_255(value: Any) -> Optional[int]:
        try:
            bri = int(round(float(value)))
        except Exception:
            return None
        return max(0, min(255, int(round((max(0, min(254, bri)) / 254.0) * 255.0))))

    def _light_state(self, light_id: str, light: dict) -> dict:
        state = light.get("state") if isinstance(light.get("state"), dict) else {}
        attrs: Dict[str, Any] = {
            "friendly_name": _coerce_text(light.get("name")) or f"Hue light {light_id}",
            "hue_id": str(light_id),
            "hue_resource": "light",
        }
        bri = self._bri_to_255(state.get("bri"))
        if bri is not None:
            attrs["brightness"] = bri
        if isinstance(state.get("xy"), list):
            attrs["xy_color"] = state.get("xy")
        if _coerce_text(state.get("colormode")):
            attrs["color_mode"] = _coerce_text(state.get("colormode"))
        if "reachable" in state:
            attrs["reachable"] = bool(state.get("reachable"))
        if _coerce_text(light.get("type")):
            attrs["hue_type"] = _coerce_text(light.get("type"))
        return {
            "entity_id": f"hue.{light_id}",
            "state": "on" if bool(state.get("on")) else "off",
            "attributes": attrs,
        }

    def _group_state(self, group_id: str, group: dict) -> dict:
        state = group.get("state") if isinstance(group.get("state"), dict) else {}
        action = group.get("action") if isinstance(group.get("action"), dict) else {}
        group_type = _coerce_text(group.get("type"))
        name = _coerce_text(group.get("name")) or f"Hue group {group_id}"
        attrs: Dict[str, Any] = {
            "friendly_name": name,
            "hue_id": str(group_id),
            "hue_resource": "group",
        }
        if group_type:
            attrs["hue_type"] = group_type
        bri = self._bri_to_255(action.get("bri"))
        if bri is not None:
            attrs["brightness"] = bri
        if isinstance(action.get("xy"), list):
            attrs["xy_color"] = action.get("xy")
        if _coerce_text(action.get("colormode")):
            attrs["color_mode"] = _coerce_text(action.get("colormode"))
        is_on = bool(action.get("on") or state.get("any_on") or state.get("all_on"))
        return {
            "entity_id": f"hue_group.{group_id}",
            "state": "on" if is_on else "off",
            "attributes": attrs,
        }

    def list_states(self):
        rows: List[dict] = []
        lights = self._req("GET", f"/api/{self.app_key}/lights") or {}
        if isinstance(lights, dict):
            for light_id, light in lights.items():
                if isinstance(light, dict):
                    rows.append(self._light_state(str(light_id), light))

        try:
            groups = self._req("GET", f"/api/{self.app_key}/groups") or {}
        except Exception as exc:
            logger.debug("[hue_lights] group lookup skipped: %s", exc)
            groups = {}
        if isinstance(groups, dict):
            for group_id, group in groups.items():
                if isinstance(group, dict) and _coerce_text(group.get("type")).lower() in {"room", "zone", "lightgroup"}:
                    rows.append(self._group_state(str(group_id), group))
        return rows

    def get_state(self, entity_id: str):
        resource, hue_id = self._parse_entity_id(entity_id)
        if resource == "group":
            group = self._req("GET", f"/api/{self.app_key}/groups/{hue_id}") or {}
            return self._group_state(hue_id, group if isinstance(group, dict) else {})
        light = self._req("GET", f"/api/{self.app_key}/lights/{hue_id}") or {}
        return self._light_state(hue_id, light if isinstance(light, dict) else {})

    def _parse_entity_id(self, entity_id: Any) -> Tuple[str, str]:
        text = _coerce_text(entity_id).lower()
        if text.startswith("hue_group."):
            return "group", text.split(".", 1)[1]
        if text.startswith("hue."):
            return "light", text.split(".", 1)[1]
        return "light", text

    def _state_payload(self, service: str, payload: dict) -> dict:
        if service == "turn_off":
            return {"on": False}

        body: Dict[str, Any] = {"on": True}
        brightness = payload.get("brightness_pct") if isinstance(payload, dict) else None
        if brightness is not None:
            pct = _as_int(brightness, 100, minimum=0, maximum=100)
            if pct <= 0:
                return {"on": False}
            body["bri"] = max(1, min(254, int(round((pct / 100.0) * 254.0))))

        xy = _normalize_xy(payload.get("xy_color")) if isinstance(payload, dict) else None
        if xy is None:
            rgb = _normalize_rgb(payload.get("rgb_color")) if isinstance(payload, dict) else None
            if rgb is not None:
                xy = _rgb_to_xy(rgb)
        if xy is None and isinstance(payload, dict):
            xy = _color_name_to_xy(payload.get("color_name"))
        if xy is not None:
            body["xy"] = xy

        return body

    def call_service(self, domain: str, service: str, payload: dict):
        resource, hue_id = self._parse_entity_id((payload or {}).get("entity_id"))
        state_payload = self._state_payload(service, payload or {})
        if resource == "group" or _coerce_text(domain).lower() == "hue_group":
            return self._req("PUT", f"/api/{self.app_key}/groups/{hue_id}/action", json_body=state_payload)
        return self._req("PUT", f"/api/{self.app_key}/lights/{hue_id}/state", json_body=state_payload)


class HueLightsPlugin(ToolVerba):
    name = "hue_lights"
    verba_name = "Philips Hue Lights"
    version = "1.0.4"
    min_tater_version = "59"
    pretty_name = "Philips Hue Lights"
    settings_category = "Philips Hue Control"
    platforms = ["voice_core", "homeassistant", "webui", "little_spud", "macos", "xbmc", "homekit", "discord", "telegram", "matrix", "irc", "meshtastic"]
    tags = ["hue", "philips-hue", "light"]
    routing_keywords = ["hue", "philips hue", "hue bridge", "hue lights"]
    forced_route = "hue_light"
    forced_domain_hint = "philips hue light"

    usage = '{"function":"hue_lights","arguments":{"query":"turn off the office Hue lights"}}'
    description = "Control Philips Hue Bridge lights, rooms, and zones: on/off, brightness, and color."
    verba_dec = "Control Philips Hue Bridge lights."
    when_to_use = "Use for Hue Bridge light state checks, power control, brightness changes, and color changes."
    how_to_use = "Pass a natural-language Hue light request with target plus action."
    common_needs = ["Hue light/room/zone and action (for example: office Hue lights + turn off)."]
    missing_info_prompts = ["Which Hue light, room, or zone should I control?"]
    example_calls = [
        '{"function":"hue_lights","arguments":{"query":"turn off the office Hue lights"}}',
        '{"function":"hue_lights","arguments":{"query":"set kitchen Hue lights to 30 percent"}}',
        '{"function":"hue_lights","arguments":{"query":"set living room Hue lights to blue"}}',
    ]

    entity_domains: List[str] = ["hue", "hue_group"]
    interpret_actions: List[str] = ["get_state", "turn_on", "turn_off", "set_brightness", "set_color"]
    service_map: Dict[str, Optional[str]] = {
        "get_state": None,
        "turn_on": "turn_on",
        "turn_off": "turn_off",
        "set_brightness": "turn_on",
        "set_color": "turn_on",
    }
    required_action_params: Dict[str, List[str]] = {"set_brightness": ["brightness_pct"]}
    optional_action_params: Dict[str, List[str]] = {
        "turn_on": ["brightness_pct", "color_name", "rgb_color", "xy_color"],
        "set_color": ["color_name", "rgb_color", "xy_color"],
    }
    param_payload_keys: Dict[str, str] = {}
    interpret_focus: str = (
        "- Use set_brightness when user asks for percent brightness.\n"
        "- Use set_color when user asks for any color.\n"
        "- Preserve named colors as color_name when available.\n"
        "- For explicit rgb(...) or hex colors, set params.rgb_color as [r,g,b].\n"
    )
    interpret_examples: List[str] = [
        "turn off office Hue lights -> action=turn_off, target=office Hue lights, read_target=none",
        "set kitchen Hue lights to 30 percent -> action=set_brightness, target=kitchen Hue lights, params.brightness_pct=30, read_target=none",
        "set living room Hue lights to blue -> action=set_color, target=living room Hue lights, params.color_name=blue, read_target=none",
        "set bedroom Hue bulb to rgb(255, 120, 0) -> action=set_color, target=bedroom Hue bulb, params.rgb_color=[255,120,0], read_target=none",
        "set patio Hue zone to #33ccff -> action=set_color, target=patio Hue zone, params.rgb_color=[51,204,255], read_target=none",
    ]

    required_settings = {
        "HUE_MAX_CANDIDATES": {
            "label": "Max Hue Candidates",
            "type": "number",
            "default": 150,
            "description": "Maximum Hue lights/groups sent to chooser LLM call.",
        },
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are controlling Philips Hue lights now. "
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
        number = self._normalize_number(value)
        if number is None:
            return None
        try:
            return int(round(number))
        except Exception:
            return None

    def _normalize_rgb_triplet(self, value: Any) -> Optional[List[int]]:
        if isinstance(value, (list, tuple)) and len(value) == 3:
            out: List[int] = []
            for item in value:
                number = self._normalize_int(item)
                if number is None:
                    return None
                out.append(max(0, min(255, int(number))))
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
                return [max(0, min(255, int(match.group(i)))) for i in (1, 2, 3)]
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
        return " ".join(text.split()) if text else ""

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
                number = self._normalize_number(item)
                if number is None:
                    return None
                out.append(max(0.0, min(1.0, float(number))))
            return [round(out[0], 6), round(out[1], 6)]
        return None

    async def _resolve_color_spec_llm(self, *, query: str, params: dict, llm_client) -> dict:
        source_color_name = self._normalize_color_name((params or {}).get("color_name"))
        source_rgb = self._normalize_rgb_triplet((params or {}).get("rgb_color"))
        source_xy = self._normalize_xy_pair((params or {}).get("xy_color"))
        source_hex = self._normalize_hex_color((params or {}).get("hex_color"))
        if source_rgb is None and source_hex:
            source_rgb = self._hex_to_rgb(source_hex)

        if llm_client is None:
            xy = source_xy or (_rgb_to_xy(source_rgb) if source_rgb else _color_name_to_xy(source_color_name))
            if source_rgb is None and xy is None:
                return {}
            return {
                "color_name": self._normalize_display_color_label(source_color_name),
                "rgb_color": source_rgb,
                "xy_color": xy,
            }

        system = (
            "Resolve a requested Philips Hue light color.\n"
            "Return STRICT JSON only:\n"
            "{\n"
            '  "color_name": "<short lowercase display name>",\n'
            '  "rgb_color": [<int 0-255>, <int 0-255>, <int 0-255>] or null,\n'
            '  "xy_color": [<float 0-1>, <float 0-1>] or null\n'
            "}\n"
            "Rules:\n"
            "- Resolve mixed or fuzzy color phrases to a concrete light color.\n"
            "- Always return at least one of rgb_color or xy_color.\n"
            "- color_name must be 1-3 words and should be user-friendly.\n"
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
        rgb_color = self._normalize_rgb_triplet(payload.get("rgb_color")) or source_rgb
        xy_color = self._normalize_xy_pair(payload.get("xy_color")) or source_xy
        if xy_color is None and rgb_color is not None:
            xy_color = _rgb_to_xy(rgb_color)
        if xy_color is None and color_name:
            xy_color = _color_name_to_xy(color_name)
        if not color_name:
            color_name = self._normalize_display_color_label(source_color_name)

        if rgb_color is None and xy_color is None:
            return {}

        return {"color_name": color_name, "rgb_color": rgb_color, "xy_color": xy_color}

    def _normalize_interpret_result(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            return {}

        action = self._coerce_text(payload.get("action")).lower()
        if action not in set(self.interpret_actions or []):
            return {}

        params_in = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        params: Dict[str, Any] = {}

        brightness = self._normalize_int(params_in.get("brightness_pct"))
        if brightness is not None:
            brightness = max(0, min(100, brightness))
        params["brightness_pct"] = brightness

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

        return {
            "action": action,
            "target": self._coerce_text(payload.get("target")),
            "read_target": self._coerce_text(payload.get("read_target")).lower() or "state",
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
            payload[self.param_payload_keys.get(key, key)] = value

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
        if xy_color is None and rgb_color is not None:
            xy_color = _rgb_to_xy(rgb_color)

        color_name = self._normalize_color_name(base.get("color_name"))
        if not color_name:
            color_name = self._normalize_color_name((params or {}).get("color_name"))
        if xy_color is None and color_name:
            xy_color = _color_name_to_xy(color_name)

        core = {k: v for k, v in base.items() if k not in {"rgb_color", "xy_color", "color_name"}}
        attempts: List[dict] = []

        if isinstance(xy_color, list) and len(xy_color) == 2:
            item = dict(core)
            item["xy_color"] = xy_color
            attempts.append(item)
        if isinstance(rgb_color, list) and len(rgb_color) == 3:
            item = dict(core)
            item["rgb_color"] = rgb_color
            attempts.append(item)
        if color_name:
            item = dict(core)
            item["color_name"] = color_name
            attempts.append(item)

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

    def _state_summary(self, *, entity_id: str, state_payload: dict, read_target: str, color_label: str = "") -> str:
        state_data = state_payload if isinstance(state_payload, dict) else {}
        attrs = state_data.get("attributes") if isinstance(state_data.get("attributes"), dict) else {}
        name = self._coerce_text(attrs.get("friendly_name")) or entity_id
        state = self._coerce_text(state_data.get("state")) or "unknown"

        extras: List[str] = []
        brightness_raw = self._normalize_int(attrs.get("brightness"))
        if brightness_raw is not None:
            brightness_raw = max(0, min(255, int(brightness_raw)))
            extras.append(f"brightness {int(round((brightness_raw / 255.0) * 100.0))}%")

        label = self._normalize_display_color_label(color_label)
        if label:
            extras.append(f"color {label}")

        if extras:
            return f"{name} is {state}; " + ", ".join(extras) + "."
        return f"{name} is {state}."

    def _sanitize_state_for_hydra(self, *, entity_id: str, state_payload: dict, color_label: str = "") -> dict:
        state_data = state_payload if isinstance(state_payload, dict) else {}
        attrs = state_data.get("attributes") if isinstance(state_data.get("attributes"), dict) else {}

        safe_attrs: Dict[str, Any] = {}
        friendly = self._coerce_text(attrs.get("friendly_name"))
        if friendly:
            safe_attrs["friendly_name"] = friendly

        brightness_raw = self._normalize_int(attrs.get("brightness"))
        if brightness_raw is not None:
            brightness_raw = max(0, min(255, int(brightness_raw)))
            safe_attrs["brightness_pct"] = int(round((brightness_raw / 255.0) * 100.0))

        label = self._normalize_display_color_label(color_label)
        if label:
            safe_attrs["color"] = label

        color_mode = self._coerce_text(attrs.get("color_mode"))
        if color_mode:
            safe_attrs["color_mode"] = color_mode

        return {
            "entity_id": self._coerce_text(state_data.get("entity_id")) or entity_id,
            "state": self._coerce_text(state_data.get("state")) or "unknown",
            "attributes": safe_attrs,
        }

    def _get_plugin_settings(self) -> dict:
        return _read_plugin_settings()

    def _get_int_setting(self, key: str, default: int, minimum: int, maximum: int) -> int:
        return _as_int(self._get_plugin_settings().get(key), default, minimum=minimum, maximum=maximum)

    def _hue_diagnosis(self) -> dict:
        settings = read_hue_settings()
        bridge = normalize_hue_bridge_root(settings.get("HUE_BRIDGE_HOST") or HUE_DEFAULT_BRIDGE_HOST)
        parsed = urlparse(bridge)
        app_key = _coerce_text(settings.get("HUE_APP_KEY"))
        return {
            "hue_bridge_host": "set" if parsed.scheme in {"http", "https"} and bool(parsed.netloc) else "invalid",
            "hue_app_key": "set" if len(app_key.strip()) >= 8 else "missing",
        }

    def _ha_diagnosis(self) -> dict:
        return self._hue_diagnosis()

    def _get_client(self) -> Optional[HueClient]:
        try:
            return HueClient()
        except Exception as exc:
            logger.error("[%s] Failed to initialize Hue client: %s", self.name, exc)
            return None

    def _ensure_llm_available(self, llm_client) -> Optional[dict]:
        if llm_client is not None:
            return None
        return action_failure(
            code="llm_unavailable",
            message="This Philips Hue verba requires LLM parsing for natural-language requests.",
            needs=["Try again when the model is reachable."],
            say_hint="Explain the model was unavailable and retry once model connectivity returns.",
        )

    def handle_setting_button(self, key):
        if _coerce_text(key) == "HUE_LINK_BRIDGE":
            try:
                return HueClient.pair_bridge()
            except Exception as exc:
                return f"Hue Bridge pairing failed: {exc}"
        return f"Unknown Philips Hue settings action: {key}"

    def webui_prepare_settings_values(self, values=None, redis_client=None):
        out = dict(values or {})
        out["HUE_MAX_CANDIDATES"] = str(_as_int(out.get("HUE_MAX_CANDIDATES"), 150, minimum=5, maximum=800))
        return out

    def _interpret_system_prompt(self) -> str:
        actions = "|".join(self.interpret_actions or ["get_state"])
        examples_blob = "\n".join([f"- {x}" for x in (self.interpret_examples or []) if str(x).strip()])
        focus = self._coerce_text(self.interpret_focus)
        if focus:
            focus += "\n"

        return (
            f"You interpret Philips Hue Bridge light requests for verba '{self.name}'.\n"
            "Return STRICT JSON only:\n"
            "{\n"
            f'  "action": "{actions}",\n'
            '  "target": "<short Hue room/light/zone phrase or empty>",\n'
            '  "read_target": "state|none",\n'
            '  "params": {\n'
            '    "brightness_pct": <int 0-100 or null>,\n'
            '    "color_name": "<string or empty>",\n'
            '    "rgb_color": [<int 0-255>, <int 0-255>, <int 0-255>] or null,\n'
            '    "xy_color": [<float 0-1>, <float 0-1>] or null,\n'
            '    "hex_color": "#RRGGBB" or "#RGB" or empty\n'
            "  }\n"
            "}\n"
            "Rules:\n"
            f"- action must be one of: {actions}.\n"
            "- target is the user-mentioned Hue room, zone, or light phrase when present.\n"
            "- Use read_target=state for status checks.\n"
            "- For control actions use read_target=none.\n"
            f"{focus}"
            "Examples:\n"
            f"{examples_blob if examples_blob else '- none'}\n"
        )

    async def _choose_entity(self, *, query: str, intent: dict, catalog: List[dict], llm_client) -> str:
        if not catalog:
            return ""
        if len(catalog) == 1:
            return self._coerce_text(catalog[0].get("entity_id")).lower()

        max_candidates = self._get_int_setting("HUE_MAX_CANDIDATES", 150, 5, 800)
        shortlist = catalog[:max_candidates]
        compact = [
            {
                "entity_id": row.get("entity_id"),
                "name": row.get("name"),
                "domain": row.get("domain"),
                "state": row.get("state"),
                "hue_type": row.get("hue_type"),
            }
            for row in shortlist
        ]
        valid_ids = {self._coerce_text(row.get("entity_id")).lower() for row in shortlist}

        system = (
            f"Choose the best Philips Hue entity for verba '{self.name}'.\n"
            "Return STRICT JSON only: {\"entity_id\":\"hue.1\"}.\n"
            "Rules:\n"
            "- Pick exactly one entity_id from candidates.\n"
            "- Prefer hue_group entities when the user asks for a room, area, zone, or plural lights.\n"
            "- Prefer hue entities when the user names a single bulb/device.\n"
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

    def _build_catalog(self, states: List[dict]) -> List[dict]:
        domains = {domain.strip().lower() for domain in (self.entity_domains or []) if str(domain).strip()}
        rows: List[dict] = []
        seen: set[str] = set()

        for state in states or []:
            if not isinstance(state, dict):
                continue
            entity_id = self._coerce_text(state.get("entity_id")).lower()
            if not entity_id or entity_id in seen or "." not in entity_id:
                continue

            domain = entity_id.split(".", 1)[0]
            if domains and domain not in domains:
                continue

            attrs = state.get("attributes") if isinstance(state.get("attributes"), dict) else {}
            row = {
                "entity_id": entity_id,
                "domain": domain,
                "name": self._coerce_text(attrs.get("friendly_name")) or entity_id,
                "state": self._coerce_text(state.get("state")) or "unknown",
                "hue_type": self._coerce_text(attrs.get("hue_type")),
            }
            rows.append(row)
            seen.add(entity_id)

        rows.sort(key=lambda r: (0 if r.get("domain") == "hue_group" else 1, (r.get("name") or "").lower()))
        return rows

    async def _handle(self, args, llm_client):
        args = self._normalize_handler_args(args)
        query = self._coerce_text(args.get("query"))
        if not query:
            return action_failure(
                code="missing_query",
                message=f"Please provide a {self.pretty_name or self.name} request in query.",
                needs=[f"What should I do with {self.pretty_name or self.name}?"],
                say_hint="Ask for the Philips Hue request in natural language.",
            )

        llm_missing = self._ensure_llm_available(llm_client)
        if llm_missing:
            return llm_missing

        client = self._get_client()
        if not client:
            diagnosis = self._hue_diagnosis()
            needs = needs_from_diagnosis(
                diagnosis,
                {
                    "hue_bridge_host": "Please set your Hue Bridge host/IP in Tater Settings > Integrations > Philips Hue.",
                    "hue_app_key": "Open Tater Settings > Integrations > Philips Hue, press the Hue Bridge button, then click Link Hue Bridge.",
                },
            )
            return action_failure(
                code="hue_not_configured",
                message="Philips Hue Bridge connection is not configured.",
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain Philips Hue settings are missing and ask to link the bridge.",
            )

        try:
            states = client.list_states()
        except Exception as exc:
            return action_failure(
                code="hue_states_failed",
                message=f"Could not read Philips Hue Bridge lights: {exc}",
                diagnosis=self._hue_diagnosis(),
                say_hint="Explain Hue Bridge state fetch failed and suggest retrying.",
            )

        catalog = self._build_catalog(states)
        if not catalog:
            return action_failure(
                code="no_entities",
                message="No Philips Hue lights, rooms, or zones were found on the bridge.",
                needs=["Add lights or rooms to the Hue Bridge and try again."],
                say_hint="Explain no matching Hue entities were available for this verba.",
            )

        intent = await self._interpret_query(query, llm_client)
        if not intent:
            return action_failure(
                code="interpret_failed",
                message="Could not interpret this request for Philips Hue lights.",
                needs=["Try rephrasing with a clearer action and Hue light/room/zone target."],
                say_hint="Ask for clearer wording and keep focus on Hue lights.",
            )

        action = self._coerce_text(intent.get("action")).lower()
        entity_id = self._coerce_text(args.get("entity_id")).lower()
        valid_ids = {self._coerce_text(row.get("entity_id")).lower() for row in catalog}
        if entity_id:
            if entity_id not in valid_ids:
                return action_failure(
                    code="invalid_entity_id",
                    message=f"Entity '{entity_id}' is not available for {self.name}.",
                    needs=["Use a valid Hue entity_id from this bridge."],
                    say_hint="Explain provided Hue entity_id is invalid.",
                )
        else:
            entity_id = await self._choose_entity(query=query, intent=intent, catalog=catalog, llm_client=llm_client)
            if not entity_id:
                return action_failure(
                    code="entity_selection_failed",
                    message="Could not select a Philips Hue light, room, or zone for this request.",
                    needs=["Specify the Hue light, room, or zone more clearly."],
                    say_hint="Ask for a clearer Hue target.",
                )

        selected = next((row for row in catalog if self._coerce_text(row.get("entity_id")).lower() == entity_id), None)
        if not isinstance(selected, dict):
            return action_failure(
                code="entity_not_found",
                message=f"Selected Hue entity '{entity_id}' was not found.",
                say_hint="Explain Hue entity lookup failed and suggest retrying.",
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
                    message="Could not resolve a concrete Hue light color from this request.",
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
                summary = self._state_summary(
                    entity_id=entity_id,
                    state_payload=state_dict,
                    read_target=read_target,
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
                    say_hint="Report the Philips Hue state from returned data.",
                )
            except Exception as exc:
                return action_failure(
                    code="state_read_failed",
                    message=f"Could not read Hue state for {entity_id}: {exc}",
                    diagnosis=self._hue_diagnosis(),
                    say_hint="Explain Hue state read failed and suggest retrying.",
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
                message=f"Philips Hue {service} failed for {entity_id}: {call_error}",
                diagnosis=self._hue_diagnosis(),
                needs=["Retry or choose a different Hue light, room, or zone."],
                say_hint="Explain Hue service call failed and ask whether to retry.",
            )

        display_name = self._coerce_text(selected.get("name")) or entity_id
        summary = f"Completed Hue light update for {display_name}."
        if requested_color_label:
            summary = f"Completed Hue light update for {display_name}; color {requested_color_label}."
        state_after: Dict[str, Any] = {}
        try:
            raw_state = client.get_state(entity_id)
            if isinstance(raw_state, dict):
                state_after = self._sanitize_state_for_hydra(
                    entity_id=entity_id,
                    state_payload=raw_state,
                    color_label=requested_color_label,
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
            say_hint="Reply briefly that the Hue light command is complete. Do not include rgb/xy/internal attribute dumps.",
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


verba = HueLightsPlugin()
