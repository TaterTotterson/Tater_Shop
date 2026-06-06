# verba/unifi_protect_camera.py
import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from verba_base import ToolVerba
from verba_result import action_failure, action_success
from helpers import redis_client
from tateros import integration_store as integration_store_module

def _build_media_metadata(binary: bytes, *, media_type: str, name: str, mimetype: str) -> dict:
    if not isinstance(binary, (bytes, bytearray)):
        raise TypeError("binary must be bytes")
    return {
        "type": media_type,
        "name": name,
        "mimetype": mimetype,
        "size": len(binary),
        "bytes": bytes(binary),
    }

logger = logging.getLogger("unifi_protect_camera")
logger.setLevel(logging.INFO)


def _protect_client():
    module = integration_store_module.integration_module("unifi_protect")
    if module is None:
        raise RuntimeError("UniFi Protect integration is not enabled.")
    return module.ProtectClient()


# ----------------------------
# Small helpers
# ----------------------------
def _coerce_str(x) -> str:
    return "" if x is None else str(x)


def _ms_to_local_str(ms: Any) -> str:
    if not ms:
        return "unknown"
    try:
        dt = datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).astimezone()
        return dt.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return "unknown"


def _c_to_f(c: Any) -> Optional[float]:
    if c is None:
        return None
    try:
        return (float(c) * 9.0 / 5.0) + 32.0
    except Exception:
        return None


def _strip_code_fences(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.MULTILINE).strip()
    if s.endswith("```"):
        s = re.sub(r"\s*```$", "", s, flags=re.MULTILINE).strip()
    return s


def _extract_context_text(context: Optional[dict]) -> str:
    if not isinstance(context, dict):
        return ""
    for key in ("raw_message", "request_text", "user_text", "task_prompt", "body"):
        val = context.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    msg = context.get("message")
    if msg is not None:
        for attr in ("content", "text", "message"):
            val = getattr(msg, attr, None)
            if isinstance(val, str) and val.strip():
                return val.strip()

    update = context.get("update")
    if isinstance(update, dict):
        text = ((update.get("message") or {}).get("text") or "")
        if isinstance(text, str) and text.strip():
            return text.strip()

    ctx = context.get("context")
    if isinstance(ctx, dict):
        for key in ("raw_message", "text", "message"):
            val = ctx.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()

    return ""


def _split_action_and_target(raw_action: str) -> Tuple[str, str]:
    raw = (raw_action or "").strip().lower()
    if not raw:
        return "", ""
    raw = raw.replace("-", "_")
    if raw in {"describe area", "describe camera", "list cameras", "sensor detail", "sensors status"}:
        raw = raw.replace(" ", "_")
        return raw, ""

    tokens = raw.split()
    if len(tokens) >= 2:
        candidate = f"{tokens[0]}_{tokens[1]}"
        if candidate in {"sensors_status", "sensor_detail", "list_cameras", "describe_camera", "describe_area"}:
            return candidate, " ".join(tokens[2:]).strip()
        return tokens[0], " ".join(tokens[1:]).strip()
    return raw, ""


def _looks_like_area(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in [
        "front yard", "back yard", "yard", "porch", "driveway", "patio", "garage", "front porch", "back porch",
    ])


def _guess_action_from_text(text: str, target: str = "") -> str:
    t = f"{text} {target}".lower().strip()
    if not t:
        return ""

    if "list" in t and "camera" in t:
        return "list_cameras"

    if any(k in t for k in ["sensor", "sensors", "battery", "humidity", "temperature", "temp"]):
        if any(k in t for k in ["front door", "back door", "garage door", "doorbell"]):
            return "sensor_detail"
        return "sensors_status"

    if any(k in t for k in ["snapshot", "photo", "picture", "image"]):
        return "describe_camera" if target or "camera" in t or "doorbell" in t else "describe_area"

    if any(k in t for k in ["what's happening", "whats happening", "what is happening", "show me", "look", "see", "view", "describe"]):
        if _looks_like_area(t):
            return "describe_area"
        if any(k in t for k in ["camera", "cam", "doorbell", "front door", "back door"]):
            return "describe_camera"
        return "describe_area"

    if _looks_like_area(t):
        return "describe_area"

    if any(k in t for k in ["door", "window"]):
        return "sensor_detail" if target else "sensors_status"

    return ""


def _platform_supports_media(platform: str) -> bool:
    return (platform or "").strip().lower() in {"webui", "discord", "telegram", "matrix"}


# ----------------------------
# Plugin
# ----------------------------
class UniFiProtectCameraPlugin(ToolVerba):
    """
    UniFi Protect (Integration API) plugin.

    What it can do (with what we know works right now):
    - Sensors:
      - "are any doors open?"
      - "when was the garage door last opened?"
      - "how are the sensors doing?"
      - "any low battery sensors?"
    - Cameras:
      - "list cameras"
      - "what's going on in the front yard?" (takes snapshots from matching cameras and runs Vision model)
      - "describe the doorbell" (snapshot + vision summary)

    Notes:
    - Some event endpoints (subscribe/events, smartDetects) are NOT exposed on your console right now
      and return "Entity 'endpoint' not found". This plugin does NOT rely on those.
    """

    name = 'unifi_protect_camera'
    verba_name = 'UniFi Protect Camera'
    version = "1.0.6"
    min_tater_version = "59"
    pretty_name = 'UniFi Protect Camera'
    settings_category = "UniFi Protect"
    tags = ['unifi', 'camera_snapshot']

    platforms = ['webui', 'little_spud', 'macos', 'voice_core', 'homeassistant', 'homekit', 'xbmc', 'discord', 'telegram', 'matrix', 'irc', 'meshtastic']
    routing_keywords = ['unifi protect', 'protect', 'camera', 'doorbell', 'snapshot', 'describe camera', 'front door camera']

    usage = '{"function":"unifi_protect_camera","arguments":{"query":"describe the doorbell camera"}}'

    description = 'Capture one UniFi Protect camera snapshot and describe what is happening.'
    verba_dec = 'Describe a single UniFi Protect camera snapshot.'
    when_to_use = 'Use when the request is about one specific camera.'
    how_to_use = 'Set query to what camera to inspect, or provide camera/target directly.'
    common_needs = ['Camera-focused request in query, or camera/target argument.']
    missing_info_prompts = []
    example_calls = ['{"function":"unifi_protect_camera","arguments":{"query":"describe the doorbell camera"}}', '{"function":"unifi_protect_camera","arguments":{"camera":"garage"}}']

    argument_schema = {'type': 'object', 'properties': {'target': {'type': 'string', 'description': 'Camera name to inspect (for example: doorbell, garage, front door).'}, 'camera': {'type': 'string', 'description': 'Camera name (alias of target).'}, 'query': {'type': 'string', 'description': 'The camera description request (for example: describe the doorbell camera, what is happening at the garage camera).'}}, 'required': []}
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking UniFi Protect now. "
        "Only output that message."
    )

    required_settings = {}

    _ALLOWED_ACTIONS = {"sensors_status", "sensor_detail", "list_cameras", "describe_camera", "describe_area"}
    _ACTION_ALIASES = {
        "sensors": "sensors_status",
        "sensor_status": "sensors_status",
        "sensor_statuses": "sensors_status",
        "sensor": "sensor_detail",
        "camera": "describe_camera",
        "snapshot": "describe_camera",
        "camera_snapshot": "describe_camera",
        "area": "describe_area",
        "area_snapshot": "describe_area",
        "cameras": "list_cameras",
        "camera_list": "list_cameras",
        "list_camera": "list_cameras",
    }

    def _normalize_action(self, value: Any) -> str:
        text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
        key = re.sub(r"[^a-z0-9_]+", "_", text).strip("_")
        if not key:
            return ""
        if key in self._ALLOWED_ACTIONS:
            return key
        return self._ACTION_ALIASES.get(key, "")

    @staticmethod
    def _json_object_from_text(text: str) -> dict:
        clean = _strip_code_fences(str(text or ""))
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

    def _infer_action(self, args: dict, query: str, target: str) -> Tuple[str, str]:
        args = args or {}
        for key in ("action", "intent", "command"):
            raw = str(args.get(key) or "").strip()
            if not raw:
                continue
            split_action, split_target = _split_action_and_target(raw)
            action = self._normalize_action(split_action)
            if action:
                return action, split_target or target

        if args.get("area") or args.get("area_name"):
            return "describe_area", target
        if args.get("camera") or args.get("camera_name"):
            return "describe_camera", target
        if args.get("sensor") or args.get("sensor_name"):
            return "sensor_detail", target

        guessed = self._normalize_action(_guess_action_from_text(query, target))
        if guessed:
            return guessed, target
        return "", target

    async def _ai_pick_action(self, llm_client, query: str, target: str) -> dict:
        if not llm_client:
            return {}
        text = str(query or "").strip()
        target_text = str(target or "").strip()
        if not text and not target_text:
            return {}
        allowed = ", ".join(sorted(self._ALLOWED_ACTIONS))
        prompt = (
            "Choose the best UniFi Protect action for the user request.\n"
            f"Allowed actions: {allowed}.\n"
            "Rules:\n"
            "1) Use list_cameras for requests to list or show available cameras.\n"
            "2) Use describe_camera for one named camera, doorbell, snapshot, or camera view.\n"
            "3) Use describe_area for areas such as front yard, back yard, porch, driveway, patio, or garage area.\n"
            "4) Use sensors_status for overall door/window sensor status, low battery, temperature, or humidity questions.\n"
            "5) Use sensor_detail for one named sensor or a specific door/window sensor question.\n"
            "6) Include target for describe_camera, describe_area, or sensor_detail when the request names one.\n"
            "Respond with JSON: "
            '{"action":"<allowed action or empty string>","target":"<camera area or sensor target or empty string>"}'
            "\n\n"
            f'User request: "{text}"\n'
            f'Existing target hint: "{target_text}"\n'
        )
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            raw = ((resp or {}).get("message") or {}).get("content", "")
            data = self._json_object_from_text(raw)
            return {
                "action": self._normalize_action(data.get("action")),
                "target": str(data.get("target") or "").strip(),
            }
        except Exception as e:
            logger.warning("[%s] _ai_pick_action failed: %s", self.name, e)
            return {}

    async def _resolve_action(self, args: dict, query: str, target: str, llm_client) -> Tuple[str, str]:
        action, resolved_target = self._infer_action(args, query, target)
        ai_payload: dict = {}
        if not action:
            ai_payload = await self._ai_pick_action(llm_client, query, target)
            action = self._normalize_action(ai_payload.get("action"))
            resolved_target = resolved_target or str(ai_payload.get("target") or "").strip()
        if not action:
            raise ValueError("Could not determine a UniFi Protect action from the request.")
        if action not in self._ALLOWED_ACTIONS:
            raise ValueError(f"Invalid UniFi Protect action: {action}")
        resolved_target = resolved_target or str(ai_payload.get("target") or "").strip()
        if action == "sensors_status" and resolved_target:
            action = "sensor_detail"
        return action, resolved_target

    # ----------------------------
    # Settings / client
    # ----------------------------
    def _get_client(self) -> Optional[Any]:
        try:
            return _protect_client()
        except Exception as e:
            logger.error(f"[unifi_protect_camera] Failed to init client: {e}")
            return None

    # ----------------------------
    # Data shaping: sensors
    # ----------------------------
    def _compact_sensors(self, sensors: List[dict]) -> dict:
        """
        Make a compact facts blob so LLM is fast + less likely to hallucinate.
        """
        items = []
        open_sensors = []
        low_batt = []

        for s in sensors or []:
            if not isinstance(s, dict):
                continue
            name = _coerce_str(s.get("name") or "Unknown")
            state = _coerce_str(s.get("state") or "")
            mount = _coerce_str(s.get("mountType") or "")

            batt = (s.get("batteryStatus") or {})
            batt_pct = batt.get("percentage")
            batt_low = batt.get("isLow")

            is_open = bool(s.get("isOpened"))
            open_changed = s.get("openStatusChangedAt")

            stats = (s.get("stats") or {})
            temp_c = ((stats.get("temperature") or {}).get("value"))
            hum = ((stats.get("humidity") or {}).get("value"))

            item = {
                "id": s.get("id"),
                "name": name,
                "state": state,
                "mountType": mount,
                "isOpened": is_open,
                "openStatusChangedAt": _ms_to_local_str(open_changed),
                "batteryPct": batt_pct,
                "batteryLow": batt_low,
                "tempC": temp_c,
                "tempF": _c_to_f(temp_c),
                "humidity": hum,
            }
            items.append(item)

            if is_open:
                open_sensors.append({"name": name, "changed": _ms_to_local_str(open_changed)})
            if batt_low is True or (isinstance(batt_pct, (int, float)) and batt_pct <= 20):
                low_batt.append({"name": name, "batteryPct": batt_pct})

        return {
            "total": len(items),
            "open": open_sensors[:10],
            "lowBattery": low_batt[:10],
            "items": items,  # keep full list for follow-up questions (still compact)
        }

    def _find_best_named(self, items: List[dict], name_hint: str) -> Optional[dict]:
        """
        Find a sensor/camera by fuzzy name match.
        """
        hint = (name_hint or "").strip().lower()
        if not hint:
            return None

        # exact contains match
        for it in items:
            n = (it.get("name") or "").strip().lower()
            if hint == n:
                return it
        for it in items:
            n = (it.get("name") or "").strip().lower()
            if hint in n:
                return it

        # loose keyword match
        hint_words = [w for w in re.split(r"\s+", hint) if w]
        best = None
        best_score = 0
        for it in items:
            n = (it.get("name") or "").strip().lower()
            score = sum(1 for w in hint_words if w in n)
            if score > best_score:
                best_score = score
                best = it
        return best if best_score > 0 else None

    # ----------------------------
    # Data shaping: cameras
    # ----------------------------
    def _compact_cameras(self, cams: List[dict]) -> List[dict]:
        out = []
        for c in cams or []:
            if not isinstance(c, dict):
                continue
            out.append({
                "id": c.get("id"),
                "name": c.get("name"),
                "state": c.get("state"),
                "modelKey": c.get("modelKey"),
                "hasSpeaker": ((c.get("featureFlags") or {}).get("hasSpeaker")),
                "smartDetectTypes": ((c.get("featureFlags") or {}).get("smartDetectTypes")) or [],
            })
        return out

    def _area_keywords(self, target: str) -> List[str]:
        t = (target or "").lower().strip()
        if not t:
            return []

        # Normalize common variants
        t = t.replace("backyard", "back yard").replace("frontyard", "front yard")

        mapping = {
            # --- Front ---
            "front yard": ["front yard", "frontyard", "front"],
            "front door": ["front door", "frontdoor"],
            "doorbell": ["doorbell"],

            # Keep "porch" specific so it doesn't match both sides
            "front porch": ["front porch", "frontporch", "porch front", "front porch cam"],
            "porch": ["porch"],  # neutral porch keyword only, see selection logic below

            # --- Back ---
            "back yard": ["back yard", "backyard", "back"],  # removed generic "yard"
            "back porch": ["back porch", "backporch", "porch back", "back patio", "patio back"],
            "patio": ["patio"],

            # --- Other ---
            "garage": ["garage"],
            "door": ["door", "front door", "back door", "doorbell"],
        }

        # Exact mapping key match
        if t in mapping:
            return mapping[t]

        # If user said "back porch" or "front porch" in a longer phrase, detect it
        if "back porch" in t or ("porch" in t and "back" in t):
            return mapping["back porch"]
        if "front porch" in t or ("porch" in t and "front" in t):
            return mapping["front porch"]

        # If user says "back yard" or "front yard" inside a longer phrase
        if "back yard" in t or ("yard" in t and "back" in t):
            return mapping["back yard"]
        if "front yard" in t or ("yard" in t and "front" in t):
            return mapping["front yard"]

        # Default: use the literal phrase as a keyword
        return [t]

    def _pick_cameras_for_area(self, cameras: List[dict], target: str) -> List[dict]:
        keys = self._area_keywords(target)
        if not keys:
            return cameras[:3]

        picked = []
        t = (target or "").lower()

        wants_front = "front" in t
        wants_back = "back" in t
        wants_garage = "garage" in t

        for c in cameras:
            name = (c.get("name") or "").lower()

            # ---- Direction bias for porch ----
            if "porch" in keys and (wants_front or wants_back):
                if wants_front and "front" not in name:
                    continue
                if wants_back and "back" not in name:
                    continue

            # ---- Garage logic ----
            if "garage" in name:
                if wants_garage:
                    picked.append(c)
                    continue
                if wants_front and not wants_back:
                    picked.append(c)  # treat garage as front
                    continue
                if wants_back:
                    continue  # exclude garage from back queries

            # ---- Normal keyword match ----
            if any(k in name for k in keys):
                picked.append(c)

        return picked

    # ----------------------------
    # Vision prompt
    # ----------------------------
    def _vision_prompt_for_query(self, user_query: str, camera_name: str) -> str:
        """
        Keep it factual and useful for a spoken summary.
        """
        q = (user_query or "").strip()
        cam = (camera_name or "Camera").strip()
        return (
            f"You are a careful visual assistant.\n"
            f"Camera: {cam}\n"
            f"User asked: {q}\n\n"
            "Describe ONLY what you can actually see in the image.\n"
            "Mention notable people, vehicles, packages, animals, doors open/closed, and obvious activity.\n"
            "If nothing notable is happening, say so.\n"
            "Be concise (2-6 sentences)."
        )

    # ----------------------------
    # Deterministic summary from facts
    # ----------------------------
    def _summary_from_facts(self, user_query: str, facts: Any) -> str:
        if not isinstance(facts, dict):
            return "Completed UniFi Protect request."

        if isinstance(facts.get("sensor"), dict):
            sensor = facts.get("sensor") or {}
            name = _coerce_str(sensor.get("name") or "sensor")
            opened = "open" if sensor.get("isOpened") else "closed"
            batt = sensor.get("batteryPct")
            return f"{name} is {opened}. Battery: {batt}%." if batt is not None else f"{name} is {opened}."

        if isinstance(facts.get("sensors"), dict):
            sensors = facts.get("sensors") or {}
            total = int(sensors.get("total") or 0)
            open_count = len(sensors.get("open") or [])
            low_batt = len(sensors.get("lowBattery") or [])
            return f"Sensors: {total} total, {open_count} open, {low_batt} low battery."

        if isinstance(facts.get("cameras"), list):
            cams = facts.get("cameras") or []
            return f"Cameras: {len(cams)} available."

        if isinstance(facts.get("vision_description"), str):
            text = _coerce_str(facts.get("vision_description")).strip()
            cam = _coerce_str((facts.get("camera") or {}).get("name") or "camera")
            if text:
                return f"{cam}: {text}"
            return f"{cam}: snapshot captured."

        if isinstance(facts.get("camera_descriptions"), list):
            desc = [d for d in facts.get("camera_descriptions") or [] if isinstance(d, dict)]
            if not desc:
                return "No camera descriptions available."
            first = desc[0]
            cam = _coerce_str(first.get("camera") or "camera")
            txt = _coerce_str(first.get("vision") or "").strip()
            if len(desc) == 1:
                return f"{cam}: {txt}" if txt else f"{cam}: snapshot captured."
            return f"Captured {len(desc)} camera snapshots. First: {cam}: {txt}" if txt else f"Captured {len(desc)} camera snapshots."

        note = _coerce_str(facts.get("note")).strip()
        if note:
            return note
        return "Completed UniFi Protect request."

    # ----------------------------
    # Platform handlers
    # ----------------------------
    async def handle_webui(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="webui", context=context)

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self.handle_webui(args or {}, llm_client, context=context)


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
    async def handle_homeassistant(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="homeassistant", context=context)
    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        try:
            return await self.handle_homeassistant(args=args, llm_client=llm_client, context=context)
        except TypeError:
            try:
                return await self.handle_homeassistant(args=args, llm_client=llm_client)
            except TypeError:
                return await self.handle_homeassistant(args, llm_client)


    async def handle_homekit(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="homekit", context=context)

    async def handle_xbmc(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="xbmc", context=context)

    async def handle_discord(self, message, args, llm_client, context=None):
        ctx = context or {}
        if "message" not in ctx:
            ctx["message"] = message
        return await self._handle(args, llm_client, platform="discord", context=ctx)

    async def handle_telegram(self, update, args, llm_client, context=None):
        ctx = context or {}
        if "update" not in ctx:
            ctx["update"] = update
        return await self._handle(args, llm_client, platform="telegram", context=ctx)

    async def handle_matrix(self, client, room, sender, body, args, llm_client, context=None):
        ctx = context or {}
        if "body" not in ctx:
            ctx["body"] = body
        return await self._handle(args, llm_client, platform="matrix", context=ctx)


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

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client, context=None):
        ctx = context or {}
        if "raw_message" not in ctx:
            ctx["raw_message"] = raw_message
        return await self._handle(args, llm_client, platform="irc", context=ctx)

    # ----------------------------
    # Core
    # ----------------------------
    async def _handle(self, args, llm_client, platform: str = "webui", context: Optional[dict] = None):
        client = self._get_client()
        if not client:
            return action_failure(
                code="unifi_protect_camera_not_configured",
                message=(
                    "UniFi Protect is not configured. "
                    "Set UniFi Protect API settings in WebUI Settings -> Integrations -> UniFi Protect."
                ),
                say_hint="Explain required UniFi Protect settings are missing.",
            )

        args = args or {}
        context = context or {}
        target = (
            args.get("target")
            or args.get("camera")
            or args.get("area")
            or args.get("camera_name")
            or args.get("area_name")
            or ""
        )
        target = str(target).strip()
        query = (args.get("query") or "").strip()
        if not query:
            query = _extract_context_text(context)

        if not query and target:
            query = target

        if not query and not target:
            return action_failure(
                code="missing_query",
                message="Please provide a UniFi Protect request in `query`.",
                needs=["Ask for sensors status, camera list, a camera description, or an area description."],
                say_hint="Ask what UniFi Protect information the user wants.",
            )

        try:
            action, target = await self._resolve_action(args, query, target, llm_client)
        except Exception as e:
            return action_failure(
                code="unknown_action",
                message=f"I couldn't determine the UniFi Protect action from that request: {e}",
                needs=["Ask for sensors status, camera list, a camera description, or an area description."],
                say_hint="Ask the user to restate the UniFi Protect request in one short sentence.",
            )

        # ---- Sensors status / detail
        if action in ("sensors_status", "sensor_detail"):
            try:
                sensors = client.list_sensors()
            except Exception as e:
                logger.error(f"[unifi_protect_camera] list_sensors error: {e}")
                return action_failure(
                    code="unifi_sensors_failed",
                    message=f"I couldn't reach UniFi Protect sensors. {e}",
                    say_hint="Explain that sensor retrieval failed.",
                )

            compact = self._compact_sensors(sensors)

            if action == "sensor_detail":
                hit = self._find_best_named(compact.get("items") or [], target or query)
                if not hit:
                    return action_failure(
                        code="sensor_not_found",
                        message=f"No specific sensor matched: {target or query}",
                        needs=["Specify a camera/sensor name, such as front door or garage door."],
                        say_hint="Explain that no matching sensor was found and ask for a clearer name.",
                    )

                # Optionally fetch full detail
                try:
                    full = client.get_sensor(_coerce_str(hit.get("id")))
                except Exception:
                    full = None

                facts = {"sensor": hit, "sensor_detail": full}
                return action_success(
                    facts=facts,
                    summary_for_user=self._summary_from_facts(query, facts),
                    say_hint="Report the requested UniFi sensor status from the provided facts.",
                )

            facts = {"sensors": compact}
            return action_success(
                facts=facts,
                summary_for_user=self._summary_from_facts(query, facts),
                say_hint="Summarize UniFi sensor status from the provided facts.",
            )

        # ---- Cameras list
        if action == "list_cameras":
            try:
                cams = client.list_cameras()
            except Exception as e:
                logger.error(f"[unifi_protect_camera] list_cameras error: {e}")
                return action_failure(
                    code="unifi_cameras_failed",
                    message=f"I couldn't list cameras. {e}",
                    say_hint="Explain that camera listing failed.",
                )

            facts = {"cameras": self._compact_cameras(cams), "total": len(cams or [])}
            return action_success(
                facts=facts,
                summary_for_user=self._summary_from_facts(query, facts),
                say_hint="Summarize available cameras from the provided facts.",
            )

        # ---- Describe a specific camera (snapshot + vision + summary)
        if action == "describe_camera":
            try:
                cams = client.list_cameras()
            except Exception as e:
                logger.error(f"[unifi_protect_camera] list_cameras error: {e}")
                return action_failure(
                    code="unifi_cameras_failed",
                    message=f"I couldn't list cameras. {e}",
                    say_hint="Explain that camera listing failed.",
                )

            compact_cams = self._compact_cameras(cams)
            hit = self._find_best_named(compact_cams, target or query)
            if not hit:
                return action_failure(
                    code="camera_not_found",
                    message=f"No camera matched: {target or query}",
                    needs=["Specify a camera name, such as doorbell, front yard, or garage."],
                    say_hint="Explain that no matching camera was found and ask for a clearer name.",
                )

            cam_id = _coerce_str(hit.get("id"))
            cam_name = _coerce_str(hit.get("name") or "Camera")

            try:
                img_bytes, mimetype = client.get_camera_snapshot(cam_id)
            except Exception as e:
                logger.error(f"[unifi_protect_camera] snapshot error: {e}")
                facts = {"camera": hit, "note": f"Snapshot not available for that camera: {e}"}
                return action_failure(
                    code="snapshot_failed",
                    message=f"Snapshot not available for that camera: {e}",
                    say_hint="Explain snapshot capture failed for the selected camera.",
                )

            # Vision describe (run in thread; requests is blocking)
            vision_prompt = self._vision_prompt_for_query(query, cam_name)
            try:
                vision_text = await asyncio.to_thread(
                    client.call_vision, img_bytes, prompt=vision_prompt, filename=f"{cam_name}.jpg"
                )
            except Exception as e:
                vision_text = f"(vision error) {e}"

            facts = {
                "camera": hit,
                "vision_description": vision_text,
                "snapshot_meta": {"mimetype": mimetype, "bytes": len(img_bytes)},
            }
            summary = self._summary_from_facts(query, facts)

            # WebUI can render image blobs. Others: return text only (spoken-friendly).
            if _platform_supports_media(platform):
                artifact = _build_media_metadata(
                    img_bytes,
                    media_type="image",
                    name=f"{cam_name}.jpg",
                    mimetype=mimetype,
                )
                return action_success(
                    facts=facts,
                    summary_for_user=summary,
                    say_hint="Describe the camera snapshot using the vision facts provided.",
                    artifacts=[artifact],
                )

            return action_success(
                facts=facts,
                summary_for_user=summary,
                say_hint="Describe the camera snapshot using the vision facts provided.",
            )

        # ---- Describe an area (multiple snapshots + per-cam vision + combined summary)
        if action == "describe_area":
            try:
                cams = client.list_cameras()
            except Exception as e:
                logger.error(f"[unifi_protect_camera] list_cameras error: {e}")
                return action_failure(
                    code="unifi_cameras_failed",
                    message=f"I couldn't list cameras. {e}",
                    say_hint="Explain that camera listing failed.",
                )

            compact_cams = self._compact_cameras(cams)
            picked = self._pick_cameras_for_area(compact_cams, target or query)
            if not picked:
                return action_failure(
                    code="area_not_found",
                    message=f"No cameras matched area: {target or query}",
                    needs=["Specify an area such as front yard, back yard, porch, or garage."],
                    say_hint="Explain no area cameras matched and ask for a clearer area.",
                )

            # We'll do up to 6 snapshots max (keeps it reasonable)
            picked = picked[:6]

            images_out = []
            camera_descriptions = []
            snapshot_results = []

            for cam in picked:
                cam_id = _coerce_str(cam.get("id"))
                cam_name = _coerce_str(cam.get("name") or "Camera")

                try:
                    img_bytes, mimetype = client.get_camera_snapshot(cam_id)

                    # Vision describe
                    vision_prompt = self._vision_prompt_for_query(query, cam_name)
                    try:
                        vision_text = await asyncio.to_thread(
                            client.call_vision, img_bytes, prompt=vision_prompt, filename=f"{cam_name}.jpg"
                        )
                    except Exception as e:
                        vision_text = f"(vision error) {e}"

                    camera_descriptions.append({
                        "camera": cam_name,
                        "state": cam.get("state"),
                        "vision": vision_text,
                    })
                    snapshot_results.append({
                        "camera": cam_name,
                        "mimetype": mimetype,
                        "bytes": len(img_bytes),
                        "ok": True,
                    })

                    if _platform_supports_media(platform):
                        images_out.append(
                            _build_media_metadata(
                                img_bytes,
                                media_type="image",
                                name=f"{cam_name}.jpg",
                                mimetype=mimetype,
                            )
                        )

                except Exception as e:
                    logger.info(f"[unifi_protect_camera] snapshot/vision failed for {cam_name}: {e}")
                    camera_descriptions.append({
                        "camera": cam_name,
                        "state": cam.get("state"),
                        "vision": f"(snapshot error) {e}",
                    })
                    snapshot_results.append({
                        "camera": cam_name,
                        "ok": False,
                        "error": str(e),
                        "state": cam.get("state"),
                    })

            facts = {
                "area": target or "",
                "cameras_used": [c.get("name") for c in picked],
                "camera_descriptions": camera_descriptions,
                "snapshot_results": snapshot_results,
            }
            summary = self._summary_from_facts(query, facts)

            if _platform_supports_media(platform):
                return action_success(
                    facts=facts,
                    summary_for_user=summary,
                    say_hint="Summarize area snapshots and notable activity from the provided facts.",
                    artifacts=images_out,
                )
            return action_success(
                facts=facts,
                summary_for_user=summary,
                say_hint="Summarize area snapshots and notable activity from the provided facts.",
            )

        # Unknown action
        return action_failure(
            code="invalid_action",
            message="I couldn't figure out what to do with that request.",
            say_hint="Ask the user to choose sensors status, camera list, or a camera/area description.",
        )


verba = UniFiProtectCameraPlugin()
