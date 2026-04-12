# verba/voicepe_remote_timer.py
import asyncio
import json
import logging
import re
from typing import Any, Dict, Optional
from dotenv import load_dotenv
import requests

from verba_base import ToolVerba
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("voicepe_remote_timer")
logger.setLevel(logging.INFO)


class VoicePERemoteTimerPlugin(ToolVerba):
    """
    Voice PE Remote Timer (device-local, ESPHome-driven)

    Features:
      - Start a timer (device-local countdown + LEDs)
      - Ask how much time is left (reads remaining seconds sensor)
      - Cancel a running timer (press cancel button)

    Behavior:
      - If a timer is already running, starting a new one is BLOCKED.
        The user must cancel first.
    """

    name = "voicepe_remote_timer"
    verba_name = "Voice PE Remote Timer"
    version = "1.1.5"
    min_tater_version = "59"
    pretty_name = "Voice PE Remote Timer"
    settings_category = "Voice PE Remote Timer"

    usage = (
        '{"function":"voicepe_remote_timer","arguments":{"query":"ONE natural-language Voice PE timer request '
        '(for example: set a timer for 5 minutes, how much time is left, cancel the timer)."}}'
    )

    description = (
        "Start, cancel, or check the remaining time for a device-local Voice PE timer from one natural-language request."
    )
    verba_dec = "Start, cancel, or check a Voice PE (ESPHome) timer device."

    required_settings = {
        "MAX_SECONDS": {
            "label": "Max Seconds",
            "type": "number",
            "default": 7200,
            "description": "Clamp very large durations (default 2 hours).",
        },
        "TIMER_SECONDS_ENTITY": {
            "label": "Timer Seconds Entity (optional)",
            "type": "string",
            "default": "",
            "description": "Explicit number entity for timer seconds, e.g. number.voicepe_office_remote_timer_seconds.",
        },
        "START_BUTTON_ENTITY": {
            "label": "Start Button Entity (optional)",
            "type": "string",
            "default": "",
            "description": "Explicit button entity to start timer, e.g. button.voicepe_office_remote_timer_start.",
        },
        "CANCEL_BUTTON_ENTITY": {
            "label": "Cancel Button Entity (optional)",
            "type": "string",
            "default": "",
            "description": "Explicit button entity to cancel timer, e.g. button.voicepe_office_remote_timer_cancel.",
        },
        "REMAINING_SENSOR_ENTITY": {
            "label": "Remaining Sensor Entity (optional)",
            "type": "string",
            "default": "",
            "description": "Explicit sensor entity for remaining seconds, e.g. sensor.voicepe_office_remote_timer_remaining_seconds.",
        },
        "RUNNING_SENSOR_ENTITY": {
            "label": "Running Sensor Entity (optional)",
            "type": "string",
            "default": "",
            "description": "Explicit binary_sensor entity for running state, e.g. binary_sensor.voicepe_office_remote_timer_running.",
        },
        # (optional): if your entity ids follow a predictable pattern, you can override this.
        "VOICEPE_ENTITY_PREFIX": {
            "label": "Voice PE entity prefix (optional)",
            "type": "string",
            "default": "",
            "description": (
                "Optional prefix used when inferring entities from the speaking device. "
                "Example: 'voicepe_office' would produce number.voicepe_office_remote_timer_seconds, etc. "
                "If blank, we will slugify context.device_name."
            ),
        },
    }

    waiting_prompt_template = (
        "Write a short friendly message telling {mention} you’re working on the Voice PE timer now. "
        "Only output that message."
    )

    platforms = ['voice_core', 'homeassistant', 'homekit', 'xbmc', 'webui', 'macos', 'discord', 'telegram', 'matrix', 'irc']
    when_to_use = "Use when the user wants to start a timer, cancel a timer, or ask how much time is left on a Voice PE timer."
    how_to_use = (
        "Pass one natural-language timer request in query. Include the duration naturally for start requests. "
        "For status checks or cancel requests, no duration is needed."
    )
    common_needs = ["A natural-language timer request."]
    missing_info_prompts = []
    example_calls = [
        '{"function":"voicepe_remote_timer","arguments":{"query":"set a timer for 5 minutes"}}',
        '{"function":"voicepe_remote_timer","arguments":{"query":"how much time is left on the timer"}}',
        '{"function":"voicepe_remote_timer","arguments":{"query":"cancel the timer"}}',
        '{"function":"voicepe_remote_timer","arguments":{"query":"start a 90 second timer"}}',
    ]


    # ─────────────────────────────────────────────────────────────
    # Settings / HA helpers
    # ─────────────────────────────────────────────────────────────

    def _get_settings(self) -> dict:
        return (
            redis_client.hgetall(f"verba_settings:{self.settings_category}")
            or redis_client.hgetall(f"verba_settings: {self.settings_category}")
            or {}
        )

    def _ha_settings(self) -> dict:
        settings = redis_client.hgetall("homeassistant_settings") or {}
        base_url = (settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (settings.get("HA_TOKEN") or "").strip()
        return {"base_url": base_url, "token": token}

    def _ha_headers(self, token: str) -> dict:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _post_service(self, ha_base: str, token: str, domain: str, service: str, data: dict) -> None:
        url = f"{ha_base}/api/services/{domain}/{service}"
        r = requests.post(url, headers=self._ha_headers(token), json=data, timeout=15)
        if r.status_code >= 400:
            raise RuntimeError(f"{domain}.{service} failed: {r.status_code} {r.text}")

    def _get_state(self, ha_base: str, token: str, entity_id: str) -> dict:
        url = f"{ha_base}/api/states/{entity_id}"
        r = requests.get(url, headers=self._ha_headers(token), timeout=15)
        if r.status_code >= 400:
            raise RuntimeError(f"GET state failed for {entity_id}: {r.status_code} {r.text}")
        return r.json() if r.text else {}

    def _list_states(self, ha_base: str, token: str) -> list[dict]:
        url = f"{ha_base}/api/states"
        r = requests.get(url, headers=self._ha_headers(token), timeout=20)
        if r.status_code >= 400:
            raise RuntimeError(f"GET states failed: {r.status_code} {r.text}")
        return r.json() if r.text else []

    # ─────────────────────────────────────────────────────────────
    # Entity inference (Voice context)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _slugify(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"[^a-z0-9]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    @staticmethod
    def _ctx_text(context: dict | None, *keys: str) -> str:
        ctx = context if isinstance(context, dict) else {}
        origin = ctx.get("origin") if isinstance(ctx.get("origin"), dict) else {}
        for key in keys:
            raw = ctx.get(key)
            if raw in (None, ""):
                raw = origin.get(key)
            txt = str(raw or "").strip()
            if txt:
                return txt
        return ""

    @staticmethod
    def _ha_ws_url(base_url: str) -> str:
        base = str(base_url or "").strip().rstrip("/")
        if base.startswith("https://"):
            return base.replace("https://", "wss://", 1) + "/api/websocket"
        return base.replace("http://", "ws://", 1) + "/api/websocket"

    async def _ha_ws_call(self, ha_base: str, token: str, msg_type: str, timeout_s: float = 20.0):
        try:
            import aiohttp
        except Exception as e:
            raise RuntimeError(f"aiohttp is required for Home Assistant websocket calls: {e}")

        ws_url = self._ha_ws_url(ha_base)
        timeout = aiohttp.ClientTimeout(total=timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(ws_url, heartbeat=30) as ws:
                first = await ws.receive_json()
                if (first or {}).get("type") == "auth_required":
                    await ws.send_json({"type": "auth", "access_token": token})
                    auth = await ws.receive_json()
                    if (auth or {}).get("type") != "auth_ok":
                        raise RuntimeError(f"HA websocket auth failed: {auth}")
                elif (first or {}).get("type") != "auth_ok":
                    await ws.send_json({"type": "auth", "access_token": token})
                    auth = await ws.receive_json()
                    if (auth or {}).get("type") != "auth_ok":
                        raise RuntimeError(f"Unexpected HA websocket hello/auth flow: first={first}, auth={auth}")

                await ws.send_json({"id": 1, "type": msg_type})
                while True:
                    msg = await ws.receive_json()
                    if msg.get("type") != "result" or msg.get("id") != 1:
                        continue
                    if not msg.get("success", False):
                        raise RuntimeError(f"HA websocket call failed: {msg}")
                    return msg.get("result")

    @staticmethod
    def _pick_best_entity(
        entity_ids: list[str],
        domain: str,
        required_parts: list[str],
        optional_parts: list[str] | None = None,
        avoid_parts: list[str] | None = None,
    ) -> str | None:
        """
        Score entity ids and return best match for a timer role.
        """
        opts = optional_parts or []
        avoids = avoid_parts or []
        best = None
        best_score = -1
        for eid in entity_ids or []:
            eid_text = str(eid or "").strip()
            if not eid_text.startswith(domain + "."):
                continue
            low = eid_text.lower()
            if any(part not in low for part in required_parts):
                continue
            if any(part in low for part in avoids):
                continue
            score = 100 + (10 * len(required_parts))
            for part in opts:
                if part in low:
                    score += 4
            if "remote_timer" in low:
                score += 6
            if "_timer_" in low:
                score += 3
            if score > best_score:
                best_score = score
                best = eid_text
        return best

    def _bundle_timer_entities_from_ids(self, entity_ids: list[str]) -> dict | None:
        ids = [str(x or "").strip() for x in (entity_ids or []) if str(x or "").strip()]
        if not ids:
            return None

        seconds = (
            self._pick_best_entity(ids, "number", ["remote", "timer", "seconds"])
            or self._pick_best_entity(ids, "number", ["timer", "seconds"])
            or self._pick_best_entity(ids, "number", ["seconds"])
        )
        start = (
            self._pick_best_entity(ids, "button", ["remote", "timer", "start"], optional_parts=["press"], avoid_parts=["restart"])
            or self._pick_best_entity(ids, "button", ["timer", "start"], optional_parts=["press"], avoid_parts=["restart"])
            or self._pick_best_entity(ids, "button", ["start"], optional_parts=["timer", "press"], avoid_parts=["restart"])
        )
        cancel = (
            self._pick_best_entity(ids, "button", ["remote", "timer", "cancel"], optional_parts=["press"])
            or self._pick_best_entity(ids, "button", ["timer", "cancel"], optional_parts=["press"])
            or self._pick_best_entity(ids, "button", ["cancel"], optional_parts=["timer", "press"])
        )
        remaining = (
            self._pick_best_entity(ids, "sensor", ["remote", "timer", "remaining"], optional_parts=["seconds"])
            or self._pick_best_entity(ids, "sensor", ["timer", "remaining"], optional_parts=["seconds"])
            or self._pick_best_entity(ids, "sensor", ["remaining"], optional_parts=["timer", "seconds"])
        )
        running = (
            self._pick_best_entity(ids, "binary_sensor", ["remote", "timer", "running"])
            or self._pick_best_entity(ids, "binary_sensor", ["timer", "running"])
            or self._pick_best_entity(ids, "binary_sensor", ["running"], optional_parts=["timer"])
        )

        if not (seconds and start and cancel and remaining and running):
            return None
        return {
            "TIMER_SECONDS_ENTITY": seconds,
            "START_BUTTON_ENTITY": start,
            "CANCEL_BUTTON_ENTITY": cancel,
            "REMAINING_SENSOR_ENTITY": remaining,
            "RUNNING_SENSOR_ENTITY": running,
        }

    async def _infer_entities_from_device_registry(self, context: dict | None) -> dict | None:
        """
        Deterministic path: use speaking HA device_id and entity registry to grab the
        timer entities that belong to the same device.
        """
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]
        if not token:
            return None

        device_id = self._ctx_text(context, "device_id")
        if not device_id:
            return None

        try:
            entity_reg = await self._ha_ws_call(ha_base, token, "config/entity_registry/list", timeout_s=30.0)
        except Exception as e:
            logger.warning(f"[voicepe_remote_timer] device-registry lookup failed for device_id={device_id}: {e}")
            return None

        ids_on_device: list[str] = []
        for row in (entity_reg or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("device_id") or "").strip() != device_id:
                continue
            if row.get("disabled_by") not in (None, ""):
                continue
            eid = str(row.get("entity_id") or "").strip()
            if eid:
                ids_on_device.append(eid)

        resolved = self._bundle_timer_entities_from_ids(ids_on_device)
        if resolved:
            logger.info(
                "[voicepe_remote_timer] resolved entities via device registry for device_id=%s start=%s running=%s",
                device_id,
                resolved.get("START_BUTTON_ENTITY"),
                resolved.get("RUNNING_SENSOR_ENTITY"),
            )
        return resolved

    def _infer_entities_from_context(self, context: dict | None) -> dict | None:
        """
        Try to infer the correct entity ids based on the speaking device (context.device_name).

        Strategy:
          1) Determine a base prefix:
              - VOICEPE_ENTITY_PREFIX setting, OR slugified context.device_name
          2) Try common suffix patterns. We test existence via /api/states/<entity_id>.
          3) If that fails, scan all states and try fuzzy match by entity_id containing the prefix + suffix.

        Returns dict with keys:
          seconds_entity, start_entity, cancel_entity, remaining_entity, running_entity
        """
        s = self._get_settings()
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]
        if not token:
            return None

        device_name = self._ctx_text(context, "device_name", "device")
        if device_name.startswith("assist_satellite.") and "." in device_name:
            device_name = device_name.split(".", 1)[1]
        if not device_name and (s.get("VOICEPE_ENTITY_PREFIX") or "").strip():
            # if user set prefix, allow inference without device_name
            device_name = s.get("VOICEPE_ENTITY_PREFIX")

        prefix = (s.get("VOICEPE_ENTITY_PREFIX") or "").strip()
        if prefix:
            base = self._slugify(prefix)
        else:
            base = self._slugify(device_name)

        if not base:
            return None

        # Common patterns you might use across devices
        # Example target:
        #  number.voicepe_office_remote_timer_seconds
        #  button.voicepe_office_remote_timer_start
        #  button.voicepe_office_remote_timer_cancel
        #  sensor.voicepe_office_remote_timer_remaining_seconds
        #  binary_sensor.voicepe_office_remote_timer_running
        candidates = [
            {
                "seconds": f"number.{base}_remote_timer_seconds",
                "start": f"button.{base}_remote_timer_start",
                "cancel": f"button.{base}_remote_timer_cancel",
                "remaining": f"sensor.{base}_remote_timer_remaining_seconds",
                "running": f"binary_sensor.{base}_remote_timer_running",
            },
            # Alternative: some folks keep "voicepe_" prefix already in base, or use "timer_" names
            {
                "seconds": f"number.{base}_timer_seconds",
                "start": f"button.{base}_timer_start",
                "cancel": f"button.{base}_timer_cancel",
                "remaining": f"sensor.{base}_timer_remaining_seconds",
                "running": f"binary_sensor.{base}_timer_running",
            },
        ]

        # Fast path: verify one of the candidate sets exists (running sensor is a good test)
        def exists(eid: str) -> bool:
            try:
                st = self._get_state(ha_base, token, eid)
                return bool(st and st.get("entity_id") == eid)
            except Exception:
                return False

        for c in candidates:
            if exists(c["running"]) and exists(c["remaining"]) and exists(c["start"]) and exists(c["cancel"]) and exists(c["seconds"]):
                return {
                    "TIMER_SECONDS_ENTITY": c["seconds"],
                    "START_BUTTON_ENTITY": c["start"],
                    "CANCEL_BUTTON_ENTITY": c["cancel"],
                    "REMAINING_SENSOR_ENTITY": c["remaining"],
                    "RUNNING_SENSOR_ENTITY": c["running"],
                }

        # Fallback: scan all states and attempt fuzzy matching by entity_id containing the base string.
        try:
            all_states = self._list_states(ha_base, token)
        except Exception:
            return None

        def find(domain: str, contains: list[str]) -> str | None:
            for st in all_states:
                eid = (st.get("entity_id") or "")
                if not eid.startswith(domain + "."):
                    continue
                low = eid.lower()
                ok = True
                for part in contains:
                    if part not in low:
                        ok = False
                        break
                if ok:
                    return eid
            return None

        # Try to find each required entity by pieces
        seconds_eid = find("number", [base, "seconds"]) or find("number", [base, "timer", "seconds"])
        start_eid = find("button", [base, "start"]) or find("button", [base, "timer", "start"])
        cancel_eid = find("button", [base, "cancel"]) or find("button", [base, "timer", "cancel"])
        remaining_eid = find("sensor", [base, "remaining"]) or find("sensor", [base, "timer", "remaining"])
        running_eid = find("binary_sensor", [base, "running"]) or find("binary_sensor", [base, "timer", "running"])

        if seconds_eid and start_eid and cancel_eid and remaining_eid and running_eid:
            return {
                "TIMER_SECONDS_ENTITY": seconds_eid,
                "START_BUTTON_ENTITY": start_eid,
                "CANCEL_BUTTON_ENTITY": cancel_eid,
                "REMAINING_SENSOR_ENTITY": remaining_eid,
                "RUNNING_SENSOR_ENTITY": running_eid,
            }

        return None

    @staticmethod
    def _clean_entity_id(value: Any) -> str:
        return str(value or "").strip()

    def _explicit_entities_from_settings(self) -> Optional[Dict[str, str]]:
        s = self._get_settings() or {}
        out = {
            "TIMER_SECONDS_ENTITY": self._clean_entity_id(s.get("TIMER_SECONDS_ENTITY")),
            "START_BUTTON_ENTITY": self._clean_entity_id(s.get("START_BUTTON_ENTITY")),
            "CANCEL_BUTTON_ENTITY": self._clean_entity_id(s.get("CANCEL_BUTTON_ENTITY")),
            "REMAINING_SENSOR_ENTITY": self._clean_entity_id(s.get("REMAINING_SENSOR_ENTITY")),
            "RUNNING_SENSOR_ENTITY": self._clean_entity_id(s.get("RUNNING_SENSOR_ENTITY")),
        }
        if all(out.values()):
            return out
        return None

    def _missing_entities_message(self, context: dict | None = None) -> str:
        dev = self._ctx_text(context, "device_name", "device_id")
        area = self._ctx_text(context, "area_name", "area_id")
        hint = ""
        if dev or area:
            hint = f" (I heard you from device={dev or 'unknown'}, area={area or 'unknown'}.)"
        return (
            "Voice PE Remote Timer couldn't determine which device timer entities to use."
            f"{hint} "
            "Either set the timer entity IDs in the plugin settings, or ensure your Voice PE device "
            "exposes timer entities in Home Assistant."
        )

    async def _resolve_entities(self, context: dict | None = None) -> dict | None:
        explicit = self._explicit_entities_from_settings()
        if explicit:
            return explicit
        by_device = await self._infer_entities_from_device_registry(context)
        if by_device:
            return by_device
        return None

    # ─────────────────────────────────────────────────────────────
    # Duration parsing (forgiving)
    # ─────────────────────────────────────────────────────────────

    def _parse_integer_word_tokens(self, tokens: list[str]) -> int | None:
        """
        Parse integer-like number words (supports up to thousands).
        Examples:
          - ["two"] -> 2
          - ["twenty", "five"] -> 25
          - ["one", "hundred", "ten"] -> 110
        """
        if not tokens:
            return None

        units = {
            "zero": 0,
            "one": 1, "a": 1, "an": 1,
            "two": 2, "couple": 2,
            "three": 3, "few": 3,
            "four": 4,
            "five": 5,
            "six": 6,
            "seven": 7,
            "eight": 8,
            "nine": 9,
            "ten": 10,
            "eleven": 11,
            "twelve": 12, "dozen": 12,
            "thirteen": 13,
            "fourteen": 14,
            "fifteen": 15,
            "sixteen": 16,
            "seventeen": 17,
            "eighteen": 18,
            "nineteen": 19,
        }
        tens = {
            "twenty": 20,
            "thirty": 30,
            "forty": 40,
            "fifty": 50,
            "sixty": 60,
            "seventy": 70,
            "eighty": 80,
            "ninety": 90,
        }

        total = 0
        current = 0
        seen = False
        for tok in tokens:
            t = str(tok or "").strip().lower()
            if not t:
                continue
            if t in {"and", "of"}:
                continue
            if re.fullmatch(r"\d+", t):
                current += int(t)
                seen = True
                continue
            if t in units:
                current += units[t]
                seen = True
                continue
            if t in tens:
                current += tens[t]
                seen = True
                continue
            if t == "hundred":
                current = (current or 1) * 100
                seen = True
                continue
            if t == "thousand":
                total += (current or 1) * 1000
                current = 0
                seen = True
                continue
            return None

        if not seen:
            return None
        return total + current

    def _parse_amount_phrase(self, phrase: str) -> float | None:
        """
        Parse quantity phrases used before duration units.
        Handles numerals and common spoken forms:
          - "2", "2.5", "two", "twenty five"
          - "half", "quarter", "three quarters"
          - "one and a half", "two and a quarter"
        """
        text = str(phrase or "").strip().lower()
        if not text:
            return None

        text = text.replace("-", " ")
        text = re.sub(r"[^a-z0-9.\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return None

        def parse_fractional(raw: str) -> float | None:
            s = str(raw or "").strip()
            if not s:
                return None
            if re.fullmatch(r"\d+(?:\.\d+)?", s):
                try:
                    return float(s)
                except Exception:
                    return None

            direct = {
                "half": 0.5,
                "a half": 0.5,
                "one half": 0.5,
                "quarter": 0.25,
                "a quarter": 0.25,
                "one quarter": 0.25,
                "three quarters": 0.75,
                "three quarter": 0.75,
            }
            if s in direct:
                return direct[s]

            patterns = [
                (r"^(.+?)\s+and\s+a\s+half$", 0.5),
                (r"^(.+?)\s+and\s+half$", 0.5),
                (r"^(.+?)\s+and\s+a\s+quarter$", 0.25),
                (r"^(.+?)\s+and\s+quarter$", 0.25),
                (r"^(.+?)\s+and\s+three\s+quarters?$", 0.75),
            ]
            for pat, frac in patterns:
                m = re.match(pat, s)
                if not m:
                    continue
                base = m.group(1).strip()
                base_int = self._parse_integer_word_tokens(base.split())
                if base_int is not None:
                    return float(base_int) + frac
                try:
                    return float(base) + frac
                except Exception:
                    continue

            int_val = self._parse_integer_word_tokens(s.split())
            if int_val is not None:
                return float(int_val)
            return None

        tokens = text.split()
        for i in range(len(tokens)):
            suffix = " ".join(tokens[i:]).strip()
            val = parse_fractional(suffix)
            if val is not None:
                return val

        return parse_fractional(text)

    def _duration_components_from_text(self, text: str) -> list[tuple[float, int]]:
        """
        Extract duration components from free text as (amount, unit_seconds).
        """
        src = str(text or "").strip().lower()
        if not src:
            return []

        src = src.replace("-", " ")
        src = re.sub(r"\s+", " ", src).strip()

        unit_seconds = {
            "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
            "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
            "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
        }
        unit_pattern = r"(hours?|hrs?|hr|h|minutes?|mins?|min|m|seconds?|secs?|sec|s)"
        amount_pattern = r"(\d+(?:\.\d+)?|[a-z]+(?:\s+[a-z]+){0,7})"
        pattern = re.compile(rf"\b{amount_pattern}\s*{unit_pattern}\b")

        parts: list[tuple[float, int]] = []
        for m in pattern.finditer(src):
            amount_raw = (m.group(1) or "").strip()
            unit_raw = (m.group(2) or "").strip().lower()
            amt = self._parse_amount_phrase(amount_raw)
            if amt is None or amt <= 0:
                continue
            mult = unit_seconds.get(unit_raw)
            if not mult:
                continue
            parts.append((amt, mult))

        return parts

    @staticmethod
    def _merged_request_text(args: Dict[str, Any]) -> str:
        a = args if isinstance(args, dict) else {}
        text_fields = (
            a.get("request"),
            a.get("query"),
            a.get("message"),
            a.get("prompt"),
            a.get("text"),
            a.get("content"),
            a.get("duration"),
        )
        merged = " ".join(str(v or "").strip() for v in text_fields if str(v or "").strip()).strip().lower()
        return re.sub(r"\s+", " ", merged).strip()

    def _parse_duration_to_seconds(self, text: str, max_seconds: int) -> int:
        t = (text or "").strip().lower()
        if not t:
            return 0

        t = re.sub(r"[,;]+", " ", t)
        t = re.sub(r"[()]+", " ", t)
        t = t.replace(".", " ")
        t = re.sub(r"\s+", " ", t).strip()

        if re.fullmatch(r"\d+", t):
            return max(0, min(int(t), max_seconds))

        if re.fullmatch(r"\d{1,2}:\d{2}", t):
            mm, ss = t.split(":")
            total = int(mm) * 60 + int(ss)
            return max(0, min(total, max_seconds))

        t_spaced = re.sub(r"(\d)([a-z])", r"\1 \2", t)
        t_spaced = re.sub(r"([a-z])(\d)", r"\1 \2", t_spaced)
        t_spaced = re.sub(r"\s+", " ", t_spaced).strip()

        total = 0.0
        for amount, mult in self._duration_components_from_text(t_spaced):
            total += float(amount) * int(mult)

        if total > 0:
            return max(0, min(int(round(total)), max_seconds))

        # If we got only a plain number token, interpret as seconds.
        if re.fullmatch(r"\d+(?:\.\d+)?", t_spaced):
            try:
                return max(0, min(int(round(float(t_spaced))), max_seconds))
            except Exception:
                return 0

        return 0

    def _format_remaining(self, seconds: int) -> str:
        seconds = max(0, int(seconds))
        if seconds < 60:
            return f"{seconds} seconds"
        m = seconds // 60
        s = seconds % 60
        if s == 0:
            return f"{m} minute" + ("s" if m != 1 else "")
        return f"{m}m {s}s"

    def _extract_duration_from_text(self, text: str) -> str:
        src = str(text or "").strip().lower()
        if not src:
            return ""

        hhmm = re.search(r"\b(\d{1,2}:\d{2})\b", src)
        if hhmm:
            return hhmm.group(1)

        if self._duration_components_from_text(src):
            return src

        if "timer" in src:
            n_for = re.search(r"\bfor\s+(\d{1,4})\b", src)
            if n_for:
                return f"{n_for.group(1)} minutes"
            n_plain = re.search(r"\b(\d{1,4})\b", src)
            if n_plain:
                return f"{n_plain.group(1)} minutes"
        return ""

    def _resolve_action_duration(self, args: Dict[str, Any]) -> tuple[str, str]:
        a = args if isinstance(args, dict) else {}
        action = str(a.get("action") or "").strip().lower()
        duration = str(a.get("duration") or "").strip()
        if duration:
            return action, duration

        merged = self._merged_request_text(a)
        if not merged:
            return action, ""

        if not action:
            if re.search(r"\b(cancel|stop|clear|end)\b", merged):
                action = "cancel"
            elif re.search(r"\b(status|remaining|left|how much time|time left|check timer)\b", merged):
                action = "status"
            elif re.search(r"\b(start|set)\b", merged) and "timer" in merged:
                action = "start"

        inferred_duration = self._extract_duration_from_text(merged)
        if inferred_duration:
            duration = inferred_duration
        return action, duration

    # ─────────────────────────────────────────────────────────────
    # LLM phrasing helpers
    # ─────────────────────────────────────────────────────────────

    async def _llm_phrase(self, llm_client, prompt: str, fallback: str, max_chars: int = 240) -> str:
        text = re.sub(r"[`*_]{1,3}", "", str(fallback or ""))
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]

    async def _llm_time_left_message(self, remaining_seconds: int, llm_client) -> str:
        remaining_text = self._format_remaining(remaining_seconds)
        fallback = f"You've got {remaining_text} left on the timer."
        prompt = (
            "The user asked how much time is left on a timer.\n\n"
            f"Remaining time: {remaining_text}\n\n"
            "Write ONE short, natural sentence answering the user.\n"
            "Rules:\n- No emojis\n- No markdown\n- Friendly but concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_no_timer_message(self, llm_client) -> str:
        fallback = "No timer is currently running."
        prompt = (
            "The user asked about a timer.\n\n"
            "Fact: There is no timer currently running.\n\n"
            "Write ONE short, natural sentence telling the user.\n"
            "Rules:\n- No emojis\n- No markdown\n- Friendly but concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_started_message(self, seconds: int, llm_client) -> str:
        dur = self._format_remaining(seconds)
        fallback = f"Timer started for {dur}."
        prompt = (
            "The user asked you to start a timer.\n\n"
            f"Fact: The timer has been started for {dur}.\n\n"
            "Write ONE short, friendly confirmation sentence.\n"
            "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_cancelled_message(self, llm_client) -> str:
        fallback = "Timer cancelled."
        prompt = (
            "The user asked you to cancel a timer.\n\n"
            "Fact: The timer has been cancelled.\n\n"
            "Write ONE short, friendly confirmation sentence.\n"
            "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_cancel_nothing_message(self, llm_client) -> str:
        fallback = "No timer is currently running."
        prompt = (
            "The user asked you to cancel a timer.\n\n"
            "Fact: There is no timer running.\n\n"
            "Write ONE short, friendly sentence telling the user there's nothing to cancel.\n"
            "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_block_new_timer_message(self, remaining_seconds: int | None, llm_client) -> str:
        if remaining_seconds is None:
            fallback = "A timer is already running. Cancel it first if you want to start a new one."
            prompt = (
                "The user asked to start a timer, but a timer is already running.\n\n"
                "Write ONE short, friendly sentence telling the user they need to cancel the current timer first.\n"
                "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
                "Only output the sentence."
            )
            return await self._llm_phrase(llm_client, prompt, fallback)

        remaining_text = self._format_remaining(remaining_seconds)
        fallback = f"A timer is already running with {remaining_text} remaining. Cancel it first if you want to start a new one."
        prompt = (
            "The user asked to start a timer, but a timer is already running.\n\n"
            f"Fact: Remaining time on current timer: {remaining_text}.\n\n"
            "Write ONE short, friendly sentence telling the user a timer is already running and they need to cancel it first to start another.\n"
            "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_infer_duration_seconds(self, request_text: str, llm_client, max_seconds: int) -> int | None:
        """
        Last-resort parser for odd/natural duration phrases.
        Returns clamped seconds or None.
        """
        if llm_client is None:
            return None

        text = str(request_text or "").strip()
        if not text:
            return None

        system_prompt = (
            "Extract the timer duration from the user request.\n"
            "Return ONLY JSON in this exact shape:\n"
            '{"seconds": <integer|null>}\n'
            "Rules:\n"
            "- Parse natural language durations (e.g. 'two minutes', 'one and a half hours', '90 seconds').\n"
            "- If no duration is present, use null.\n"
            "- If a bare number appears in a timer request, interpret it as minutes.\n"
            "- No explanation text, no markdown, JSON only."
        )
        user_prompt = f"Request: {text}"

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
            )
            content = (resp.get("message") or {}).get("content", "").strip()
            try:
                parsed = json.loads(content)
            except Exception:
                m = re.search(r"\{.*\}", content, re.S)
                if not m:
                    return None
                parsed = json.loads(m.group(0))

            if not isinstance(parsed, dict):
                return None
            value = parsed.get("seconds")
            if value in (None, "", False):
                return None
            seconds = int(round(float(value)))
            if seconds <= 0:
                return None
            return max(1, min(seconds, int(max_seconds)))
        except Exception as e:
            logger.debug(f"[voicepe_remote_timer] LLM duration parse failed: {e}")
            return None

    # ─────────────────────────────────────────────────────────────
    # Read timer state (running + remaining)
    # ─────────────────────────────────────────────────────────────

    async def _is_timer_running(self, context: dict | None = None, entities: dict | None = None) -> bool | None:
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]

        ents = entities or await self._resolve_entities(context=context)
        if not token or not ents:
            return None

        running_entity = (ents.get("RUNNING_SENSOR_ENTITY") or "").strip()
        if not running_entity:
            return None

        def do_get():
            st = self._get_state(ha_base, token, running_entity)
            raw = (st.get("state") or "").strip().lower()
            if raw in ("on", "true", "1", "yes"):
                return True
            if raw in ("off", "false", "0", "no"):
                return False
            return None

        try:
            return await asyncio.to_thread(do_get)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] Failed reading running sensor: {e}")
            return None

    async def _get_remaining_seconds(self, context: dict | None = None, entities: dict | None = None) -> int | None:
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]

        ents = entities or await self._resolve_entities(context=context)
        if not token or not ents:
            return None

        remaining_entity = (ents.get("REMAINING_SENSOR_ENTITY") or "").strip()
        if not remaining_entity:
            return None

        def do_get():
            st = self._get_state(ha_base, token, remaining_entity)
            raw = st.get("state")
            try:
                return int(float(raw))
            except Exception:
                return None

        try:
            return await asyncio.to_thread(do_get)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] Failed reading remaining sensor: {e}")
            return None

    # ─────────────────────────────────────────────────────────────
    # Actions: start / status / cancel
    # ─────────────────────────────────────────────────────────────

    async def _start_timer(self, duration_text: str, llm_client, context: dict | None = None) -> str:
        s = self._get_settings()
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]
        if not token:
            return (
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        ents = await self._resolve_entities(context=context)
        if not ents:
            return self._missing_entities_message(context)

        seconds_entity = (ents.get("TIMER_SECONDS_ENTITY") or "").strip()
        start_entity = (ents.get("START_BUTTON_ENTITY") or "").strip()
        remaining_entity = (ents.get("REMAINING_SENSOR_ENTITY") or "").strip()
        running_entity = (ents.get("RUNNING_SENSOR_ENTITY") or "").strip()
        if not seconds_entity or not start_entity or not remaining_entity or not running_entity:
            return "Voice PE Remote Timer is missing required entity IDs."

        try:
            max_seconds = int(s.get("MAX_SECONDS") or 7200)
        except Exception:
            max_seconds = 7200

        new_seconds = self._parse_duration_to_seconds(duration_text, max_seconds)
        if new_seconds <= 0:
            llm_seconds = await self._llm_infer_duration_seconds(duration_text, llm_client, max_seconds)
            if llm_seconds:
                new_seconds = llm_seconds
        if new_seconds <= 0:
            return "Please provide a valid timer duration (examples: 20s, 2min, 5 minutes, 1h 10m)."

        running = await self._is_timer_running(context=context, entities=ents)
        if running is None:
            return "I couldn't read the Voice PE running sensor (check plugin settings)."

        if running:
            remaining = await self._get_remaining_seconds(context=context, entities=ents)
            return (await self._llm_block_new_timer_message(remaining, llm_client)).strip()

        def do_calls():
            self._post_service(
                ha_base, token, "number", "set_value",
                {"entity_id": seconds_entity, "value": new_seconds},
            )
            self._post_service(
                ha_base, token, "button", "press",
                {"entity_id": start_entity},
            )

        try:
            await asyncio.to_thread(do_calls)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] HA start calls failed: {e}")
            return "Failed to start the Voice PE timer (Home Assistant service call error)."

        return (await self._llm_started_message(new_seconds, llm_client)).strip()

    async def _status(self, llm_client, context: dict | None = None) -> str:
        ha = self._ha_settings()
        if not ha["token"]:
            return (
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
        ents = await self._resolve_entities(context=context)
        running = await self._is_timer_running(context=context, entities=ents)
        if running is None:
            if not ents:
                return self._missing_entities_message(context)
            return "I couldn't read the Voice PE running sensor (check plugin settings)."

        if not running:
            return (await self._llm_no_timer_message(llm_client)).strip()

        remaining = await self._get_remaining_seconds(context=context, entities=ents)
        if remaining is None:
            fallback = "A timer is running, but I couldn't read the remaining time."
            return (await self._llm_phrase(
                llm_client,
                "The user asked how much time is left on a timer.\n\n"
                "Fact: A timer is running, but remaining time is unavailable.\n\n"
                "Write ONE short sentence explaining that.\n"
                "Rules:\n- No emojis\n- No markdown\n- Friendly but concise\n"
                "Only output the sentence.",
                fallback
            )).strip()

        if remaining <= 0:
            fallback = "The timer is running, but it’s at the end right now."
            return (await self._llm_phrase(
                llm_client,
                "The user asked how much time is left on a timer.\n\n"
                "Fact: The timer is running but remaining time is 0 (end/transition).\n\n"
                "Write ONE short, natural sentence explaining it.\n"
                "Rules:\n- No emojis\n- No markdown\n- Friendly but concise\n"
                "Only output the sentence.",
                fallback
            )).strip()

        return (await self._llm_time_left_message(remaining, llm_client)).strip()

    async def _cancel(self, llm_client, context: dict | None = None) -> str:
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]
        if not token:
            return (
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        ents = await self._resolve_entities(context=context)
        if not ents:
            return self._missing_entities_message(context)

        cancel_entity = (ents.get("CANCEL_BUTTON_ENTITY") or "").strip()
        running_entity = (ents.get("RUNNING_SENSOR_ENTITY") or "").strip()
        if not cancel_entity or not running_entity:
            return "Voice PE Remote Timer is missing CANCEL_BUTTON_ENTITY / RUNNING_SENSOR_ENTITY."

        running = await self._is_timer_running(context=context, entities=ents)
        if running is None:
            return "I couldn't read the Voice PE running sensor (check plugin settings)."
        if not running:
            return (await self._llm_cancel_nothing_message(llm_client)).strip()

        def do_cancel():
            self._post_service(
                ha_base, token, "button", "press",
                {"entity_id": cancel_entity},
            )

        try:
            await asyncio.to_thread(do_cancel)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] HA cancel call failed: {e}")
            return "Failed to cancel the Voice PE timer (Home Assistant service call error)."

        return (await self._llm_cancelled_message(llm_client)).strip()

    # ─────────────────────────────────────────────────────────────
    # Main dispatcher
    # ─────────────────────────────────────────────────────────────

    async def _handle(self, args, llm_client, context: dict | None = None) -> str:
        args = args or {}
        action, duration = self._resolve_action_duration(args)
        merged_request = self._merged_request_text(args)

        if action in ("cancel", "stop", "clear"):
            return await self._cancel(llm_client, context=context)

        if action in ("status", "check", "remaining", "time_left"):
            return await self._status(llm_client, context=context)

        if action in ("start", "set") and not duration:
            try:
                max_seconds = int((self._get_settings() or {}).get("MAX_SECONDS") or 7200)
            except Exception:
                max_seconds = 7200

            llm_seconds = await self._llm_infer_duration_seconds(merged_request, llm_client, max_seconds)
            if llm_seconds:
                duration = str(llm_seconds)
            else:
                return "Please provide a valid timer duration (examples: 20s, 2min, 5 minutes, 1h 10m)."

        if not duration:
            return await self._status(llm_client, context=context)

        return await self._start_timer(duration, llm_client, context=context)

    # ─────────────────────────────────────────────────────────────
    # Platform handlers (hybrid-safe webui)
    # ─────────────────────────────────────────────────────────────

    async def handle_homeassistant(self, args, llm_client, context: dict | None = None):
        return (await self._handle(args, llm_client, context=context)).strip()
    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        try:
            return await self.handle_homeassistant(args=args, llm_client=llm_client, context=context)
        except TypeError:
            try:
                return await self.handle_homeassistant(args=args, llm_client=llm_client)
            except TypeError:
                return await self.handle_homeassistant(args, llm_client)


    async def handle_homekit(self, args, llm_client, context: dict | None = None):
        return (await self._handle(args, llm_client, context=context)).strip()

    async def handle_xbmc(self, args, llm_client, context: dict | None = None):
        return (await self._handle(args, llm_client, context=context)).strip()

    async def handle_webui(self, args, llm_client, context: dict | None = None):
        args = args or {}

        async def inner():
            return await self._handle(args, llm_client, context=context)

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
    async def handle_discord(self, message, args, llm_client):
        return await self.handle_webui(args, llm_client, context=None)

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args, llm_client, context=None)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self.handle_webui(args, llm_client, context=None)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self.handle_webui(args, llm_client, context=None)


verba = VoicePERemoteTimerPlugin()
