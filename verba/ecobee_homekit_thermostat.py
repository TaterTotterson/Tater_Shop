import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from tateros import integration_store as integration_store_module
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("ecobee_homekit_thermostat")
logger.setLevel(logging.INFO)


def _homekit_module():
    module = integration_store_module.integration_module("homekit")
    if module is None:
        raise RuntimeError("Ecobee HomeKit integration is not enabled.")
    return module


def list_homekit_thermostats(*args, **kwargs):
    return _homekit_module().list_homekit_thermostats(*args, **kwargs)


def set_homekit_thermostat_mode(*args, **kwargs):
    return _homekit_module().set_homekit_thermostat_mode(*args, **kwargs)


def set_homekit_thermostat_temperature(*args, **kwargs):
    return _homekit_module().set_homekit_thermostat_temperature(*args, **kwargs)


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


class EcobeeHomeKitThermostatPlugin(ToolVerba):
    name = "ecobee_homekit_thermostat"
    verba_name = "Ecobee HomeKit Thermostat"
    version = "1.0.3"
    min_tater_version = "59"
    pretty_name = "Ecobee HomeKit Thermostat"
    settings_category = "Ecobee (HomeKit)"
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
    tags = ["ecobee", "homekit", "thermostat", "climate"]
    routing_keywords = ["ecobee", "thermostat", "temperature", "heat", "cool", "hvac"]
    forced_route = "climate"
    forced_domain_hint = "ecobee homekit thermostat"

    usage = '{"function":"ecobee_homekit_thermostat","arguments":{"query":"set the ecobee to 72"}}'
    description = "Control Ecobee thermostats paired directly through HomeKit."
    verba_dec = "Control Ecobee thermostats through HomeKit pairing."
    when_to_use = "Use for Ecobee thermostat status, target temperature, and HVAC mode changes without Home Assistant."
    how_to_use = "Pass a natural-language thermostat request with an optional thermostat name, mode, and setpoint."
    common_needs = ["Thermostat action: status, set temperature, or set mode."]
    missing_info_prompts = ["Which Ecobee thermostat should I control?"]
    example_calls = [
        '{"function":"ecobee_homekit_thermostat","arguments":{"query":"what is the ecobee set to"}}',
        '{"function":"ecobee_homekit_thermostat","arguments":{"query":"set the ecobee to 72"}}',
        '{"function":"ecobee_homekit_thermostat","arguments":{"query":"set the ecobee to cool mode"}}',
    ]

    required_settings = {}

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are checking or controlling the Ecobee now. "
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
                    return parsed
            except Exception:
                pass
            return {"query": text}
        return {}

    def _normalize_mode(self, value: Any, query: str = "") -> str:
        explicit = self._coerce_text(value).lower().replace("-", "_").replace(" ", "_")
        if explicit in {"off", "heat", "cool", "auto", "heat_cool"}:
            return explicit
        if explicit in {"heating", "heater"}:
            return "heat"
        if explicit in {"cooling", "ac", "air_conditioning"}:
            return "cool"

        text = f" {self._coerce_text(query).lower()} "
        if re.search(r"\b(turn|set|switch)\s+(the\s+)?[^.]*\boff\b", text) or re.search(r"\boff\s+mode\b", text):
            return "off"
        if re.search(r"\bheat(ing)?\b", text):
            return "heat"
        if re.search(r"\bcool(ing)?\b|\bac\b|\bair conditioning\b", text):
            return "cool"
        if re.search(r"\bauto(matic)?\b|\bheat cool\b", text):
            return "auto"
        return ""

    def _temperature_unit(self, args: Dict[str, Any], query: str) -> str:
        explicit = self._coerce_text(args.get("temperature_unit") or args.get("unit")).lower()
        text = self._coerce_text(query).lower()
        if explicit in {"c", "celsius"} or "celsius" in text or "°c" in text:
            return "C"
        return "F"

    def _temperature_value(self, args: Dict[str, Any], query: str) -> Optional[float]:
        for key in ("temperature", "target_temperature", "setpoint", "target"):
            if key in args:
                try:
                    return float(args.get(key))
                except Exception:
                    pass
        text = self._coerce_text(query).lower()
        matches = re.findall(r"(?<![a-z0-9])(-?\d+(?:\.\d+)?)\s*(?:degrees?|deg|°)?\s*(?:f|c|fahrenheit|celsius)?", text)
        candidates: List[float] = []
        for raw in matches:
            try:
                number = float(raw)
            except Exception:
                continue
            if 5 <= number <= 100:
                candidates.append(number)
        return candidates[-1] if candidates else None

    def _normalize_action_value(self, value: Any) -> str:
        explicit = self._coerce_text(value).lower().replace("-", "_").replace(" ", "_")
        if explicit in {"status", "state", "get_state", "check"}:
            return "status"
        if explicit in {"set_temperature", "temperature", "set_temp", "setpoint"}:
            return "set_temperature"
        if explicit in {"set_hvac_mode", "mode", "hvac_mode", "turn_off", "turn_on"}:
            return "set_hvac_mode"
        return ""

    def _normalize_action(self, args: Dict[str, Any], query: str) -> str:
        explicit = self._normalize_action_value(args.get("action"))
        if explicit:
            return explicit

        has_temp = self._temperature_value(args, query) is not None
        has_mode = bool(self._normalize_mode(args.get("mode") or args.get("hvac_mode"), query))
        text = f" {self._coerce_text(query).lower()} "
        if has_temp and re.search(r"\b(set|make|put|change|raise|lower|to)\b", text):
            return "set_temperature"
        if has_mode and re.search(r"\b(set|turn|switch|change|mode)\b", text):
            return "set_hvac_mode"
        if has_temp:
            return "set_temperature"
        if re.search(r"\b(status|state|current|check|what|what's|whats|is|reading|set to)\b", text):
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

    async def _ai_pick_action(self, llm_client, query: str) -> Dict[str, Any]:
        if not llm_client:
            return {}
        text = self._coerce_text(query)
        if not text:
            return {}
        prompt = (
            "Choose the best Ecobee HomeKit thermostat action for the user request.\n"
            "Allowed actions: status, set_temperature, set_hvac_mode.\n"
            "Rules:\n"
            "1) Use status for questions about current temperature, setpoint, HVAC state, or mode.\n"
            "2) Use set_temperature when the user wants a numeric setpoint.\n"
            "3) Use set_hvac_mode when the user wants off, heat, cool, auto, or heat_cool mode.\n"
            "4) Include temperature, mode, and target when clearly present.\n"
            "Respond with JSON: "
            '{"action":"<allowed action or empty string>","temperature":"<number or empty string>","mode":"<off|heat|cool|auto|heat_cool or empty string>","target":"<thermostat target or empty string>"}'
            "\n\n"
            f'User request: "{text}"\n'
        )
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            raw = ((resp or {}).get("message") or {}).get("content", "")
            data = self._json_object_from_text(raw)
            return {
                "action": self._normalize_action_value(data.get("action")),
                "temperature": self._coerce_text(data.get("temperature")),
                "mode": self._normalize_mode(data.get("mode")),
                "target": self._coerce_text(data.get("target")),
            }
        except Exception as exc:
            logger.warning("[%s] _ai_pick_action failed: %s", self.name, exc)
            return {}

    def _target_text(self, args: Dict[str, Any], query: str) -> str:
        target = self._coerce_text(args.get("target") or args.get("thermostat") or args.get("thermostat_name"))
        if target:
            return target
        text = self._coerce_text(query).lower()
        text = re.sub(r"\b(ecobee|homekit|thermostat|temperature|degrees?|please|can you|would you|the|my|a|an)\b", " ", text)
        text = re.sub(
            r"\b(set|make|put|change|raise|lower|turn|switch|mode|heat|heating|cool|cooling|auto|automatic|off|status|state|check|is|what|to)\b",
            " ",
            text,
        )
        text = re.sub(r"\d+(?:\.\d+)?", " ", text)
        return " ".join(re.findall(r"[a-z0-9]+", text))

    def _select_thermostat(self, rows: List[dict], args: Dict[str, Any], query: str) -> Tuple[Optional[dict], List[str]]:
        thermostat_id = self._coerce_text(args.get("thermostat_id") or args.get("entity_id") or args.get("id"))
        if thermostat_id:
            for row in rows:
                if self._coerce_text(row.get("id")) == thermostat_id:
                    return row, []
            return None, [f"No Ecobee HomeKit thermostat matched id {thermostat_id}."]
        if not rows:
            return None, ["No Ecobee HomeKit thermostats were returned for the saved pairing."]
        if len(rows) == 1:
            return rows[0], []

        target = self._target_text(args, query).lower()
        if not target:
            choices = [self._coerce_text(row.get("name")) or self._coerce_text(row.get("id")) for row in rows]
            return None, ["Which Ecobee thermostat? Available thermostats: " + ", ".join(choices)]

        target_tokens = {token for token in re.split(r"[^a-z0-9]+", target) if token}
        scored: List[Tuple[int, dict]] = []
        for row in rows:
            haystack = f"{row.get('name', '')} {row.get('model', '')}".lower()
            if target and target in haystack:
                scored.append((100 + len(target), row))
                continue
            name_tokens = {token for token in re.split(r"[^a-z0-9]+", haystack) if token}
            score = len(target_tokens & name_tokens)
            if score:
                scored.append((score, row))
        if not scored:
            choices = [self._coerce_text(row.get("name")) or self._coerce_text(row.get("id")) for row in rows]
            return None, [f"I could not match '{target}' to an Ecobee thermostat. Available thermostats: " + ", ".join(choices)]
        scored.sort(key=lambda item: item[0], reverse=True)
        if len(scored) > 1 and scored[0][0] == scored[1][0]:
            tied = [row.get("name") for score, row in scored if score == scored[0][0]]
            return None, ["That matched multiple Ecobee thermostats: " + ", ".join(self._coerce_text(name) for name in tied if name)]
        return scored[0][1], []

    def _temperature_for_user(self, row: dict, key: str) -> Optional[float]:
        unit = self._coerce_text(row.get("temperature_unit")).upper() or "F"
        value = row.get(f"{key}_{unit.lower()}")
        if value is None and unit == "F":
            value = row.get(f"{key}_c")
        return value

    def _thermostat_summary(self, row: dict) -> str:
        name = self._coerce_text(row.get("name")) or "Ecobee"
        unit = self._coerce_text(row.get("temperature_unit")).upper() or "F"
        pieces: List[str] = []
        mode = self._coerce_text(row.get("target_hvac_mode"))
        state = self._coerce_text(row.get("current_hvac_state"))
        if mode and state and state != mode:
            pieces.append(f"mode {mode} ({state})")
        elif mode:
            pieces.append(f"mode {mode}")
        current = self._temperature_for_user(row, "current_temperature")
        target = self._temperature_for_user(row, "target_temperature")
        if current is not None:
            pieces.append(f"current {current}°{unit}")
        if target is not None:
            pieces.append(f"setpoint {target}°{unit}")
        humidity = row.get("current_humidity")
        if humidity is not None:
            pieces.append(f"humidity {humidity}%")
        if pieces:
            return f"{name}: " + ", ".join(pieces) + "."
        return f"{name} is available through HomeKit."

    async def _handle(self, args, llm_client=None):
        payload = self._normalize_handler_args(args)
        query = self._coerce_text(payload.get("query") or payload.get("text") or payload.get("prompt"))
        action = self._normalize_action(payload, query)
        if not action:
            ai_payload = await self._ai_pick_action(llm_client, query)
            action = self._normalize_action_value(ai_payload.get("action"))
            if ai_payload.get("temperature") and not any(
                key in payload for key in ("temperature", "target_temperature", "setpoint", "target")
            ):
                payload["temperature"] = ai_payload.get("temperature")
            if ai_payload.get("mode") and not any(payload.get(key) for key in ("mode", "hvac_mode")):
                payload["mode"] = ai_payload.get("mode")
            if ai_payload.get("target") and not any(
                self._coerce_text(payload.get(key)) for key in ("target", "thermostat", "thermostat_name")
            ):
                payload["target"] = ai_payload.get("target")
        if not action:
            return action_failure(
                code="unknown_action",
                message="I couldn't determine the Ecobee HomeKit action from that request.",
                needs=["Ask for Ecobee status, a target temperature, or mode off/heat/cool/auto."],
                say_hint="Ask the user to restate the Ecobee request in one short sentence.",
            )

        try:
            thermostats = list_homekit_thermostats()
        except Exception as exc:
            return action_failure(
                code="ecobee_homekit_unavailable",
                message=f"Could not read Ecobee HomeKit thermostats: {exc}",
                needs=["Pair Ecobee in Tater Settings > Integrations > Ecobee (HomeKit), then retry."],
                say_hint="Explain that the Ecobee HomeKit pairing is not ready and ask to configure it.",
            )

        target, needs = self._select_thermostat(thermostats, payload, query)
        if not target:
            return action_failure(
                code="thermostat_selection_failed",
                message=needs[0] if needs else "Could not choose an Ecobee thermostat.",
                needs=needs or ["Specify the Ecobee thermostat name."],
                say_hint="Ask which Ecobee thermostat the user wants.",
            )

        thermostat_id = self._coerce_text(target.get("id"))
        target_name = self._coerce_text(target.get("name")) or "Ecobee"

        if action == "status":
            return action_success(
                facts={
                    "action": "status",
                    "thermostat_id": thermostat_id,
                    "target_hvac_mode": target.get("target_hvac_mode"),
                    "current_hvac_state": target.get("current_hvac_state"),
                    "current_temperature_f": target.get("current_temperature_f"),
                    "target_temperature_f": target.get("target_temperature_f"),
                    "current_humidity": target.get("current_humidity"),
                },
                data={"thermostat": target},
                summary_for_user=self._thermostat_summary(target),
                say_hint="Report the Ecobee thermostat state from returned data.",
            )

        if action == "set_hvac_mode":
            mode = self._normalize_mode(payload.get("mode") or payload.get("hvac_mode"), query)
            if not mode:
                return action_failure(
                    code="missing_hvac_mode",
                    message="Please include a thermostat mode: off, heat, cool, or auto.",
                    needs=["Say off, heat, cool, or auto."],
                    say_hint="Ask which HVAC mode should be used.",
                )
            try:
                updated = set_homekit_thermostat_mode(mode, thermostat_id=thermostat_id)
            except Exception as exc:
                return action_failure(
                    code="homekit_mode_failed",
                    message=f"Could not set {target_name} to {mode}: {exc}",
                    needs=["Retry or choose a different Ecobee thermostat."],
                    say_hint="Explain the HomeKit mode update failed.",
                )
            return action_success(
                facts={"action": "set_hvac_mode", "thermostat_id": thermostat_id, "hvac_mode": mode},
                data={"thermostat": updated},
                summary_for_user=self._thermostat_summary(updated),
                say_hint="Confirm the Ecobee mode change using returned data.",
            )

        if action == "set_temperature":
            temp = self._temperature_value(payload, query)
            if temp is None:
                return action_failure(
                    code="missing_temperature",
                    message="Please include the target temperature.",
                    needs=["Say the thermostat setpoint, for example 72."],
                    say_hint="Ask what temperature to set.",
                )
            mode = self._normalize_mode(payload.get("mode") or payload.get("hvac_mode"), query)
            unit = self._temperature_unit(payload, query)
            try:
                updated = set_homekit_thermostat_temperature(temp, temperature_unit=unit, mode=mode, thermostat_id=thermostat_id)
            except Exception as exc:
                return action_failure(
                    code="homekit_temperature_failed",
                    message=f"Could not set {target_name} to {temp}°{unit}: {exc}",
                    needs=["Retry or choose a different Ecobee thermostat."],
                    say_hint="Explain the HomeKit temperature update failed.",
                )
            return action_success(
                facts={
                    "action": "set_temperature",
                    "thermostat_id": thermostat_id,
                    "temperature": temp,
                    "temperature_unit": unit,
                    "hvac_mode": mode,
                },
                data={"thermostat": updated},
                summary_for_user=self._thermostat_summary(updated),
                say_hint="Confirm the Ecobee temperature change using returned data.",
            )

        return action_failure(
            code="unsupported_action",
            message="Ecobee HomeKit supports status, set temperature, and set HVAC mode.",
            needs=["Ask for Ecobee status, a target temperature, or mode off/heat/cool/auto."],
            say_hint="Explain supported Ecobee HomeKit actions.",
        )

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)

    async def handle_webui(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self.handle_webui(args or {}, llm_client, context=context)

    async def handle_macos(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)

    async def handle_xbmc(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)

    async def handle_homekit(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)

    async def handle_discord(self, message=None, args=None, llm_client=None, *unused_args, **unused_kwargs):
        payload = args if args is not None else {"query": self._coerce_text(message)}
        return await self._handle(payload, llm_client)

    async def handle_telegram(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)

    async def handle_matrix(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)

    async def handle_irc(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)

    async def handle_meshtastic(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, llm_client)


verba = EcobeeHomeKitThermostatPlugin()
