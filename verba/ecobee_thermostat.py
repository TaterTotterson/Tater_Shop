import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from integrations.ecobee import EcobeeClient, read_ecobee_settings
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("ecobee_thermostat")
logger.setLevel(logging.INFO)


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        text = _coerce_text(value)
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None


class EcobeeThermostatPlugin(ToolVerba):
    name = "ecobee_thermostat"
    verba_name = "Ecobee Thermostat"
    version = "1.0.0"
    min_tater_version = "59"
    pretty_name = "Ecobee Thermostat"
    settings_category = "Ecobee"
    platforms = [
        "voice_core",
        "homeassistant",
        "webui",
        "macos",
        "xbmc",
        "homekit",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    tags = ["ecobee", "thermostat", "climate", "hvac"]
    routing_keywords = ["ecobee", "thermostat", "climate", "hvac", "heat", "cool", "temperature"]
    forced_route = "climate"
    forced_domain_hint = "ecobee thermostat climate"

    usage = '{"function":"ecobee_thermostat","arguments":{"query":"set downstairs thermostat to 72"}}'
    description = "Control Ecobee thermostats directly through the Ecobee API."
    verba_dec = "Control Ecobee thermostats."
    when_to_use = "Use for Ecobee thermostat status, heat/cool/auto/off mode changes, temperature holds, and resume schedule."
    how_to_use = "Pass a natural-language Ecobee thermostat request with an action, optional thermostat name, and optional temperature."
    common_needs = ["Thermostat/device and action, for example: downstairs thermostat + set to 72."]
    missing_info_prompts = ["Which Ecobee thermostat should I control?"]
    example_calls = [
        '{"function":"ecobee_thermostat","arguments":{"query":"what is the downstairs thermostat set to"}}',
        '{"function":"ecobee_thermostat","arguments":{"query":"set downstairs thermostat to 72 for 2 hours"}}',
        '{"function":"ecobee_thermostat","arguments":{"query":"resume the downstairs ecobee schedule"}}',
    ]

    required_settings = {}

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are controlling the Ecobee thermostat now. "
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

    def _diagnosis(self) -> Dict[str, str]:
        settings = read_ecobee_settings()
        return {
            "ecobee_api_key": "set" if self._coerce_text(settings.get("ECOBEE_API_KEY")) else "missing",
            "ecobee_refresh_token": "set" if self._coerce_text(settings.get("ECOBEE_REFRESH_TOKEN")) else "missing",
        }

    def _get_client(self) -> Optional[EcobeeClient]:
        try:
            return EcobeeClient()
        except Exception as exc:
            logger.error("[%s] Failed to initialize Ecobee client: %s", self.name, exc)
            return None

    def _target_text(self, args: dict, query: str) -> str:
        target = self._coerce_text(args.get("target") or args.get("thermostat") or args.get("name"))
        if target:
            return target
        text = self._coerce_text(query).lower()
        text = re.sub(r"\b(ecobee|thermostat|climate|hvac|please|can you|would you|the|my|a|an)\b", " ", text)
        text = re.sub(
            r"\b(set|make|turn|put|change|resume|schedule|hold|temperature|temp|heat|cool|auto|off|mode|to|at|for|hours?|hour|degrees?|degree|what|is|status|current|currently)\b",
            " ",
            text,
        )
        text = re.sub(r"\b\d+(?:\.\d+)?\b", " ", text)
        return " ".join(re.findall(r"[a-z0-9]+", text))

    def _find_thermostat(self, thermostats: List[dict], args: dict, query: str) -> Tuple[Optional[dict], List[str]]:
        thermostat_id = self._coerce_text(args.get("thermostat_id") or args.get("identifier") or args.get("id"))
        if thermostat_id:
            for thermostat in thermostats:
                if self._coerce_text(thermostat.get("id")) == thermostat_id:
                    return thermostat, []
            return None, [f"No Ecobee thermostat matched id={thermostat_id}."]

        if not thermostats:
            return None, ["No Ecobee thermostats were returned for this account."]
        if len(thermostats) == 1:
            return thermostats[0], []

        target = self._target_text(args, query).lower()
        if not target:
            choices = [f"{row.get('name')} ({row.get('hvac_mode')})" for row in thermostats]
            return None, ["Which thermostat? Available Ecobee thermostats: " + ", ".join(choices)]

        target_tokens = {token for token in re.split(r"[^a-z0-9]+", target) if token}
        scored: List[Tuple[int, dict]] = []
        for thermostat in thermostats:
            name = self._coerce_text(thermostat.get("name")).lower()
            if target and target in name:
                scored.append((100 + len(target), thermostat))
                continue
            name_tokens = {token for token in re.split(r"[^a-z0-9]+", name) if token}
            score = len(target_tokens & name_tokens)
            if score:
                scored.append((score, thermostat))
        if not scored:
            choices = [f"{row.get('name')} ({row.get('hvac_mode')})" for row in thermostats]
            return None, [f"I could not match '{target}' to an Ecobee thermostat. Available thermostats: " + ", ".join(choices)]
        scored.sort(key=lambda item: item[0], reverse=True)
        if len(scored) > 1 and scored[0][0] == scored[1][0]:
            tied = [row.get("name") for score, row in scored if score == scored[0][0]]
            return None, ["That matched multiple Ecobee thermostats: " + ", ".join(self._coerce_text(name) for name in tied if name)]
        return scored[0][1], []

    def _extract_temp(self, text: str) -> Optional[float]:
        candidates: List[float] = []
        for match in re.finditer(r"(?<!\d)([4-9]\d(?:\.\d)?)(?!\d)", text):
            value = _number(match.group(1))
            if value is not None and 45 <= value <= 95:
                candidates.append(value)
        return candidates[0] if candidates else None

    def _extract_heat_cool(self, args: dict, text: str) -> Tuple[Optional[float], Optional[float]]:
        heat = _number(args.get("heat_temp") or args.get("heat") or args.get("target_temp_low"))
        cool = _number(args.get("cool_temp") or args.get("cool") or args.get("target_temp_high"))

        lower = text.lower()
        between = re.search(r"\bbetween\s+([4-9]\d(?:\.\d)?)\s+(?:and|-)\s+([4-9]\d(?:\.\d)?)", lower)
        if between:
            first = _number(between.group(1))
            second = _number(between.group(2))
            if first is not None and second is not None:
                heat = min(first, second)
                cool = max(first, second)

        heat_match = re.search(r"\bheat(?:ing)?(?:\s+(?:to|at|on))?\s+([4-9]\d(?:\.\d)?)", lower)
        if heat_match:
            heat = _number(heat_match.group(1))
        cool_match = re.search(r"\b(?:cool|cooling|ac)(?:\s+(?:to|at|on))?\s+([4-9]\d(?:\.\d)?)", lower)
        if cool_match:
            cool = _number(cool_match.group(1))
        return heat, cool

    def _extract_hvac_mode(self, args: dict, text: str) -> str:
        explicit = self._coerce_text(args.get("hvac_mode") or args.get("mode")).lower()
        aliases = {
            "heat": "heat",
            "heating": "heat",
            "cool": "cool",
            "cooling": "cool",
            "ac": "cool",
            "auto": "auto",
            "automatic": "auto",
            "off": "off",
            "aux": "auxHeatOnly",
            "auxheatonly": "auxHeatOnly",
        }
        if explicit:
            return aliases.get(explicit.replace("_", "").replace("-", ""), explicit)

        lower = text.lower()
        if re.search(r"\bturn\s+(?:the\s+)?(?:ecobee|thermostat|hvac|heat|cooling|ac)?\s*off\b", lower) or re.search(r"\boff\s+mode\b", lower):
            return "off"
        for pattern, mode in (
            (r"\bheat(?:ing)?\s+mode\b|\bset\s+.*\bto\s+heat\b", "heat"),
            (r"\bcool(?:ing)?\s+mode\b|\bac\s+mode\b|\bset\s+.*\bto\s+cool\b", "cool"),
            (r"\bauto(?:matic)?\s+mode\b|\bset\s+.*\bto\s+auto\b", "auto"),
        ):
            if re.search(pattern, lower):
                return mode
        return ""

    def _extract_hold_hours(self, args: dict, text: str) -> Optional[int]:
        raw = args.get("hold_hours") or args.get("hours")
        try:
            if raw is not None:
                return max(1, min(24, int(float(str(raw)))))
        except Exception:
            pass
        match = re.search(r"\bfor\s+(\d{1,2})\s+hours?\b", text.lower())
        if not match:
            return None
        return max(1, min(24, int(match.group(1))))

    def _infer_action(self, args: dict, query: str) -> str:
        explicit = self._coerce_text(args.get("action")).lower()
        aliases = {
            "status": "status",
            "get_state": "status",
            "state": "status",
            "set_temperature": "set_hold",
            "set_temp": "set_hold",
            "set_hold": "set_hold",
            "hold": "set_hold",
            "set_hvac_mode": "set_mode",
            "set_mode": "set_mode",
            "mode": "set_mode",
            "resume": "resume",
            "resume_program": "resume",
            "clear_hold": "resume",
        }
        if explicit in aliases:
            return aliases[explicit]

        lower = query.lower()
        if re.search(r"\b(resume|schedule|clear\s+hold|cancel\s+hold|resume\s+program)\b", lower):
            return "resume"
        if self._extract_temp(lower) is not None or self._extract_heat_cool(args, lower) != (None, None):
            return "set_hold"
        if self._extract_hvac_mode(args, lower):
            return "set_mode"
        return "status"

    def _summary(self, thermostat: dict) -> str:
        name = self._coerce_text(thermostat.get("name")) or "Ecobee"
        current = thermostat.get("current_temperature_f")
        mode = self._coerce_text(thermostat.get("hvac_mode")) or "unknown"
        heat = thermostat.get("desired_heat_f")
        cool = thermostat.get("desired_cool_f")
        humidity = thermostat.get("humidity")
        parts = [f"{name} is {current}F" if current is not None else f"{name} temperature is unknown"]
        parts.append(f"mode {mode}")
        if heat is not None and cool is not None:
            parts.append(f"heat {heat}F / cool {cool}F")
        elif heat is not None:
            parts.append(f"heat {heat}F")
        elif cool is not None:
            parts.append(f"cool {cool}F")
        if humidity is not None:
            parts.append(f"humidity {humidity}%")
        equipment = self._coerce_text(thermostat.get("equipment_status"))
        if equipment:
            parts.append(f"equipment {equipment}")
        return "; ".join(parts) + "."

    async def _handle(self, args, llm_client=None):
        payload = self._normalize_handler_args(args)
        query = self._coerce_text(payload.get("query") or payload.get("text") or payload.get("prompt"))
        client = self._get_client()
        if not client:
            return action_failure(
                code="ecobee_not_configured",
                message="Ecobee is not configured.",
                diagnosis=self._diagnosis(),
                needs=["Set Ecobee API key and complete PIN linking in Tater Settings > Integrations > Ecobee."],
                say_hint="Explain Ecobee needs to be linked in Settings before thermostat control works.",
            )

        try:
            thermostats = client.list_thermostats()
        except Exception as exc:
            return action_failure(
                code="ecobee_lookup_failed",
                message=f"Could not read Ecobee thermostats: {exc}",
                diagnosis=self._diagnosis(),
                say_hint="Explain Ecobee lookup failed and suggest checking the Ecobee link in Settings.",
            )

        thermostat, needs = self._find_thermostat(thermostats, payload, query)
        if not thermostat:
            return action_failure(
                code="thermostat_selection_failed",
                message="Could not select an Ecobee thermostat.",
                needs=needs,
                say_hint="Ask which Ecobee thermostat to use.",
            )

        action = self._infer_action(payload, query)
        thermostat_id = thermostat.get("id")
        try:
            if action == "status":
                return action_success(
                    facts={
                        "action": "status",
                        "thermostat_id": thermostat_id,
                        "thermostat_name": thermostat.get("name"),
                        "hvac_mode": thermostat.get("hvac_mode"),
                        "current_temperature_f": thermostat.get("current_temperature_f"),
                        "desired_heat_f": thermostat.get("desired_heat_f"),
                        "desired_cool_f": thermostat.get("desired_cool_f"),
                    },
                    data={"thermostat": thermostat, "thermostats": thermostats},
                    summary_for_user=self._summary(thermostat),
                    say_hint="Report the Ecobee thermostat status briefly.",
                )

            if action == "resume":
                updated = client.resume_program(thermostat_id, resume_all=True)
                return action_success(
                    facts={"action": "resume", "thermostat_id": thermostat_id, "thermostat_name": updated.get("name")},
                    data={"thermostat": updated},
                    summary_for_user=f"{updated.get('name') or 'Ecobee'} resumed its schedule.",
                    say_hint="Tell the user the Ecobee schedule was resumed.",
                )

            hvac_mode = self._extract_hvac_mode(payload, query)
            if action == "set_mode":
                if not hvac_mode:
                    return action_failure(
                        code="missing_hvac_mode",
                        message="Tell me which mode to use: heat, cool, auto, or off.",
                        needs=["HVAC mode: heat, cool, auto, or off."],
                        say_hint="Ask which Ecobee mode the user wants.",
                    )
                updated = client.set_hvac_mode(thermostat_id, hvac_mode)
                return action_success(
                    facts={
                        "action": "set_mode",
                        "thermostat_id": thermostat_id,
                        "thermostat_name": updated.get("name"),
                        "hvac_mode": updated.get("hvac_mode"),
                    },
                    data={"thermostat": updated},
                    summary_for_user=f"{updated.get('name') or 'Ecobee'} is now in {updated.get('hvac_mode')} mode.",
                    say_hint="Tell the user the Ecobee mode was changed.",
                )

            heat, cool = self._extract_heat_cool(payload, query)
            temp = _number(payload.get("temperature") or payload.get("target_temperature") or payload.get("temp"))
            if temp is None:
                temp = self._extract_temp(query)
            if heat is None and cool is None and temp is None:
                return action_failure(
                    code="missing_temperature",
                    message="Tell me the target temperature for the Ecobee hold.",
                    needs=["Target temperature, for example 72."],
                    say_hint="Ask what temperature the user wants.",
                )

            if hvac_mode:
                thermostat = client.set_hvac_mode(thermostat_id, hvac_mode)
            else:
                hvac_mode = self._coerce_text(thermostat.get("hvac_mode")).lower()

            if heat is None and cool is None and temp is not None:
                if hvac_mode == "heat":
                    heat = temp
                elif hvac_mode == "cool":
                    cool = temp
                elif hvac_mode == "auto":
                    heat = temp - 2
                    cool = temp + 2
                else:
                    return action_failure(
                        code="missing_hvac_mode",
                        message="Set the Ecobee to heat, cool, or auto before setting a temperature hold.",
                        needs=["HVAC mode: heat, cool, or auto."],
                        say_hint="Ask whether to use heat, cool, or auto mode.",
                    )

            hold_hours = self._extract_hold_hours(payload, query)
            updated = client.set_hold(
                thermostat_id,
                heat_temp_f=heat,
                cool_temp_f=cool,
                hold_type="holdHours" if hold_hours else "indefinite",
                hold_hours=hold_hours,
            )
            duration = f" for {hold_hours} hour{'s' if hold_hours != 1 else ''}" if hold_hours else ""
            return action_success(
                facts={
                    "action": "set_hold",
                    "thermostat_id": thermostat_id,
                    "thermostat_name": updated.get("name"),
                    "hvac_mode": updated.get("hvac_mode"),
                    "heat_temp_f": heat,
                    "cool_temp_f": cool,
                    "hold_hours": hold_hours,
                },
                data={"thermostat": updated},
                summary_for_user=f"{updated.get('name') or 'Ecobee'} hold is set{duration}. {self._summary(updated)}",
                say_hint="Tell the user the Ecobee hold was set briefly.",
            )
        except Exception as exc:
            return action_failure(
                code="ecobee_command_failed",
                message=f"Ecobee command failed: {exc}",
                diagnosis=self._diagnosis(),
                needs=["Check the Ecobee app or Tater Settings > Integrations > Ecobee."],
                say_hint="Explain the Ecobee command failed and suggest checking the Ecobee link.",
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


verba = EcobeeThermostatPlugin()
