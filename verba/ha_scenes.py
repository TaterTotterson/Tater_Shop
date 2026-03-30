import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

from helpers import extract_json, redis_client
from verba_base import ToolVerba
from verba_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from verba_result import action_failure, action_success

logger = logging.getLogger('ha_scenes')
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


class HAScenesPlugin(ToolVerba):
    name = 'ha_scenes'
    verba_name = 'Home Assistant Scenes'
    version = '2.0.0'
    min_tater_version = "59"
    pretty_name = 'Home Assistant Scenes'
    settings_category = "Home Assistant Control"
    platforms = ['homeassistant', 'webui', 'macos', 'xbmc', 'homekit', 'discord', 'telegram', 'matrix', 'irc']
    tags = ['homeassistant', 'scene']

    forced_route = 'scene'
    forced_domain_hint = 'scene'

    usage = '{"function":"ha_scenes","arguments":{"query":"run movie time scene"}}'
    description = 'Activate Home Assistant scenes and check scene state.'
    verba_dec = 'Control Home Assistant scenes.'
    when_to_use = 'Use for scene activation and basic scene status checks.'
    how_to_use = 'Pass a natural-language scene request with scene name and action.'
    common_needs = ['Scene name and action (for example: movie time scene + activate).']
    missing_info_prompts = ['Which scene should I run?']
    example_calls = ['{"function":"ha_scenes","arguments":{"query":"activate movie time scene"}}', '{"function":"ha_scenes","arguments":{"query":"is bedtime scene active"}}']

    entity_domains: List[str] = ['scene']
    interpret_actions: List[str] = ['get_state', 'activate']
    service_map: Dict[str, Optional[str]] = {'get_state': None, 'activate': 'turn_on'}
    required_action_params: Dict[str, List[str]] = {}
    optional_action_params: Dict[str, List[str]] = {}
    param_payload_keys: Dict[str, str] = {}
    summary_attribute_keys: List[str] = []
    interpret_focus: str = '- Use activate for explicit scene run/activate requests.\n- Use get_state for status checks.'
    interpret_examples: List[str] = ['activate movie time scene -> action=activate, target=movie time scene, read_target=none', 'is bedtime scene active -> action=get_state, target=bedtime scene, read_target=state']

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

        params["color_name"] = self._coerce_text(params_in.get("color_name"))
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

        return service, payload, None

    def _state_summary(self, *, entity_id: str, state_payload: dict, read_target: str) -> str:
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
        for key in self.summary_attribute_keys or []:
            if attrs.get(key) not in (None, "", []):
                extras.append(f"{key.replace('_', ' ')} {attrs.get(key)}")

        if extras:
            return f"{name} is {state}; " + ", ".join(extras) + "."
        return f"{name} is {state}."

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
                summary = self._state_summary(entity_id=entity_id, state_payload=state_dict, read_target=read_target)
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

        try:
            client.call_service(self._coerce_text(selected.get("domain")), service, payload)
        except Exception as exc:
            return action_failure(
                code="service_call_failed",
                message=f"Home Assistant {selected.get('domain')}.{service} failed for {entity_id}: {exc}",
                diagnosis=self._ha_diagnosis(),
                needs=["Retry or choose a different entity target."],
                say_hint="Explain service call failed and ask whether to retry.",
            )

        summary = ""
        state_after: Any = {}
        try:
            state_after = client.get_state(entity_id)
            summary = self._state_summary(
                entity_id=entity_id,
                state_payload=state_after if isinstance(state_after, dict) else {},
                read_target=read_target,
            )
        except Exception:
            summary = f"Sent {service.replace('_', ' ')} to {entity_id}."

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
                "payload": payload,
                "intent": intent,
                "state_after": state_after,
            },
            summary_for_user=summary,
            say_hint="Confirm what was executed and include resulting state when available.",
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


verba = HAScenesPlugin()
