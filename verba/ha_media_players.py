import json
import logging
from typing import Any, Dict, List, Optional

import requests

from helpers import extract_json, redis_client
from verba_base import ToolVerba
from verba_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from verba_result import action_failure, action_success

logger = logging.getLogger("ha_media_players")
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

    def call_service(self, service: str, payload: dict):
        return self._req("POST", f"/api/services/media_player/{service}", json_body=payload)


class HAMediaPlayersPlugin(ToolVerba):
    name = "ha_media_players"
    verba_name = "Home Assistant Media Players"
    version = "2.0.5"
    min_tater_version = "59"
    pretty_name = "Home Assistant Media Players"
    settings_category = "Home Assistant Control"
    platforms = ['voice_core', 'homeassistant', 'webui', 'macos', 'xbmc', 'homekit', 'discord', 'telegram', 'matrix', 'irc']
    tags = ["homeassistant", "media_player"]

    usage = (
        '{"function":"ha_media_players","arguments":{"query":"play https://example.com/video.m3u8 on the living room apple tv"}}'
    )
    description = "Control Home Assistant media players, including URL playback and playback state checks."
    verba_dec = "Media-player control for Home Assistant."
    when_to_use = "Use for media_player playback controls, now-playing status, and direct URL playback."
    how_to_use = (
        "Pass a natural-language query. Include an http/https URL in the query for direct playback. "
        "Optional media_content_type defaults to video."
    )
    common_needs = ["A media player target and desired action in natural language."]
    missing_info_prompts = ["Which media player should I control?"]
    example_calls = [
        '{"function":"ha_media_players","arguments":{"query":"what is the living room tv playing"}}',
        '{"function":"ha_media_players","arguments":{"query":"pause the family room apple tv"}}',
        '{"function":"ha_media_players","arguments":{"query":"play https://example.com/video.m3u8 on the living room apple tv"}}',
    ]

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are controlling Home Assistant media players now. "
        "Only output that message."
    )

    required_settings = {
        "HA_MEDIA_PLAYERS_MAX_CANDIDATES": {
            "label": "Max Candidate Media Players",
            "type": "number",
            "default": 120,
            "description": "Maximum number of media_player candidates sent to the chooser LLM call.",
        },
    }

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
        val = max(minimum, min(maximum, val))
        return val

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
            logger.error("[ha_media_players] Failed to initialize HA client: %s", exc)
            return None

    @staticmethod
    def _coerce_text(value: Any) -> str:
        return str(value or "").strip()

    async def _llm_json(
        self,
        *,
        llm_client,
        system: str,
        user_payload: dict,
        max_tokens: Optional[int] = 220,
        temperature: float = 0.0,
    ) -> dict:
        if llm_client is None:
            return {}
        try:
            kwargs: Dict[str, Any] = {
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                "temperature": temperature,
            }
            if isinstance(max_tokens, int) and max_tokens > 0:
                kwargs["max_tokens"] = int(max_tokens)
            resp = await llm_client.chat(**kwargs)
            text = self._coerce_text((resp.get("message", {}) or {}).get("content", ""))
            raw = extract_json(text) or ""
            if not raw:
                return {}
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else {}
        except Exception as exc:
            logger.debug("[ha_media_players] llm_json failed: %s", exc)
            return {}

    def _media_players_catalog(self, states: List[dict]) -> List[dict]:
        out: List[dict] = []
        seen: set[str] = set()
        for st in states or []:
            if not isinstance(st, dict):
                continue
            entity_id = self._coerce_text(st.get("entity_id")).lower()
            if not entity_id.startswith("media_player.") or entity_id in seen:
                continue
            seen.add(entity_id)
            attrs = st.get("attributes") if isinstance(st.get("attributes"), dict) else {}
            if not isinstance(attrs, dict):
                attrs = {}
            out.append(
                {
                    "entity_id": entity_id,
                    "name": self._coerce_text(attrs.get("friendly_name")) or entity_id,
                    "state": self._coerce_text(st.get("state")) or "unknown",
                    "source": self._coerce_text(attrs.get("source")),
                    "app_name": self._coerce_text(attrs.get("app_name")),
                    "media_title": self._coerce_text(attrs.get("media_title")),
                }
            )
        out.sort(key=lambda row: (row.get("name") or "").lower())
        return out

    async def _interpret_query(self, query: str, llm_client) -> dict:
        q = self._coerce_text(query)
        if not q:
            return {}
        system = (
            "You interpret Home Assistant media_player requests.\n"
            "Return STRICT JSON only:\n"
            '{"intent":"get_state|control","action":"get_state|turn_on|turn_off|media_play|media_pause|media_stop|play_media","target":"<short target phrase or empty>"}\n'
            "Rules:\n"
            "- If request asks what is playing/state/source/app, use action=get_state.\n"
            "- If request asks to play an http/https URL, use action=play_media.\n"
            "- For pause/play/stop, use media_pause/media_play/media_stop.\n"
            "- target should be the user-mentioned room/device phrase when present."
        )
        obj = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={"query": q},
            max_tokens=180,
            temperature=0.0,
        )
        action = self._coerce_text(obj.get("action")).lower()
        intent = self._coerce_text(obj.get("intent")).lower()
        target = self._coerce_text(obj.get("target"))
        allowed_actions = {
            "get_state",
            "turn_on",
            "turn_off",
            "media_play",
            "media_pause",
            "media_stop",
            "play_media",
        }
        if action not in allowed_actions:
            action = "get_state"
        if intent not in {"get_state", "control"}:
            intent = "get_state" if action == "get_state" else "control"
        return {"intent": intent, "action": action, "target": target}

    async def _extract_media_url_with_llm(self, query: str, llm_client) -> str:
        q = self._coerce_text(query)
        if not q:
            return ""
        system = (
            "Extract a direct media URL from the query.\n"
            'Return STRICT JSON only: {"url":"<http/https url or empty string>"}\n'
            "If no URL is present, return empty string."
        )
        obj = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={"query": q},
            max_tokens=None,
            temperature=0.0,
        )
        url = self._coerce_text(obj.get("url")).rstrip(").,;!?]}\"'")
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return ""

    async def _choose_media_player_entity(
        self,
        *,
        query: str,
        action: str,
        target_hint: str,
        catalog: List[dict],
        llm_client,
    ) -> str:
        if not catalog:
            return ""
        if len(catalog) == 1:
            return self._coerce_text(catalog[0].get("entity_id")).lower()

        max_candidates = self._get_int_setting("HA_MEDIA_PLAYERS_MAX_CANDIDATES", 120, 5, 500)
        candidates = catalog[:max_candidates]
        compact = [
            {
                "entity_id": row.get("entity_id"),
                "name": row.get("name"),
                "state": row.get("state"),
                "source": row.get("source"),
                "app_name": row.get("app_name"),
                "media_title": row.get("media_title"),
            }
            for row in candidates
        ]
        valid_ids = {self._coerce_text(row.get("entity_id")).lower() for row in candidates}
        system = (
            "Choose the best Home Assistant media_player entity for the request.\n"
            'Return STRICT JSON only: {"entity_id":"media_player.x"}\n'
            "- Choose exactly one entity_id from the provided candidates."
        )
        obj = await self._llm_json(
            llm_client=llm_client,
            system=system,
            user_payload={
                "query": self._coerce_text(query),
                "action": self._coerce_text(action),
                "target_hint": self._coerce_text(target_hint),
                "candidates": compact,
            },
            max_tokens=200,
            temperature=0.0,
        )
        picked = self._coerce_text(obj.get("entity_id")).lower()
        if picked in valid_ids:
            return picked
        return ""

    @staticmethod
    def _service_for_action(action: str) -> str:
        a = str(action or "").strip().lower()
        if a in {"turn_on", "turn_off", "media_play", "media_pause", "media_stop", "play_media"}:
            return a
        return ""

    async def _call_media_service_with_retries(
        self,
        *,
        client: HAClient,
        service: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        base_payload: Dict[str, Any] = dict(payload or {})
        attempted_payloads: List[Dict[str, Any]] = []

        def _push_attempt(row: Dict[str, Any]) -> None:
            if not isinstance(row, dict):
                return
            entity_id = self._coerce_text(row.get("entity_id")).lower()
            media_content_id = self._coerce_text(row.get("media_content_id"))
            media_content_type = self._coerce_text(row.get("media_content_type")).lower()
            key = (entity_id, media_content_id, media_content_type)
            for prev in attempted_payloads:
                prev_key = (
                    self._coerce_text(prev.get("entity_id")).lower(),
                    self._coerce_text(prev.get("media_content_id")),
                    self._coerce_text(prev.get("media_content_type")).lower(),
                )
                if key == prev_key:
                    return
            attempted_payloads.append(dict(row))

        _push_attempt(base_payload)
        if service == "play_media":
            default_type = self._coerce_text(base_payload.get("media_content_type")).lower() or "video"
            for alt_type in ("video", "video/mp4"):
                if alt_type == default_type:
                    continue
                variant = dict(base_payload)
                variant["media_content_type"] = alt_type
                _push_attempt(variant)

        last_exc: Optional[Exception] = None
        used_payload: Dict[str, Any] = dict(base_payload)
        for candidate in attempted_payloads:
            used_payload = dict(candidate)
            try:
                client.call_service(service, candidate)
                return {
                    "ok": True,
                    "payload": used_payload,
                    "attempt_count": len(attempted_payloads),
                    "last_error": "",
                }
            except Exception as exc:
                last_exc = exc

        return {
            "ok": False,
            "payload": used_payload,
            "attempt_count": len(attempted_payloads),
            "last_error": str(last_exc or ""),
        }

    def _state_summary(self, entity_id: str, state_payload: dict) -> str:
        st = state_payload if isinstance(state_payload, dict) else {}
        attrs = st.get("attributes") if isinstance(st.get("attributes"), dict) else {}
        if not isinstance(attrs, dict):
            attrs = {}
        name = self._coerce_text(attrs.get("friendly_name")) or entity_id
        state = self._coerce_text(st.get("state")) or "unknown"
        media_title = self._coerce_text(attrs.get("media_title"))
        media_artist = self._coerce_text(attrs.get("media_artist"))
        source = self._coerce_text(attrs.get("source"))
        app_name = self._coerce_text(attrs.get("app_name"))

        details: List[str] = []
        if media_title:
            if media_artist:
                details.append(f"now playing {media_title} by {media_artist}")
            else:
                details.append(f"now playing {media_title}")
        if source:
            details.append(f"source {source}")
        if app_name:
            details.append(f"app {app_name}")

        if details:
            return f"{name} is {state}; " + ", ".join(details) + "."
        return f"{name} is {state}."

    async def _handle(self, args, llm_client):
        args = args or {}
        query = self._coerce_text(args.get("query"))
        if not query:
            return action_failure(
                code="missing_query",
                message="Please provide a Home Assistant media player request in query.",
                needs=["What should I do with Home Assistant media players?"],
                say_hint="Ask for the media player action in natural language.",
            )

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

        catalog = self._media_players_catalog(states)
        if not catalog:
            return action_failure(
                code="no_media_players",
                message="No media_player entities were found in Home Assistant.",
                needs=["Add or expose at least one media_player entity in Home Assistant."],
                say_hint="Explain no media_player entities were available.",
            )

        intent = await self._interpret_query(query, llm_client)
        action = self._coerce_text(intent.get("action")).lower() or "get_state"
        target_hint = self._coerce_text(intent.get("target"))

        entity_id = self._coerce_text(args.get("entity_id")).lower()
        if entity_id:
            valid_ids = {self._coerce_text(row.get("entity_id")).lower() for row in catalog}
            if entity_id not in valid_ids:
                return action_failure(
                    code="invalid_entity_id",
                    message=f"Entity '{entity_id}' is not an available media_player.",
                    needs=["Use a valid media_player entity_id from Home Assistant."],
                    say_hint="Explain the provided entity_id is not a valid media_player.",
                )
        else:
            entity_id = await self._choose_media_player_entity(
                query=query,
                action=action,
                target_hint=target_hint,
                catalog=catalog,
                llm_client=llm_client,
            )
            if not entity_id:
                return action_failure(
                    code="entity_selection_failed",
                    message="Could not select a media player for this request.",
                    needs=["Specify the room/device more clearly (for example 'living room apple tv')."],
                    say_hint="Ask for a clearer media player target.",
                )

        if action == "get_state":
            try:
                st = client.get_state(entity_id)
                summary = self._state_summary(entity_id, st if isinstance(st, dict) else {})
                return action_success(
                    facts={"action": "get_state", "entity_id": entity_id},
                    data={"entity_id": entity_id, "state": st},
                    summary_for_user=summary,
                    say_hint="Report the media player status and now-playing details from returned data.",
                )
            except Exception as exc:
                return action_failure(
                    code="state_read_failed",
                    message=f"Could not read state for {entity_id}: {exc}",
                    diagnosis=self._ha_diagnosis(),
                    say_hint="Explain state read failed and suggest retrying.",
                )

        service = self._service_for_action(action)
        if not service:
            return action_failure(
                code="unsupported_action",
                message=f"Unsupported media_player action '{action}'.",
                needs=["Try actions like turn on/off, play, pause, stop, or play a URL."],
                say_hint="Explain supported media-player actions.",
            )

        payload: Dict[str, Any] = {"entity_id": entity_id}
        media_url = ""
        if service == "play_media":
            media_url = await self._extract_media_url_with_llm(query, llm_client)
            if not media_url:
                return action_failure(
                    code="missing_media_url",
                    message="No media URL was found in the request.",
                    needs=["Include a full http/https media URL in your request."],
                    say_hint="Ask for a direct media URL to play.",
                )
            media_type = self._coerce_text(args.get("media_content_type")) or "video"
            payload["media_content_id"] = media_url
            payload["media_content_type"] = media_type

        service_result = await self._call_media_service_with_retries(
            client=client,
            service=service,
            payload=payload,
        )
        payload = dict(service_result.get("payload") or payload)
        if not service_result.get("ok"):
            exc = self._coerce_text(service_result.get("last_error"))
            attempts = int(service_result.get("attempt_count") or 1)
            suffix = ""
            if service == "play_media":
                suffix = (
                    " Home Assistant/Apple TV rejected playback details. "
                    "Try a different URL or verify the Apple TV can directly reach this media source."
                )
            return action_failure(
                code="service_call_failed",
                message=f"Home Assistant media_player.{service} failed for {entity_id} after {attempts} attempt(s): {exc}{suffix}",
                diagnosis=self._ha_diagnosis(),
                needs=["Retry or choose a different media player target."],
                say_hint="Explain the Home Assistant service call failed and ask whether to retry.",
            )

        summary = ""
        try:
            st_after = client.get_state(entity_id)
            summary = self._state_summary(entity_id, st_after if isinstance(st_after, dict) else {})
        except Exception:
            if service == "play_media":
                summary = f"Started URL playback on {entity_id}."
            else:
                summary = f"Sent {service.replace('_', ' ')} to {entity_id}."

        facts: Dict[str, Any] = {"action": service, "entity_id": entity_id}
        if media_url:
            facts["media_url"] = media_url

        data: Dict[str, Any] = {"entity_id": entity_id, "service": service}
        if media_url:
            data["media_url"] = media_url

        return action_success(
            facts=facts,
            data=data,
            summary_for_user=summary,
            say_hint="Confirm what was executed on the media player and include now-playing status if available.",
        )

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)
    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        try:
            return await self.handle_homeassistant(args=args, llm_client=llm_client, context=context)
        except TypeError:
            try:
                return await self.handle_homeassistant(args=args, llm_client=llm_client)
            except TypeError:
                return await self.handle_homeassistant(args, llm_client)


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


verba = HAMediaPlayersPlugin()
