# verba/ha_switches.py
import asyncio
import logging
import re
import json as _json
import time
import requests
import difflib
from typing import Any, List, Optional, Set, Tuple

from verba_base import ToolVerba
from helpers import redis_client
from verba_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from verba_result import action_failure, action_success

logger = logging.getLogger("ha_switches")
logger.setLevel(logging.INFO)


class HAClient:
    """Simple Home Assistant REST API helper (settings from Redis)."""

    def __init__(self):
        settings = redis_client.hgetall("homeassistant_settings") or {}

        self.base_url = (settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        self.token = (settings.get("HA_TOKEN") or "").strip()
        if not self.token:
            raise ValueError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _req(self, method: str, path: str, json=None, timeout=15):
        url = f"{self.base_url}{path}"
        resp = requests.request(method, url, headers=self.headers, json=json, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
        try:
            return resp.json()
        except Exception:
            return resp.text

    def render_template(self, template_str: str):
        return self._req("POST", "/api/template", json={"template": template_str})

    def call_service(self, domain: str, service: str, data: dict):
        return self._req("POST", f"/api/services/{domain}/{service}", json=data)

    def get_state(self, entity_id: str):
        return self._req("GET", f"/api/states/{entity_id}")

    def list_states(self):
        return self._req("GET", "/api/states") or []


class HASwitchesPlugin(ToolVerba):
    name = 'ha_switches'
    verba_name = 'Home Assistant Switches'
    version = '1.0.0'
    min_tater_version = '59'
    pretty_name = 'Home Assistant Switches'
    settings_category = 'Home Assistant Control'
    platforms = ['homeassistant', 'webui', 'macos', 'xbmc', 'homekit', 'discord', 'telegram', 'matrix', 'irc']
    tags = ['homeassistant', 'switch']

    forced_route = 'switch'
    forced_domain_hint = 'switch'

    usage = '{"function":"ha_switches","arguments":{"query":"Control or check Home Assistant switches, outlets, and plugs in natural language."}}'

    description = 'Control and check Home Assistant switches, plugs, and outlets with switch-only routing.'
    verba_dec = 'Control Home Assistant switches and plugs.'
    when_to_use = 'Use when the request is about switches, outlets, plugs, or simple power toggles.'
    common_needs = ['switch/device and action (for example: arcade plug + turn on)']
    missing_info_prompts = []

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re accessing Home Assistant devices now! "
        "Only output that message."
    )

    required_settings = {
        "HA_CATALOG_CACHE_SECONDS": {
            "label": "Catalog Cache Seconds",
            "type": "number",
            "default": 60,
            "description": "How long to cache the compact entity catalog in Redis."
        },
        "HA_MAX_CANDIDATES": {
            "label": "Max Candidates Sent to LLM",
            "type": "number",
            "default": 400,
            "description": "Max candidates to send in a single LLM call (tournament chunking used above this)."
        },
        "HA_CHUNK_SIZE": {
            "label": "LLM Tournament Chunk Size",
            "type": "number",
            "default": 120,
            "description": "Chunk size for tournament selection when candidate list is very large."
        },
        "HA_INTERPRET_CACHE_SECONDS": {
            "label": "Interpret Cache Seconds",
            "type": "number",
            "default": 45,
            "description": "Cache LLM interpret results per-query (seconds). Fast-path rules still run first."
        },
        "HA_CHOOSE_CACHE_SECONDS": {
            "label": "Choose Cache Seconds",
            "type": "number",
            "default": 45,
            "description": "Cache LLM chosen entity per-query+catalog (seconds)."
        },
    }

    # ----------------------------
    # Settings helpers
    # ----------------------------
    @staticmethod
    def _decode_map(raw: dict | None) -> dict:
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

    def _get_int_setting(
        self,
        key: str,
        default: int,
        minimum: Optional[int] = None,
        maximum: Optional[int] = None,
    ) -> int:
        raw = self._get_plugin_settings().get(key)
        try:
            value = int(float(str(raw).strip()))
        except Exception:
            value = int(default)
        if minimum is not None and value < minimum:
            value = minimum
        if maximum is not None and value > maximum:
            value = maximum
        return value

    def _get_bool_setting(self, key: str, default: bool) -> bool:
        raw = self._get_plugin_settings().get(key)
        if isinstance(raw, bool):
            return raw
        text = str(raw or "").strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
        return default

    # ----------------------------
    # Internal helpers
    # ----------------------------
    def _get_client(self):
        try:
            return HAClient()
        except Exception as e:
            logger.error(f"[ha_switches] Failed to initialize HA client: {e}")
            return None

    def _excluded_entities_set(self) -> set[str]:
        """
        Read up to five Voice PE entity IDs from platform settings and exclude them
        from light control calls.
        """
        plat = redis_client.hgetall("homeassistant_portal_settings") or {}
        ids = []
        keys = ("VOICE_PE_ENTITY_1", "VOICE_PE_ENTITY_2", "VOICE_PE_ENTITY_3", "VOICE_PE_ENTITY_4", "VOICE_PE_ENTITY_5")
        for k in keys:
            v = (plat.get(k) or plat.get(k.lower()) or "").strip()
            if v:
                ids.append(v.lower())
        excluded = set(ids)
        logger.debug(f"[ha_switches] excluded voice PE entities: {excluded}")
        return excluded

    def _clean_search_text(self, text: str) -> str:
        normalized = self._normalize_for_match(text)
        if not normalized:
            return ""
        filler = {
            "turn", "on", "off", "the", "a", "an", "please", "to", "in", "of", "for",
            "set", "make", "switch", "device", "devices", "area", "room", "what", "whats",
            "is", "are", "status", "state", "check", "show", "tell", "me", "get", "with",
            "my", "our", "your", "all", "run", "activate",
        }
        tokens = [tok for tok in normalized.split(" ") if tok and tok not in filler]
        if not tokens:
            return normalized
        return " ".join(tokens[:8]).strip()

    def _default_route_info(self, query: str) -> dict:
        return {
            "route": "unknown",
            "search_text": self._clean_search_text(query) or (query or "").strip(),
        }

    async def _route_query_with_llm(self, query: str, intent: Optional[dict], llm_client) -> dict:
        # Split verba are domain-fixed; no LLM route classification.
        return self._default_route_info(query)

    async def _base_route_query(self, query: str, intent: Optional[dict], llm_client) -> dict:
        return self._default_route_info(query)

    @staticmethod
    def _state_key(st: Any) -> str:
        """
        Build a stable-ish representation of state to detect changes.
        Useful because many remotes report activity/current_activity rather than state.
        """
        if not isinstance(st, dict):
            return str(st or "")
        attrs = st.get("attributes") or {}
        if not isinstance(attrs, dict):
            attrs = {}
        return "|".join([
            str(st.get("state", "") or ""),
            str(attrs.get("current_activity") or attrs.get("activity") or ""),
            str(attrs.get("source") or ""),
            str(attrs.get("app_id") or ""),
        ])

    def _find_related_media_player(self, catalog: List[dict], remote_friendly: str, query: str) -> Optional[str]:
        """
        Best-effort: find a media_player that looks like the same device as a remote.
        This is a fallback when remote.turn_on doesn't actually power the device.
        """
        rf = (remote_friendly or "").strip().lower()
        q = (query or "").strip().lower()

        mps = [c for c in catalog if (c.get("domain") == "media_player" and c.get("entity_id"))]
        if not mps:
            return None

        def score(c: dict) -> int:
            name = (c.get("name") or "").strip().lower()
            eid = (c.get("entity_id") or "").strip().lower()
            blob = f"{name} {eid}"

            s = 0
            if rf and (name == rf or rf in blob or name in rf):
                s += 50
            for w in ["tv", "television", "oled", "lg", "samsung", "sony", "roku", "apple tv", "appletv", "shield", "receiver", "soundbar"]:
                if w in q and w in blob:
                    s += 10
            if "tv" in q and "tv" in blob:
                s += 5
            return s

        best = None
        best_score = -1
        for c in mps:
            sc = score(c)
            if sc > best_score:
                best_score = sc
                best = c

        if best and best_score >= 10:
            return best.get("entity_id")
        return None

    def _command_variants(self, base_or_raw: str) -> List[str]:
        """
        Return a list of likely command-name variants to try with remote.send_command.
        This is the key to "works with most remotes".
        """
        raw = (base_or_raw or "").strip()
        if not raw:
            return []

        variants: List[str] = []

        def add(v: str):
            v = (v or "").strip()
            if v and v not in variants:
                variants.append(v)

        add(raw)

        base = raw.strip().lower()
        base = base.replace("-", "_").replace(" ", "_")

        camel_to_snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw).lower().replace("-", "_").replace(" ", "_")
        if camel_to_snake and camel_to_snake != base:
            base = camel_to_snake

        common = {
            "volume_up": ["VolumeUp", "VOLUMEUP", "volume_up", "volumeUp", "KEY_VOLUMEUP", "KEY_VOLUP", "VOLUP"],
            "volume_down": ["VolumeDown", "VOLUMEDOWN", "volume_down", "volumeDown", "KEY_VOLUMEDOWN", "KEY_VOLDOWN", "VOLDOWN"],
            "mute": ["Mute", "MUTE", "mute", "KEY_MUTE", "VOLUME_MUTE"],
            "play": ["Play", "PLAY", "play", "KEY_PLAY"],
            "pause": ["Pause", "PAUSE", "pause", "KEY_PAUSE"],
            "stop": ["Stop", "STOP", "stop", "KEY_STOP"],
            "home": ["Home", "HOME", "home", "KEY_HOME"],
            "menu": ["Menu", "MENU", "menu", "KEY_MENU"],
            "back": ["Back", "BACK", "back", "KEY_BACK", "RETURN"],
            "select": ["Select", "SELECT", "select", "OK", "Ok", "ok", "ENTER", "Enter", "enter", "KEY_OK", "KEY_ENTER"],
            "up": ["Up", "UP", "up", "DirectionUp", "DIR_UP", "KEY_UP"],
            "down": ["Down", "DOWN", "down", "DirectionDown", "DIR_DOWN", "KEY_DOWN"],
            "left": ["Left", "LEFT", "left", "DirectionLeft", "DIR_LEFT", "KEY_LEFT"],
            "right": ["Right", "RIGHT", "right", "DirectionRight", "DIR_RIGHT", "KEY_RIGHT"],
            "rewind": ["Rewind", "REWIND", "rewind", "KEY_REWIND"],
            "fast_forward": ["FastForward", "FASTFORWARD", "fast_forward", "fastForward", "FF", "KEY_FASTFORWARD"],
        }

        if base in common:
            for v in common[base]:
                add(v)
        else:
            add(raw.upper())
            add(raw.lower())
            add(base)
            parts = [p for p in base.split("_") if p]
            if parts:
                camel = "".join(p[:1].upper() + p[1:] for p in parts)
                add(camel)

        return variants

    def _best_match_from_list(self, wanted: str, options: List[str]) -> Optional[str]:
        if not wanted or not options:
            return None
        w = wanted.strip().lower()
        for o in options:
            if isinstance(o, str) and o.strip().lower() == w:
                return o
        for o in options:
            if isinstance(o, str) and w in o.strip().lower():
                return o
        return None

    # ----------------------------
    # Candidate filtering helpers
    # ----------------------------
    @staticmethod
    def _normalize_for_match(text: str) -> str:
        t = (text or "").lower()
        t = re.sub(r"[^a-z0-9]+", " ", t)
        return re.sub(r"\s+", " ", t).strip()

    def _extract_keywords(self, text: str) -> List[str]:
        t = self._normalize_for_match(text)
        if not t:
            return []
        tokens = [tok for tok in t.split(" ") if tok]
        stop = {
            "turn", "on", "off", "the", "a", "an", "please", "to", "in", "of", "for",
            "set", "make", "switch", "device", "devices", "area", "room",
            "light", "lights", "lamp", "lamps", "bulb", "bulbs", "led", "hue", "sconce",
            "your", "you", "yours", "my", "mine", "our", "ours", "me", "us",
            # Color/style words should not be used as entity-name match tokens.
            "red", "green", "blue", "purple", "violet", "pink", "magenta", "orange",
            "yellow", "white", "warm", "cool", "cyan", "teal", "indigo", "lavender",
            "gold", "silver", "amber", "turquoise", "brightness", "bright", "dim",
        }
        out = [tok for tok in tokens if tok not in stop and len(tok) > 1]
        return out

    def _tokens_from_scope(self, scope: str) -> List[str]:
        s = (scope or "").strip().lower()
        if not s or s in {"inside", "outside", "unknown"}:
            return []
        if ":" in s:
            s = s.split(":", 1)[1]
        return self._extract_keywords(s)

    def _scope_from_origin(self, args: dict) -> str:
        src = args.get("origin") if isinstance(args, dict) else None
        if not isinstance(src, dict):
            return ""

        def _clean(v: Any) -> str:
            text = str(v or "").strip()
            if not text:
                return ""
            low = text.lower()
            if low in {"unknown", "default", "none", "null", "n/a"}:
                return ""
            return text

        area = _clean(src.get("area_id") or src.get("area_name") or src.get("room"))
        if area:
            return f"area:{area}"

        device = _clean(src.get("device_name") or src.get("device_id"))
        if device:
            return f"device:{device}"
        return ""

    def _filter_candidates_by_tokens(self, candidates: List[dict], tokens: List[str]) -> Tuple[List[dict], bool]:
        if not candidates or not tokens:
            return candidates, False
        scores = []
        for c in candidates:
            name = (c.get("name") or "").lower()
            eid = (c.get("entity_id") or "").lower()
            blob = f"{name} {eid}"
            score = 0.0
            for t in tokens:
                if not t:
                    continue
                if t in blob:
                    score += 1.0
                    continue
                for word in blob.split():
                    if difflib.SequenceMatcher(None, t, word).ratio() >= 0.86:
                        score += 0.6
                        break
            scores.append(score)
        max_score = max(scores) if scores else 0
        if max_score <= 0:
            return candidates, False
        return [c for c, sc in zip(candidates, scores) if sc == max_score], True

    def _shortlist_candidates_for_search(
        self,
        candidates: List[dict],
        *,
        search_text: str,
        fallback_text: str,
        limit: int = 48,
    ) -> Tuple[List[dict], bool]:
        if not candidates:
            return [], False

        phrase = self._normalize_for_match(search_text or fallback_text)
        tokens = self._extract_keywords(search_text or fallback_text)
        if not phrase and not tokens:
            return candidates[:limit], False

        scored: List[Tuple[float, dict]] = []
        for candidate in candidates:
            name = str(candidate.get("name") or "")
            eid = str(candidate.get("entity_id") or "")
            blob = self._normalize_for_match(f"{name} {eid}")
            if not blob:
                continue

            score = 0.0
            if phrase:
                if blob == phrase:
                    score += 8.0
                elif phrase in blob:
                    score += 4.5
                else:
                    phrase_parts = [part for part in phrase.split(" ") if part]
                    if phrase_parts and all(part in blob for part in phrase_parts):
                        score += 2.5

            for token in tokens:
                if token in blob:
                    score += 1.0
                    continue
                for word in blob.split():
                    if difflib.SequenceMatcher(None, token, word).ratio() >= 0.86:
                        score += 0.55
                        break

            if score > 0:
                scored.append((score, candidate))

        if not scored:
            return candidates[:limit], False

        scored.sort(
            key=lambda item: (
                -item[0],
                str(item[1].get("name") or "").lower(),
                str(item[1].get("entity_id") or "").lower(),
            )
        )
        return [candidate for _, candidate in scored[:limit]], True

    def _domains_for_route(
        self,
        route: str,
        *,
        mode: str,
        intent_type: str,
        action: str,
        query: str,
        domain_hint: str,
    ) -> Set[str]:
        route_key = str(route or "").strip().lower()
        if route_key == "light":
            return {"light"}
        if route_key == "switch":
            return {"switch"}
        if route_key == "climate":
            return {"climate"}
        if route_key == "cover":
            return {"cover"}
        if route_key == "lock":
            return {"lock"}
        if route_key == "fan":
            return {"fan"}
        if route_key == "scene":
            return {"scene"}
        if route_key == "script":
            return {"script"}
        if route_key == "remote":
            return {"remote"}
        if route_key == "media_player":
            return {"media_player"}
        if route_key in {"sensor", "temperature"}:
            return {"sensor", "binary_sensor", "select"}
        hinted = str(domain_hint or "").strip().lower()
        if hinted in {"light", "switch", "climate", "cover", "lock", "fan", "scene", "script", "remote", "media_player"}:
            return {hinted}
        if hinted in {"sensor", "binary_sensor", "select"}:
            return {"sensor", "binary_sensor", "select"}
        if mode == "control":
            return {"light", "switch", "fan", "media_player", "scene", "script", "cover", "lock", "remote"}
        if intent_type == "get_temp":
            return {"sensor"}
        return {"sensor", "binary_sensor", "lock", "cover", "light", "switch", "fan", "media_player", "climate", "remote", "select"}

    def _route_search_seed(self, query: str, scope: str, route_info: Optional[dict]) -> str:
        route_info = route_info if isinstance(route_info, dict) else {}
        search_text = str(route_info.get("search_text") or "").strip()
        scope_text = ""
        raw_scope = str(scope or "").strip()
        if raw_scope and raw_scope.lower() not in {"unknown", "inside", "outside"}:
            scope_text = raw_scope.split(":", 1)[-1].strip()
        if search_text and scope_text and scope_text.lower() not in search_text.lower():
            return f"{scope_text} {search_text}".strip()
        return search_text or scope_text or query

    def _prepare_routed_candidates(
        self,
        *,
        catalog: List[dict],
        route: str,
        mode: str,
        intent_type: str,
        action: str,
        query: str,
        scope: str,
        domain_hint: str,
        excluded: Set[str],
        route_info: Optional[dict],
    ) -> List[dict]:
        domains = self._domains_for_route(
            route,
            mode=mode,
            intent_type=intent_type,
            action=action,
            query=query,
            domain_hint=domain_hint,
        )
        candidates = self._candidates_for_domains(catalog, domains)

        if excluded:
            candidates = [
                candidate for candidate in candidates
                if not (
                    (candidate.get("domain") or "").lower() == "light"
                    and (candidate.get("entity_id") or "").lower() in excluded
                )
            ]

        search_seed = self._route_search_seed(query, scope, route_info)
        if search_seed:
            shortlisted, narrowed = self._shortlist_candidates_for_search(
                candidates,
                search_text=search_seed,
                fallback_text=query,
            )
            if narrowed and shortlisted:
                return shortlisted
        return candidates

    # ----------------------------
    # Catalog (grounding)
    # ----------------------------
    def _catalog_cache_key(self) -> str:
        return "ha_switches:catalog:v4"

    def _build_catalog_from_states(self, states: List[dict]) -> List[dict]:
        catalog: List[dict] = []
        for s in states:
            if not isinstance(s, dict):
                continue
            eid = s.get("entity_id", "")
            if "." not in eid:
                continue

            dom = eid.split(".", 1)[0]
            attrs = s.get("attributes") or {}
            if not isinstance(attrs, dict):
                attrs = {}

            catalog.append({
                "entity_id": eid,
                "domain": dom,
                "name": attrs.get("friendly_name") or eid,
                "device_class": attrs.get("device_class"),
                "unit": attrs.get("unit_of_measurement"),
            })
        return catalog

    def _get_catalog_cached(self, client: HAClient) -> List[dict]:
        cache_seconds = self._get_int_setting("HA_CATALOG_CACHE_SECONDS", 60)
        key = self._catalog_cache_key()

        try:
            raw = redis_client.get(key)
            if raw:
                data = _json.loads(raw)
                if isinstance(data, dict) and "ts" in data and "catalog" in data:
                    ts = float(data["ts"])
                    if time.time() - ts <= max(5, cache_seconds):
                        cat = data["catalog"]
                        if isinstance(cat, list):
                            return cat
        except Exception:
            pass

        states = client.list_states()
        catalog = self._build_catalog_from_states(states)

        try:
            redis_client.set(key, _json.dumps({"ts": time.time(), "catalog": catalog}, ensure_ascii=False))
            try:
                redis_client.expire(key, max(10, cache_seconds))
            except Exception:
                pass
        except Exception:
            pass

        return catalog

    def _catalog_ts(self) -> Optional[float]:
        """
        Read catalog timestamp from cache if available (used for choose-cache keying).
        """
        key = self._catalog_cache_key()
        try:
            raw = redis_client.get(key)
            if raw:
                data = _json.loads(raw)
                if isinstance(data, dict) and "ts" in data:
                    return float(data["ts"])
        except Exception:
            pass
        return None

    # ----------------------------
    # LLM caching helpers
    # ----------------------------
    def _cache_get_json(self, key: str) -> Optional[dict]:
        try:
            raw = redis_client.get(key)
            if not raw:
                return None
            data = _json.loads(raw)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _cache_set_json(self, key: str, value: dict, ttl_seconds: int):
        try:
            redis_client.set(key, _json.dumps(value, ensure_ascii=False))
            if ttl_seconds and ttl_seconds > 0:
                try:
                    redis_client.expire(key, int(ttl_seconds))
                except Exception:
                    pass
        except Exception:
            pass

    # ----------------------------
    # Step 1: LLM interprets query → intent (with cache)
    # ----------------------------
    async def _base_interpret_query(self, query: str, llm_client) -> dict:
        allowed_domain = "light,switch,climate,sensor,binary_sensor,cover,lock,fan,media_player,scene,script,select,remote"

        cache_ttl = self._get_int_setting("HA_INTERPRET_CACHE_SECONDS", 45)
        cache_key = f"ha_switches:interpret:v1:{query.strip().lower()}"
        if cache_ttl > 0:
            cached = self._cache_get_json(cache_key)
            if cached and isinstance(cached, dict) and cached.get("intent"):
                return cached

        system = (
            "You are interpreting a smart-home request for Home Assistant.\n"
            "Return STRICT JSON only. No explanation.\n"
            "Schema:\n"
            "{\n"
            '  "intent": "get_temp|get_state|control|set_temperature",\n'
            '  "action": "turn_on|turn_off|open|close|get_state|set_temperature|send_command",\n'
            '  "scope": "inside|outside|area:<name>|device:<phrase>|unknown",\n'
            f'  "domain_hint": "one of: {allowed_domain}",\n'
            '  "read_target": "none|state|target_temperature|current_temperature",\n'
            '  "desired": {\n'
            '     "temperature": <number or null>,\n'
            '     "brightness_pct": <int 0-100 or null>,\n'
            '     "color_name": <string or null>,\n'
            '     "activity": <string or null>,\n'
            '     "command": <string or null>\n'
            "  }\n"
            "}\n"
            "Rules:\n"
            "- If user asks 'what's the temp inside' or 'temp in the kitchen', intent=get_temp, action=get_state.\n"
            "- If user asks 'thermostat set to' or 'what is the thermostat set to', intent=get_state, domain_hint=climate, read_target=target_temperature.\n"
            "- If user asks for thermostat current temp/current temperature, use intent=get_state, domain_hint=climate, read_target=current_temperature.\n"
            "- For normal status/state questions, use read_target=state.\n"
            "- When not reading state, use read_target=none.\n"
            "- If user says 'set thermostat to 74', intent=set_temperature, action=set_temperature, domain_hint=climate.\n"
            "- For lights, domain_hint=light and action turn_on/turn_off accordingly.\n"
            "- If user says 'set lights to blue' / 'turn lights blue', that's lights (domain_hint=light), NOT thermostat.\n"
            "- If user asks to set lights to a percent (brightness), you MUST use intent=control and action=turn_on,\n"
            "  and put the percent into desired.brightness_pct. Do NOT use actions like set_brightness.\n"
            "- For remotes, domain_hint=remote.\n"
            "- Use domain_hint=switch for switches, plugs, outlets, and simple power control for named devices like TVs, arcade cabinets, consoles, receivers, chargers, or lamps when they may actually be exposed as smart plugs/switches.\n"
            "- Use domain_hint=media_player for native playback/source/app/state requests or clearly native media-player device control.\n"
            "- For simple 'turn on/off the TV' style requests, domain_hint=switch is acceptable and preferred when the device could plausibly be controlled as a plug/switch.\n"
            "- 'mute', 'volume up', 'pause', 'play', 'home', 'back', 'menu' means action=send_command and desired.command.\n"
            "- If scope is a room/area (kitchen, living room), use scope=area:<name>.\n"
            "- If it's a named device (christmas tree lights), use scope=device:<phrase>.\n"
            "Examples:\n"
            '- "turn off office lights" -> {"intent":"control","action":"turn_off","scope":"area:office","domain_hint":"light","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "set office lights to blue" -> {"intent":"control","action":"turn_on","scope":"area:office","domain_hint":"light","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":"blue","activity":null,"command":null}}\n'
            '- "set kitchen lights to 50 percent" -> {"intent":"control","action":"turn_on","scope":"area:kitchen","domain_hint":"light","read_target":"none","desired":{"temperature":null,"brightness_pct":50,"color_name":null,"activity":null,"command":null}}\n'
            '- "turn on the kitchen plug" -> {"intent":"control","action":"turn_on","scope":"area:kitchen","domain_hint":"switch","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "power on the arcade" -> {"intent":"control","action":"turn_on","scope":"device:arcade","domain_hint":"switch","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "turn off the living room tv" -> {"intent":"control","action":"turn_off","scope":"area:living room","domain_hint":"switch","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "what is the living room tv playing" -> {"intent":"get_state","action":"get_state","scope":"area:living room","domain_hint":"media_player","read_target":"state","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "mute the roku" -> {"intent":"control","action":"send_command","scope":"device:roku","domain_hint":"remote","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":"mute"}}\n'
            '- "pause the family room apple tv" -> {"intent":"control","action":"send_command","scope":"area:family room","domain_hint":"remote","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":"pause"}}\n'
            '- "open the garage door" -> {"intent":"control","action":"open","scope":"device:garage door","domain_hint":"cover","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "is the front door locked" -> {"intent":"get_state","action":"get_state","scope":"device:front door","domain_hint":"lock","read_target":"state","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "turn on the bedroom fan" -> {"intent":"control","action":"turn_on","scope":"area:bedroom","domain_hint":"fan","read_target":"none","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "set the hallway thermostat to 72" -> {"intent":"set_temperature","action":"set_temperature","scope":"area:hallway","domain_hint":"climate","read_target":"none","desired":{"temperature":72,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "what is the thermostat set to in the office" -> {"intent":"get_state","action":"get_state","scope":"area:office","domain_hint":"climate","read_target":"target_temperature","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "what is the current temperature on the hallway thermostat" -> {"intent":"get_state","action":"get_state","scope":"area:hallway","domain_hint":"climate","read_target":"current_temperature","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
            '- "what is the temperature in the kitchen" -> {"intent":"get_temp","action":"get_state","scope":"area:kitchen","domain_hint":"sensor","read_target":"state","desired":{"temperature":null,"brightness_pct":null,"color_name":null,"activity":null,"command":null}}\n'
        )

        resp = await llm_client.chat(messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": query.strip()},
        ])
        content = (resp.get("message", {}) or {}).get("content", "").strip()
        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
        data = _json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("LLM interpret_query did not return JSON object")

        if cache_ttl > 0:
            self._cache_set_json(cache_key, data, cache_ttl)

        return data

    # ----------------------------
    # Step 2: Candidate building (grounded)
    # ----------------------------
    def _candidates_temperature(self, catalog: List[dict], scope: str) -> List[dict]:
        temps = [
            c for c in catalog
            if (c.get("domain") == "sensor" and (c.get("device_class") or "").lower() == "temperature")
        ]
        if not temps:
            return []

        scope_l = (scope or "").lower()

        if scope_l == "inside":
            outdoor_words = ("outside", "outdoor", "yard", "back yard", "backyard", "front yard", "porch", "patio", "driveway")
            filtered = []
            for c in temps:
                name = (c.get("name") or "").lower()
                eid = (c.get("entity_id") or "").lower()
                blob = f"{name} {eid}"
                if any(w in blob for w in outdoor_words):
                    continue
                filtered.append(c)
            return filtered if filtered else temps

        return temps

    def _candidates_for_domains(self, catalog: List[dict], domains: Set[str]) -> List[dict]:
        doms = {d.lower().strip() for d in (domains or set()) if d}
        return [c for c in catalog if (c.get("domain") or "").lower() in doms]

    # ----------------------------
    # Step 3: LLM chooser (grounded) + tournament chunking + cache + single-candidate shortcut
    # ----------------------------
    async def _choose_entity_llm(
        self,
        query: str,
        intent: dict,
        candidates: List[dict],
        llm_client,
        *,
        route: str = "",
    ) -> Optional[str]:
        if not candidates:
            return None

        # ✅ If only one candidate, skip the LLM entirely
        if len(candidates) == 1 and candidates[0].get("entity_id"):
            return candidates[0]["entity_id"]

        mini = [{
            "entity_id": c.get("entity_id"),
            "domain": c.get("domain"),
            "name": c.get("name"),
            "device_class": c.get("device_class"),
            "unit": c.get("unit"),
        } for c in candidates if c.get("entity_id")]

        if not mini:
            return None

        candidate_set = {c["entity_id"] for c in mini if c.get("entity_id")}

        # ✅ Choose-cache keyed by query + (approx) catalog timestamp
        cache_ttl = self._get_int_setting("HA_CHOOSE_CACHE_SECONDS", 45)
        cat_ts = self._catalog_ts()
        cache_key = None
        if cache_ttl > 0:
            cache_key = f"ha_switches:choose:v3:{(query or '').strip().lower()}:{str(cat_ts or 'none')}:{_json.dumps(intent, sort_keys=True, ensure_ascii=False)}"
            try:
                cached = self._cache_get_json(cache_key)
                if cached and isinstance(cached, dict):
                    ceid = cached.get("entity_id")
                    if isinstance(ceid, str) and ceid in candidate_set:
                        return ceid
            except Exception:
                pass

        route_hint = str(route or "").strip().lower()
        route_rule = ""
        if route_hint and route_hint != "unknown":
            route_rule = f"Candidates are already filtered to the {route_hint} path. Stay on that path.\n"
            if route_hint == "switch":
                route_rule += (
                    "Switch path includes smart plugs/outlets and simple power entities.\n"
                    "Prefer switch.* candidates for TVs, arcade cabinets, receivers, consoles, chargers, lamps, or similar gear when they appear to be plug-backed devices.\n"
                )
            elif route_hint == "media_player":
                route_rule += (
                    "Media-player path is for native media devices and playback/state control.\n"
                    "Prefer media_player.* candidates for playback/source/app/state, not generic smart-plug power.\n"
                )
            elif route_hint == "light":
                route_rule += "Prefer light.* candidates only; do not drift to switch.* just because names look similar.\n"

        system = (
            "Pick the SINGLE best Home Assistant entity for this user request.\n"
            "You MUST choose an entity_id from the provided candidates (no inventions).\n"
            "Return strict JSON only: {\"entity_id\":\"...\"}. No explanation.\n"
            "Use the user's exact words to match rooms/devices.\n"
            "If the request is temperature inside, do NOT pick obvious outside/outdoor sensors.\n"
            "For TV/device power requests, choose the entity type that best matches the current path and the candidate names shown.\n"
            "Use remote.* mainly for button presses (volume/mute/home/back/menu/select) or starting activities.\n"
            f"{route_rule}"
        )

        async def ask_pick(chunk: List[dict]) -> Optional[str]:
            user = _json.dumps({
                "query": query,
                "intent": intent,
                "candidates": chunk
            }, ensure_ascii=False)
            resp = await llm_client.chat(messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ])
            content = (resp.get("message", {}) or {}).get("content", "").strip()
            content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.MULTILINE).strip()
            data = _json.loads(content)
            eid = data.get("entity_id")
            if isinstance(eid, str) and eid.strip():
                eid = eid.strip()
                return eid if eid in candidate_set else None
            return None

        max_single = self._get_int_setting("HA_MAX_CANDIDATES", 400, minimum=1, maximum=5000)
        chunk_size = self._get_int_setting("HA_CHUNK_SIZE", 120, minimum=1, maximum=1000)

        # Prefer single-shot when reasonable
        picked: Optional[str] = None
        if len(mini) <= max_single:
            try:
                picked = await ask_pick(mini)
            except Exception as e:
                logger.warning(f"[ha_switches] LLM choose failed single-shot: {e}")

        # Tournament fallback
        if not picked:
            try:
                winners: List[dict] = []
                for i in range(0, len(mini), chunk_size):
                    chunk = mini[i:i + chunk_size]
                    eid = await ask_pick(chunk)
                    if eid:
                        winners.append(next(c for c in chunk if c["entity_id"] == eid))

                if winners:
                    eid = await ask_pick(winners)
                    picked = eid or winners[0]["entity_id"]
                else:
                    picked = mini[0]["entity_id"] if mini else None
            except Exception as e:
                logger.warning(f"[ha_switches] LLM choose failed tournament: {e}")
                picked = mini[0]["entity_id"] if mini else None

        if picked and cache_key and cache_ttl > 0:
            self._cache_set_json(cache_key, {"entity_id": picked}, cache_ttl)

        return picked

    # ----------------------------
    # Service mapping + confirmation
    # ----------------------------
    def _service_for_action(self, action: str, entity_domain: str) -> Optional[Tuple[str, dict]]:
        a = (action or "").lower().strip()
        d = (entity_domain or "").lower().strip()

        # ✅ (A) Brightness is implemented via light.turn_on with brightness_pct
        if a in ("set_brightness", "brightness", "dim", "set_level") and d == "light":
            return "turn_on", {}

        if a in ("turn_on", "turn_off"):
            return a, {}

        if a == "open":
            if d == "cover":
                return "open_cover", {}
            if d == "lock":
                return "unlock", {}
            return "open", {}

        if a == "close":
            if d == "cover":
                return "close_cover", {}
            if d == "lock":
                return "lock", {}
            return "close", {}

        if a == "set_temperature" and d == "climate":
            return "set_temperature", {}

        # ✅ Remote mapping (Harmony/Apple TV/etc.)
        if d == "remote":
            if a in ("turn_on", "start", "power_on", "on"):
                return "turn_on", {}
            if a in ("turn_off", "stop", "power_off", "off"):
                return "turn_off", {}
            if a in ("send_command", "command", "press", "mute", "volume_up", "volume_down", "pause", "play", "home", "back", "select", "ok", "menu"):
                return "send_command", {}

        return None

    def _expected_states_for_action(self, service: str, entity_domain: str) -> Optional[set[str]]:
        """
        Best-effort expected state mapping for verification.
        Returns None when we cannot reliably verify.
        """
        svc = (service or "").lower().strip()
        dom = (entity_domain or "").lower().strip()

        if dom in ("light", "switch", "fan"):
            if svc == "turn_on":
                return {"on"}
            if svc == "turn_off":
                return {"off"}

        if dom == "cover":
            if svc in ("open_cover", "open"):
                return {"open"}
            if svc in ("close_cover", "close"):
                return {"closed"}

        if dom == "lock":
            if svc == "lock":
                return {"locked"}
            if svc == "unlock":
                return {"unlocked"}

        return None

    async def _speak_response_state(self, user_query: str, friendly: str, value: str, unit: str, llm_client) -> str:
        return f"{friendly} is {value}{(' ' + unit) if unit else ''}."

    async def _speak_response_confirm(self, user_query: str, friendly: str, action_spoken: str, extras: str, llm_client) -> str:
        extras_txt = f" {extras.strip()}" if str(extras or "").strip() else ""
        return f"Okay, {action_spoken} {friendly}.{extras_txt}".strip()

    def _ha_diagnosis(self) -> dict:
        hash_diag = diagnose_hash_fields(
            "homeassistant_settings",
            fields={"ha_base_url": "HA_BASE_URL", "ha_token": "HA_TOKEN"},
            validators={
                "ha_base_url": lambda v: v.startswith("http://") or v.startswith("https://"),
                "ha_token": lambda v: len(v.strip()) >= 10,
            },
        )
        key_diag = diagnose_redis_keys(
            keys={
                "ha_base_url": "tater:homeassistant:base_url",
                "ha_token": "tater:homeassistant:token",
            },
            validators={
                "ha_base_url": lambda v: v.startswith("http://") or v.startswith("https://"),
                "ha_token": lambda v: len(v.strip()) >= 10,
            },
        )
        # Prefer explicit WebUI settings; only fall back to legacy keys when missing.
        merged = dict(hash_diag)
        for k, v in (key_diag or {}).items():
            if merged.get(k) == "missing":
                merged[k] = v
        return merged

    def _structured_from_text(self, text: str, query: str, args: Optional[dict] = None) -> dict:
        msg = (text or "").strip()
        lower = msg.lower()
        args = args or {}
        action = (args.get("action") or "").strip().lower()
        entity_id = (args.get("entity_id") or "").strip()
        scope = (args.get("scope") or "").strip()

        if not msg:
            return action_failure(
                code="empty_result",
                message="No response from Home Assistant control.",
                diagnosis=self._ha_diagnosis(),
                needs=["What should I control?"],
                say_hint="Explain that no result was returned and ask for the exact device/action.",
            )

        if "token is not set" in lower:
            diagnosis = self._ha_diagnosis()
            needs = needs_from_diagnosis(
                diagnosis,
                {
                    "ha_base_url": "Please set your Home Assistant base URL in settings.",
                    "ha_token": "Please set your Home Assistant long-lived access token in settings.",
                },
            )
            return action_failure(
                code="ha_auth_missing",
                message="Home Assistant authentication is not configured.",
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain setup is incomplete and ask for missing Home Assistant settings.",
            )

        is_failure = (
            lower.startswith("error ")
            or lower.startswith("i couldn't")
            or lower.startswith("i couldn’t")
            or lower.startswith("please provide")
            or lower.startswith("tell me what")
            or "is not supported" in lower
            or "couldn’t find" in lower
            or "couldn't find" in lower
        )

        if is_failure:
            needs = []
            if "query" in lower or "action" in lower or "missing action" in lower:
                needs.append("What exact Home Assistant action should I run?")
            if "couldn" in lower and "find" in lower:
                needs.append("Which device or room should I target?")
            if "entity_id" in lower or "entity id" in lower:
                needs.append("Which device or room should I target? (Exact entity_id optional if you know it.)")
            return action_failure(
                code="ha_request_failed",
                message=msg,
                diagnosis=self._ha_diagnosis(),
                needs=needs,
                say_hint="Explain what failed and ask only the missing details needed to continue.",
            )

        return action_success(
            facts={
                "query": query,
                "action": action or "auto",
                "entity_id": entity_id,
                "scope": scope,
                "result": msg,
            },
            say_hint="Confirm the completed Home Assistant action using these facts only.",
            suggested_followups=[
                "Want me to adjust anything else in Home Assistant?",
            ],
        )

    async def _handle_structured(self, args, llm_client):
        args = args or {}
        query = (args.get("query") or "").strip()
        out = await self._handle(args, llm_client)
        if isinstance(out, dict) and "ok" in out:
            return out
        return self._structured_from_text(str(out), query, {"query": query})

    # ----------------------------
    # Handlers
    # ----------------------------
    async def handle_homeassistant(self, args, llm_client):
        return await self._handle_structured(args, llm_client)

    async def handle_webui(self, args, llm_client):
        return await self._handle_structured(args, llm_client)


    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self.handle_webui(args, llm_client, context=context)
        except TypeError:
            return await self.handle_webui(args, llm_client)
    async def handle_xbmc(self, args, llm_client):
        return await self._handle_structured(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        return await self._handle_structured(args, llm_client)

    async def handle_discord(self, message, args, llm_client):
        return await self._handle_structured(args, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self._handle_structured(args, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self._handle_structured(args, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._handle_structured(args, llm_client)

    # ----------------------------
    # Core logic
    # ----------------------------
    async def _base_handle(self, args, llm_client):
        client = self._get_client()
        if not client:
            return (
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        args = args or {}
        query = (args.get("query") or "").strip()
        if not query:
            return "Please provide a Home Assistant request in 'query'."
        explicit_entity = ""

        excluded = self._excluded_entities_set()

        try:
            catalog = self._get_catalog_cached(client)
        except Exception as e:
            logger.error(f"[ha_switches] catalog build failed: {e}")
            return "I couldn't access Home Assistant states."

        try:
            intent = await self._interpret_query(query, llm_client)
        except Exception as e:
            logger.error(f"[ha_switches] interpret_query failed: {e}")
            return "I couldn't understand that request."

        intent_type = (intent.get("intent") or "").strip()
        action = (intent.get("action") or "").strip()
        scope = (intent.get("scope") or "").strip()
        domain_hint = (intent.get("domain_hint") or "").strip()
        read_target = str(intent.get("read_target") or "").strip().lower()
        if not scope or scope.lower() == "unknown":
            scope_from_origin = self._scope_from_origin(args)
            if scope_from_origin:
                scope = scope_from_origin

        desired = intent.get("desired") or {}
        if not isinstance(desired, dict):
            desired = {}

        route_info = await self._route_query(
            query,
            {
                "intent": intent_type,
                "action": action,
                "scope": scope,
                "domain_hint": domain_hint,
                "read_target": read_target,
                "desired": desired,
            },
            llm_client,
        )
        primary_route = str((route_info or {}).get("route") or "").strip().lower() or "unknown"

        if intent_type == "get_temp" or primary_route == "temperature":
            candidates = self._candidates_temperature(catalog, scope.lower() if scope else "unknown")
            if excluded:
                candidates = [c for c in candidates if (c.get("entity_id") or "").lower() not in excluded]
            temp_search = self._route_search_seed(query, scope, route_info)
            if temp_search:
                shortlisted, narrowed = self._shortlist_candidates_for_search(
                    candidates,
                    search_text=temp_search,
                    fallback_text=query,
                )
                if narrowed and shortlisted:
                    candidates = shortlisted

            entity_id = explicit_entity or await self._choose_entity_llm(
                query,
                {"intent": "get_temp", "scope": scope},
                candidates,
                llm_client,
                route="temperature",
            )
            if not entity_id:
                return "I couldn’t find a temperature sensor for that."

            try:
                st = client.get_state(entity_id)
                val = st.get("state", "unknown") if isinstance(st, dict) else str(st)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)
                unit = (attrs.get("unit_of_measurement") or "")
                return await self._speak_response_state(query, friendly, str(val), str(unit), llm_client)
            except Exception as e:
                logger.error(f"[ha_switches] temp get_state error: {e}")
                return f"Error reading {entity_id}: {e}"

        if primary_route == "climate" and intent_type in ("get_state", "control") and action == "get_state":
            climate_candidates = self._prepare_routed_candidates(
                catalog=catalog,
                route="climate",
                mode="get_state",
                intent_type=intent_type,
                action=action,
                query=query,
                scope=scope,
                domain_hint="climate",
                excluded=excluded,
                route_info=route_info,
            )

            entity_id = explicit_entity
            if entity_id and not entity_id.startswith("climate."):
                return "That entity_id is not a climate device. Please provide a climate entity."
            if not entity_id:
                entity_id = await self._choose_entity_llm(
                    query,
                    {"intent": "get_state", "domain_hint": "climate"},
                    climate_candidates,
                    llm_client,
                    route="climate",
                )
            if not entity_id:
                return "I couldn’t find a thermostat."

            try:
                st = client.get_state(entity_id)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)

                if read_target == "target_temperature":
                    temp_val = attrs.get("temperature")
                    unit = attrs.get("unit_of_measurement") or "°F"
                    if temp_val is not None:
                        return await self._speak_response_state(query, friendly, str(temp_val), str(unit), llm_client)
                elif read_target == "current_temperature":
                    temp_val = attrs.get("current_temperature")
                    unit = attrs.get("unit_of_measurement") or "°F"
                    if temp_val is not None:
                        return await self._speak_response_state(query, friendly, str(temp_val), str(unit), llm_client)

                val = st.get("state", "unknown") if isinstance(st, dict) else str(st)
                return await self._speak_response_state(query, friendly, str(val), "", llm_client)
            except Exception as e:
                logger.error(f"[ha_switches] thermostat read error: {e}")
                return f"Error reading {entity_id}: {e}"

        if intent_type == "set_temperature" or action == "set_temperature":
            candidates = self._prepare_routed_candidates(
                catalog=catalog,
                route="climate",
                mode="control",
                intent_type=intent_type,
                action=action,
                query=query,
                scope=scope,
                domain_hint="climate",
                excluded=excluded,
                route_info=route_info,
            )

            entity_id = explicit_entity
            if entity_id and not entity_id.startswith("climate."):
                return "That entity_id is not a climate device. Please provide a thermostat entity."
            if not entity_id:
                entity_id = await self._choose_entity_llm(
                    query,
                    intent,
                    candidates,
                    llm_client,
                    route="climate",
                )
            if not entity_id:
                return "I couldn’t find a thermostat to set."

            temperature = desired.get("temperature")
            try:
                temperature = float(temperature) if temperature is not None else None
            except Exception:
                temperature = None
            if temperature is None:
                return "Tell me what temperature you want, like 'set the thermostat to 74'."

            payload = {"entity_id": entity_id, "temperature": temperature}
            try:
                client.call_service("climate", "set_temperature", payload)
                st = client.get_state(entity_id)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)
                return await self._speak_response_confirm(query, friendly, f"set to {int(temperature)} degrees", "", llm_client)
            except Exception as e:
                logger.error(f"[ha_switches] set_temperature error: {e}")
                return f"Error setting {entity_id}: {e}"

        if intent_type == "control":
            candidates = self._prepare_routed_candidates(
                catalog=catalog,
                route=primary_route,
                mode="control",
                intent_type=intent_type,
                action=action,
                query=query,
                scope=scope,
                domain_hint=domain_hint,
                excluded=excluded,
                route_info=route_info,
            )

            entity_id = explicit_entity or await self._choose_entity_llm(
                query,
                intent,
                candidates,
                llm_client,
                route=primary_route,
            )
            if not entity_id:
                return "I couldn’t find a device matching that."

            if "." not in entity_id:
                return "I couldn’t find a valid Home Assistant entity to control."

            entity_domain = entity_id.split(".", 1)[0].lower()
            mapped = self._service_for_action(action, entity_domain)
            if not mapped:
                return f"The action '{action}' is not supported for {entity_domain}."

            service, extra = mapped

            payload = {"entity_id": entity_id}
            payload.update(extra)

            extras_txt_parts: List[str] = []

            # Light extras
            if entity_domain == "light" and service in ("turn_on", "turn_off"):
                if desired.get("color_name"):
                    payload["color_name"] = str(desired["color_name"])
                    extras_txt_parts.append(f"color {payload['color_name']}")
                if desired.get("brightness_pct") is not None:
                    try:
                        payload["brightness_pct"] = int(desired["brightness_pct"])
                        extras_txt_parts.append(f"brightness {payload['brightness_pct']} percent")
                    except Exception:
                        pass

            # Remote (Harmony/Apple TV/etc.) extras/payload with compatibility retries
            if entity_domain == "remote":
                # Snapshot state before (to detect "no-op")
                try:
                    st_before = client.get_state(entity_id)
                except Exception:
                    st_before = None
                before_key = self._state_key(st_before)

                # Grab current attributes (for activity_list matching + friendly name)
                attrs_now = (st_before.get("attributes") or {}) if isinstance(st_before, dict) else {}
                if not isinstance(attrs_now, dict):
                    attrs_now = {}

                activity_list = attrs_now.get("activity_list") or attrs_now.get("activities") or []
                if not isinstance(activity_list, list):
                    activity_list = []

                desired_activity = desired.get("activity") if isinstance(desired.get("activity"), str) else ""
                desired_command = desired.get("command") if isinstance(desired.get("command"), str) else ""

                desired_activity = (desired_activity or "").strip()
                desired_command = (desired_command or "").strip()

                # Align guessed activity to real activity list when available
                if desired_activity and activity_list:
                    match = self._best_match_from_list(desired_activity, [a for a in activity_list if isinstance(a, str)])
                    if match:
                        desired_activity = match

                try:
                    if service == "turn_on":
                        did_work = False

                        # Try remote.turn_on first (with activity if any)
                        try:
                            if desired_activity:
                                payload_with_activity = dict(payload)
                                payload_with_activity["activity"] = desired_activity
                                client.call_service("remote", "turn_on", payload_with_activity)
                                extras_txt_parts.append(desired_activity)
                            else:
                                client.call_service("remote", "turn_on", payload)

                            # Detect effect
                            try:
                                st_after = client.get_state(entity_id)
                                after_key = self._state_key(st_after)
                                did_work = bool(after_key and after_key != before_key)
                            except Exception:
                                did_work = True  # we successfully called the service; assume ok

                        except Exception as e:
                            logger.info(f"[ha_switches] remote.turn_on failed: {e}")

                        # If the remote didn't show any change, try powering a related media_player
                        if not did_work:
                            remote_friendly = attrs_now.get("friendly_name") or entity_id
                            mp_eid = self._find_related_media_player(catalog, str(remote_friendly), query)
                            if mp_eid:
                                try:
                                    client.call_service("media_player", "turn_on", {"entity_id": mp_eid})
                                    extras_txt_parts.append("power")
                                    did_work = True
                                except Exception as e:
                                    logger.info(f"[ha_switches] media_player.turn_on fallback failed: {e}")

                        if not did_work:
                            return (
                                "I tried to turn it on, but that remote didn’t report any change. "
                                "If you tell me which Home Assistant entity actually powers that TV on, I can use it."
                            )

                    elif service == "turn_off":
                        # remote.turn_off sometimes works; if not, we can fall back to media_player.turn_off
                        did_work = False
                        try:
                            client.call_service("remote", "turn_off", payload)
                            try:
                                st_after = client.get_state(entity_id)
                                after_key = self._state_key(st_after)
                                did_work = bool(after_key and after_key != before_key)
                            except Exception:
                                did_work = True
                        except Exception as e:
                            logger.info(f"[ha_switches] remote.turn_off failed: {e}")

                        if not did_work:
                            remote_friendly = attrs_now.get("friendly_name") or entity_id
                            mp_eid = self._find_related_media_player(catalog, str(remote_friendly), query)
                            if mp_eid:
                                try:
                                    client.call_service("media_player", "turn_off", {"entity_id": mp_eid})
                                    extras_txt_parts.append("power")
                                    did_work = True
                                except Exception as e:
                                    logger.info(f"[ha_switches] media_player.turn_off fallback failed: {e}")

                        if not did_work:
                            return (
                                "I tried to turn it off, but that remote didn’t report any change. "
                                "If you tell me which Home Assistant entity actually powers that TV off, I can use it."
                            )

                    elif service == "send_command":
                        if not desired_command:
                            return "Tell me what button to press, like 'mute' or 'volume up'."

                        tried: List[str] = []
                        variants = self._command_variants(desired_command)

                        last_error: Optional[Exception] = None
                        for cmd in variants:
                            tried.append(cmd)
                            sent = False
                            for command_value in ([cmd], cmd):
                                payload_cmd = dict(payload)
                                payload_cmd["command"] = command_value
                                try:
                                    client.call_service("remote", "send_command", payload_cmd)
                                    extras_txt_parts.append(cmd)
                                    last_error = None
                                    sent = True
                                    break
                                except Exception as e:
                                    last_error = e
                                    continue
                            if sent:
                                break

                        if last_error is not None:
                            logger.info(f"[ha_switches] remote.send_command failed. Tried: {tried}. Last error: {last_error}")
                            return (
                                "I tried a few command names for that remote, but none worked. "
                                "If you tell me what the button is called in Home Assistant, I can use it."
                            )
                    else:
                        client.call_service(entity_domain, service, payload)

                except Exception as e:
                    logger.error(f"[ha_switches] remote control error: {e}")
                    return f"Error performing {service} on {entity_id}: {e}"

                # Confirm
                try:
                    st = client.get_state(entity_id)
                    attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                    friendly = (attrs.get("friendly_name") or entity_id)

                    if service == "turn_on":
                        spoken_action = "turned on"
                    elif service == "turn_off":
                        spoken_action = "turned off"
                    elif service == "send_command":
                        spoken_action = "sent"
                    else:
                        spoken_action = service.replace("_", " ")

                    extras_txt = ", ".join(extras_txt_parts) if extras_txt_parts else ""
                    return await self._speak_response_confirm(query, friendly, spoken_action, extras_txt, llm_client)
                except Exception as e:
                    logger.error(f"[ha_switches] remote post-state error: {e}")
                    return "Done."

            # Generic non-remote control path
            try:
                st_before = None
                try:
                    st_before = client.get_state(entity_id)
                except Exception:
                    st_before = None

                client.call_service(entity_domain, service, payload)

                # Give HA a moment to update state; poll a few times before failing.
                expected = self._expected_states_for_action(service, entity_domain)
                st = None
                state_after = ""
                for delay in (0.4, 0.7, 1.0):
                    await asyncio.sleep(delay)
                    st = client.get_state(entity_id)
                    state_after = (st.get("state") if isinstance(st, dict) else str(st or "")).lower().strip()
                    if expected and state_after in expected:
                        break

                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)
                state_before = (
                    (st_before.get("state") if isinstance(st_before, dict) else str(st_before or "")).lower().strip()
                )

                if expected:
                    if state_after in {"unknown", "unavailable", ""}:
                        # proceed but mark as unverified
                        verified = False
                    elif state_after not in expected:
                        return action_failure(
                            code="ha_state_not_changed",
                            message=f"{friendly} did not change to the expected state.",
                            diagnosis=self._ha_diagnosis(),
                            needs=[
                                "Should I retry, or is there a different device/room I should target?",
                            ],
                            say_hint="Explain the device did not change state and ask whether to retry or target a different device.",
                        )
                    else:
                        verified = True
                else:
                    verified = False

                spoken_action = service.replace("_", " ")
                if spoken_action == "turn on":
                    spoken_action = "turned on"
                elif spoken_action == "turn off":
                    spoken_action = "turned off"

                extras_txt = ", ".join(extras_txt_parts) if extras_txt_parts else ""
                already = bool(expected and state_before in expected and state_after in expected)

                return action_success(
                    facts={
                        "query": query,
                        "action": action or "auto",
                        "entity_id": entity_id,
                        "friendly_name": friendly,
                        "result": spoken_action,
                        "state_before": state_before,
                        "state_after": state_after,
                        "verified": verified,
                        "already": already,
                        "extras": extras_txt,
                    },
                    say_hint=(
                        "Confirm the Home Assistant action. Mention the device name. "
                        "If already is true, say it was already in the desired state. "
                        "If verified is false, mention you couldn't confirm the state."
                    ),
                    suggested_followups=["Want me to adjust anything else in Home Assistant?"],
                )

            except Exception as e:
                logger.error(f"[ha_switches] control error: {e}")
                return action_failure(
                    code="ha_switches_failed",
                    message=f"Error performing {service} on {entity_id}: {e}",
                    diagnosis=self._ha_diagnosis(),
                    needs=["Should I retry or target a different device?"],
                    say_hint="Explain the Home Assistant action failed and ask whether to retry.",
                )

        if intent_type == "get_state" or action == "get_state":
            candidates = self._prepare_routed_candidates(
                catalog=catalog,
                route=primary_route,
                mode="get_state",
                intent_type=intent_type,
                action=action,
                query=query,
                scope=scope,
                domain_hint=domain_hint,
                excluded=excluded,
                route_info=route_info,
            )

            entity_id = explicit_entity or await self._choose_entity_llm(
                query,
                intent,
                candidates,
                llm_client,
                route=primary_route,
            )
            if not entity_id:
                return "I couldn’t find a device or sensor matching that."

            try:
                st = client.get_state(entity_id)
                val = st.get("state", "unknown") if isinstance(st, dict) else str(st)
                attrs = (st.get("attributes") or {}) if isinstance(st, dict) else {}
                friendly = (attrs.get("friendly_name") or entity_id)
                unit = (attrs.get("unit_of_measurement") or "")

                if (entity_id or "").startswith("remote.") and isinstance(attrs, dict):
                    current_activity = attrs.get("current_activity") or attrs.get("activity")
                    if isinstance(current_activity, str) and current_activity.strip():
                        val = current_activity.strip()

                return await self._speak_response_state(query, friendly, str(val), str(unit), llm_client)
            except Exception as e:
                logger.error(f"[ha_switches] get_state error: {e}")
                return f"Error reading {entity_id}: {e}"

        return "I couldn't understand that request."

    def _route_key(self) -> str:
        route = str(getattr(self, "forced_route", "") or "").strip().lower()
        return route or "unknown"

    def _hint_key(self) -> str:
        hint = str(getattr(self, "forced_domain_hint", "") or "").strip().lower()
        if hint:
            return hint
        route = self._route_key()
        if route == "temperature":
            return "sensor"
        return route

    def _domain_guard(self, intent: dict):
        route = self._route_key()
        intent_type = str((intent or {}).get("intent") or "").strip().lower()
        action = str((intent or {}).get("action") or "").strip().lower()

        if route != "temperature" and intent_type == "get_temp":
            return action_failure(
                code="ha_domain_mismatch",
                message=f"{self.name} handles {route} requests. Use ha_temperature for ambient temperature checks.",
                diagnosis=self._ha_diagnosis(),
                needs=["Ask for a state/control request in this domain, or use ha_temperature."],
                say_hint="Explain that this tool is domain-specific and suggest the matching Home Assistant verba.",
            )

        if route != "climate" and (intent_type == "set_temperature" or action == "set_temperature"):
            return action_failure(
                code="ha_domain_mismatch",
                message=f"{self.name} handles {route} requests. Use ha_climate for thermostat setpoint changes.",
                diagnosis=self._ha_diagnosis(),
                needs=["Use ha_climate to set thermostat temperatures."],
                say_hint="Explain that thermostat setpoint changes belong to the climate verba.",
            )

        if route == "temperature" and (
            intent_type in {"control", "set_temperature"}
            or action in {"turn_on", "turn_off", "open", "close", "set_temperature", "send_command"}
        ):
            return action_failure(
                code="ha_temperature_read_only",
                message="ha_temperature is read-only for ambient temperature lookups.",
                diagnosis=self._ha_diagnosis(),
                needs=["Use a control-focused Home Assistant verba (for example ha_lights, ha_switches, or ha_climate)."],
                say_hint="Explain that this tool is read-only and suggest the correct control-focused verba.",
            )

        return None

    async def _handle(self, args, llm_client):
        args = args or {}
        query = str(args.get("query") or "").strip()
        if query and llm_client is not None:
            try:
                parsed = await self._base_interpret_query(query, llm_client)
                if isinstance(parsed, dict):
                    guarded = self._domain_guard(parsed)
                    if guarded:
                        return guarded
            except Exception:
                # Guardrails are best-effort and should not block normal handling.
                pass
        return await self._base_handle(args, llm_client)

    async def _route_query(self, query: str, intent, llm_client) -> dict:
        return {
            "route": self._route_key(),
            "search_text": self._clean_search_text(query) or str(query or "").strip(),
        }

    async def _interpret_query(self, query: str, llm_client) -> dict:
        data = await self._base_interpret_query(query, llm_client)
        if not isinstance(data, dict):
            data = {}

        route = self._route_key()
        hint = self._hint_key()

        if route == "temperature":
            data["intent"] = "get_temp"
            data["action"] = "get_state"
            data["domain_hint"] = "sensor"
            data["read_target"] = "state"
            return data

        if hint:
            data["domain_hint"] = hint

        return data


verba = HASwitchesPlugin()
