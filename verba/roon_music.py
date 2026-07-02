import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from tateros import integration_store as integration_store_module
from verba_base import ToolVerba
from verba_result import action_failure, action_success

logger = logging.getLogger("roon_music")
logger.setLevel(logging.INFO)

ZONE_MATCH_STOPWORDS = {
    "area",
    "assistant",
    "device",
    "media",
    "player",
    "room",
    "satellite",
    "speaker",
    "voice",
}


def _roon_module(*, required: bool = True):
    module = integration_store_module.integration_module("roon")
    if module is None and required:
        raise RuntimeError("Roon integration is not enabled.")
    return module


def _text(value: Any) -> str:
    return str(value or "").strip()


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _text(value).lower())


def _words(value: Any) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-z0-9]+", _text(value).lower())
        if token and token not in ZONE_MATCH_STOPWORDS
    }


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "on", "shuffle", "random"}


def _json_object_from_text(text: str) -> Dict[str, Any]:
    clean = _text(text)
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


class RoonMusicPlugin(ToolVerba):
    name = "roon_music"
    verba_name = "Roon Music"
    pretty_name = "Roon Music"
    version = "1.0.1"
    min_tater_version = "59"
    settings_category = "Roon Control"
    platforms = [
        "voice_core",
        "webui",
        "little_spud",
        "macos",
        "homeassistant",
        "homekit",
        "xbmc",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    tags = ["music", "audio", "media-player", "playback", "roon"]

    usage = '{"function":"roon_music","arguments":{"query":"play some jazz in the kitchen"}}'
    description = (
        "Play music through Roon on the requested room/zone or the voice device that made the request. "
        "Handles natural-language requests for songs, artists, albums, genres, playlists, radio, "
        "shuffle/random playback, and playback controls."
    )
    verba_dec = (
        "Play existing music through Roon in a room or zone: songs, artists, albums, genres, playlists, radio, "
        "shuffle/random playback, and playback controls. Do not use for generating new songs or audio."
    )
    when_to_use = (
        "Use for Roon requests to play or control existing music: songs, artists, albums, genres, playlists, "
        "radio, shuffle/random playback, pause, resume, stop, next, and previous."
    )
    how_to_use = (
        "Pass the user's natural-language music request in query. Include room or target when the user names one. "
        "If no room is named, the target playback device is resolved from the voice request context."
    )
    common_needs = ["A song, artist, album, genre, playlist, radio station, random music request, or playback control."]
    missing_info_prompts = ["What should I play or control?"]
    example_calls = [
        '{"function":"roon_music","arguments":{"query":"play some jazz in the kitchen"}}',
        '{"function":"roon_music","arguments":{"query":"play songs by David Bowie"}}',
        '{"function":"roon_music","arguments":{"query":"play Bohemian Rhapsody by Queen"}}',
    ]

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are starting the music now. "
        "Only output that message."
    )

    control_actions = {"pause", "resume", "play", "stop", "next", "previous"}
    media_kinds = {"track", "artist", "album", "genre", "playlist", "radio", "any"}
    genre_words = {
        "ambient",
        "bluegrass",
        "blues",
        "classical",
        "country",
        "dance",
        "disco",
        "electronic",
        "folk",
        "funk",
        "hiphop",
        "hip-hop",
        "jazz",
        "metal",
        "pop",
        "punk",
        "rap",
        "reggae",
        "rock",
        "soul",
        "techno",
    }

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
            text = _text(args)
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
        try:
            module = _roon_module(required=False)
            if module is None:
                return {"roon_integration": "missing"}
            status_fn = getattr(module, "integration_status", None)
            if callable(status_fn):
                status = status_fn()
                return {
                    "roon_integration": "installed",
                    "roon_enabled": "yes" if status.get("enabled") else "no",
                    "roon_configured": "yes" if status.get("configured") else "no",
                    "roon_status": _text(status.get("message")) or "unknown",
                }
        except Exception as exc:
            return {"roon_integration": f"error: {exc}"}
        return {"roon_integration": "unknown"}

    def _normalize_action(self, value: Any, query: str) -> str:
        explicit = _text(value).lower().replace("-", "_")
        aliases = {
            "start": "play_media",
            "listen": "play_media",
            "play_music": "play_media",
            "play_media": "play_media",
            "queue": "queue_media",
            "queue_music": "queue_media",
            "add": "queue_media",
            "add_to_queue": "queue_media",
            "resume": "play",
            "unpause": "play",
            "skip": "next",
            "skip_next": "next",
            "back": "previous",
            "prev": "previous",
        }
        if explicit in aliases:
            return aliases[explicit]
        if explicit == "play" and _text(query) and not re.search(r"\b(resume|unpause|continue)\b", _text(query).lower()):
            return "play_media"
        if explicit in self.control_actions:
            return explicit

        text = f" {_text(query).lower()} "
        if re.search(r"\b(pause|hold)\b", text):
            return "pause"
        if re.search(r"\b(stop|turn off the music|stop the music)\b", text):
            return "stop"
        if re.search(r"\b(next|skip|skip this)\b", text):
            return "next"
        if re.search(r"\b(previous|go back|last track|back one)\b", text):
            return "previous"
        if re.search(r"\b(resume|unpause|continue)\b", text):
            return "play"
        if re.search(r"\b(queue|add)\b", text):
            return "queue_media"
        return "play_media" if text.strip() else ""

    def _normalize_media_kind(self, value: Any, query: str) -> str:
        explicit = _text(value).lower().replace("-", "_").replace(" ", "_")
        aliases = {
            "song": "track",
            "songs": "track",
            "tracks": "track",
            "band": "artist",
            "artists": "artist",
            "albums": "album",
            "genres": "genre",
            "playlists": "playlist",
            "station": "radio",
            "stations": "radio",
            "internet_radio": "radio",
        }
        explicit = aliases.get(explicit, explicit)
        if explicit in self.media_kinds:
            return explicit

        text = _text(query).lower()
        if re.search(r"\b(song|track)\b", text):
            return "track"
        if re.search(r"\b(album|record)\b", text):
            return "album"
        if re.search(r"\b(playlist|mix)\b", text):
            return "playlist"
        if re.search(r"\b(radio|station)\b", text):
            return "radio"
        if re.search(r"\b(artist|band|songs by|music by|by the artist)\b", text):
            return "artist"
        clean_tokens = {_slug(token) for token in re.split(r"[^a-z0-9-]+", text) if token}
        if clean_tokens & {_slug(word) for word in self.genre_words}:
            return "genre"
        return "any"

    def _extract_query_target(self, query: str) -> Tuple[str, str]:
        text = _text(query)
        if not text:
            return "", ""
        match = re.search(
            r"\b(?:in|inside|to|on|through|via)\s+(?:the\s+)?"
            r"(?P<target>[a-z0-9][a-z0-9 '._-]*?)"
            r"(?:\s+(?:room|zone|speaker|speakers|stereo|system|audio))?\s*$",
            text,
            flags=re.I,
        )
        if not match:
            return text, ""
        return text[: match.start()].strip(" ,."), _text(match.group("target"))

    def _clean_media_query(self, query: str, *, action: str, media_kind: str) -> str:
        text = _text(query)
        if action in self.control_actions:
            return ""
        clean = re.sub(r"^\s*(please\s+)?(can you\s+|could you\s+|would you\s+)?", "", text, flags=re.I)
        clean = re.sub(r"^\s*(play|put on|listen to|start|shuffle|queue|add)\s+", "", clean, flags=re.I)
        clean = re.sub(r"\b(on|with)\s+(the\s+)?(speaker|device|music system|audio system)\b.*$", "", clean, flags=re.I)
        clean = re.sub(r"\b(random|shuffle|some|a little|music|songs|tracks)\b", " ", clean, flags=re.I)
        if media_kind in {"artist", "album", "genre", "playlist", "radio"}:
            clean = re.sub(rf"\b({media_kind}|{media_kind}s)\b", " ", clean, flags=re.I)
        clean = re.sub(r"^\s*(by|from|the)\s+", "", clean, flags=re.I)
        clean = re.sub(r"\s+", " ", clean).strip(" ,.")
        return clean

    async def _ai_plan(self, llm_client, query: str) -> Dict[str, Any]:
        if not llm_client or not _text(query):
            return {}
        prompt = (
            "Extract a voice music playback intent from the user request.\n"
            "Allowed actions: play_media, queue_media, pause, play, stop, next, previous.\n"
            "Allowed media_kind: track, artist, album, genre, playlist, radio, any.\n"
            "Rules:\n"
            "1) For a specific song, use media_kind track and keep the song plus artist in query if present.\n"
            "2) For an artist request without a specific song or album, use media_kind artist and random true.\n"
            "3) For a genre request, use media_kind genre and random true.\n"
            "4) Do not choose a room, speaker, or device; the voice context decides that.\n"
            "Respond only with JSON: "
            "{\"action\":\"...\",\"query\":\"...\",\"media_kind\":\"...\",\"random\":true|false}\n\n"
            f"User request: {query}"
        )
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            raw = ((resp or {}).get("message") or {}).get("content", "")
            data = _json_object_from_text(raw)
            if not isinstance(data, dict):
                return {}
            action = self._normalize_action(data.get("action"), query)
            media_kind = self._normalize_media_kind(data.get("media_kind"), query)
            if action == "play" and _text(data.get("query")):
                action = "play_media"
            plan = {
                "action": action,
                "query": _text(data.get("query")),
                "media_kind": media_kind,
            }
            if "random" in data:
                plan["random"] = _truthy(data.get("random"))
            return plan
        except Exception as exc:
            logger.warning("[%s] intent extraction failed: %s", self.name, exc)
            return {}

    def _candidate_context_values(self, payload: Dict[str, Any], context: Optional[Dict[str, Any]]) -> List[Tuple[str, int]]:
        containers: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            containers.append(payload)
            if isinstance(payload.get("origin"), dict):
                containers.append(payload["origin"])
        if isinstance(context, dict):
            containers.append(context)
            if isinstance(context.get("origin"), dict):
                containers.append(context["origin"])
            origin_from = context.get("origin", {}).get("from") if isinstance(context.get("origin"), dict) else None
            if isinstance(origin_from, dict):
                containers.append(origin_from)

        weighted_keys = {
            "roon_zone_id": 500,
            "roon_zone": 500,
            "zone_or_output_id": 500,
            "zone_id": 480,
            "zone": 460,
            "zone_name": 460,
            "target": 340,
            "room": 320,
            "area": 320,
            "room_name": 260,
            "area_name": 260,
            "device_name": 240,
            "satellite_name": 240,
            "satellite_selector": 220,
            "room_id": 180,
            "area_id": 180,
            "device_id": 160,
            "name": 120,
            "long_name": 120,
            "short_name": 100,
        }
        seen: set[str] = set()
        values: List[Tuple[str, int]] = []
        for container in containers:
            for key, weight in weighted_keys.items():
                value = _text(container.get(key))
                if not value:
                    continue
                marker = f"{key}:{value.lower()}"
                if marker in seen:
                    continue
                seen.add(marker)
                values.append((value, weight))
        return values

    def _zone_match_terms(self, device: Dict[str, Any]) -> List[str]:
        details = device.get("details") if isinstance(device.get("details"), dict) else {}
        terms = [
            device.get("id"),
            device.get("name"),
            device.get("ref"),
            details.get("zone_id"),
            details.get("alias"),
            details.get("roon_display_name"),
        ]
        outputs = details.get("output_names") if isinstance(details.get("output_names"), list) else []
        terms.extend(outputs)
        for output in details.get("outputs") if isinstance(details.get("outputs"), list) else []:
            if isinstance(output, dict):
                terms.extend([output.get("output_id"), output.get("display_name")])
        return [_text(term) for term in terms if _text(term)]

    def _score_zone(self, device: Dict[str, Any], candidate: str, weight: int) -> int:
        cand = _text(candidate)
        if not cand:
            return -1
        cand_low = cand.lower()
        cand_slug = _slug(cand)
        cand_words = _words(cand)
        best = -1
        for term in self._zone_match_terms(device):
            term_low = term.lower()
            term_slug = _slug(term)
            term_words = _words(term)
            score = 0
            if cand_low == term_low or cand_slug == term_slug:
                score = 500
            elif cand_low in term_low or (cand_slug and cand_slug in term_slug):
                score = 260
            elif cand_words and cand_words <= term_words:
                score = 220
            elif cand_words & term_words:
                score = 80 + 20 * len(cand_words & term_words)
            if score > best:
                best = score
        return best + weight if best > 0 else -1

    def _resolve_zone(
        self,
        devices: List[Dict[str, Any]],
        payload: Dict[str, Any],
        context: Optional[Dict[str, Any]],
    ) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        if not devices:
            return None, ["No music playback zones were found."]
        candidates = self._candidate_context_values(payload, context)
        scored: List[Tuple[int, Dict[str, Any], str]] = []
        for value, weight in candidates:
            for device in devices:
                score = self._score_zone(device, value, weight)
                if score > 0:
                    scored.append((score, device, value))
        if scored:
            scored.sort(key=lambda item: item[0], reverse=True)
            best_score, best_device, matched = scored[0]
            tied = [item for item in scored if item[0] == best_score and item[1].get("id") != best_device.get("id")]
            if not tied:
                logger.info("[%s] resolved voice context '%s' to zone %s", self.name, matched, best_device.get("name"))
                return best_device, []
        if len(devices) == 1:
            return devices[0], []
        names = ", ".join(_text(device.get("name")) for device in devices[:8] if _text(device.get("name")))
        return None, [
            "I could not match this voice device to a music zone. "
            "Add a zone alias matching the voice device or room name in Tater Settings > Integrations > Roon. "
            + (f"Available zones: {names}." if names else "")
        ]

    def _integration_devices(self) -> Tuple[List[Dict[str, Any]], str]:
        module = _roon_module()
        devices_fn = getattr(module, "integration_devices", None)
        if not callable(devices_fn):
            raise RuntimeError("Installed Roon integration does not expose devices.")
        result = devices_fn()
        devices = result.get("devices") if isinstance(result.get("devices"), list) else []
        return [device for device in devices if isinstance(device, dict)], _text(result.get("message"))

    def _integration_result_ok(self, result: Any) -> bool:
        return not (isinstance(result, dict) and result.get("ok") is False)

    def _selected_media_label(self, result: Any, fallback: str = "") -> str:
        row = result if isinstance(result, dict) else {}
        selected = row.get("selected") if isinstance(row.get("selected"), dict) else {}
        browse_result = row.get("browse_result") if isinstance(row.get("browse_result"), dict) else {}
        source_browse = row.get("source_browse") if isinstance(row.get("source_browse"), dict) else {}
        for item in (selected, browse_result, source_browse, row):
            title = _text(
                item.get("title")
                or item.get("name")
                or item.get("display_name")
                or item.get("label")
                or item.get("query")
            )
            subtitle = _text(item.get("subtitle") or item.get("artist") or item.get("description"))
            if title and subtitle and subtitle.lower() not in title.lower():
                return f"{title} - {subtitle}"
            if title:
                return title
        return _text(fallback)

    def _success_result_data(self, result: Any, *, playback_started: bool) -> Any:
        if not isinstance(result, dict):
            return result
        clean = dict(result)
        clean["playback_started"] = bool(playback_started)
        if playback_started:
            message = _text(clean.get("message"))
            if message and re.search(r"\b(could not|couldn't|failed|not found|no results|error)\b", message, flags=re.I):
                clean.pop("message", None)
        return clean

    async def _handle(self, args: Any, llm_client=None, context: Optional[Dict[str, Any]] = None):
        payload = self._normalize_handler_args(args)
        query = _text(payload.get("query") or payload.get("request") or payload.get("text") or payload.get("prompt"))
        query_without_target, query_target = self._extract_query_target(query)
        if query_target and not any(_text(payload.get(key)) for key in ("target", "zone", "zone_name", "room", "room_name", "area", "area_name")):
            payload["target"] = query_target
        if query_without_target:
            query = query_without_target
        action = self._normalize_action(payload.get("action"), query)
        media_kind = self._normalize_media_kind(payload.get("media_kind") or payload.get("media_type"), query)
        randomize = _truthy(payload.get("random") if "random" in payload else payload.get("shuffle"))
        if re.search(r"\b(random|shuffle)\b", query.lower()):
            randomize = True

        need_ai = action in {"", "play_media", "queue_media"} and query
        if need_ai:
            plan = await self._ai_plan(llm_client, query)
            action = plan.get("action") or action
            if plan.get("media_kind") and plan.get("media_kind") != "any":
                media_kind = plan["media_kind"]
            if plan.get("query"):
                query = plan["query"]
            if "random" in plan:
                randomize = bool(plan["random"])

        if not action:
            return action_failure(
                code="missing_music_request",
                message="No music playback request was provided.",
                needs=["Say what to play or which playback control to use."],
                say_hint="Ask what the user wants to play or control.",
            )

        if action in {"play_media", "queue_media"}:
            if media_kind in {"artist", "genre", "radio"}:
                randomize = True if "random" not in payload else randomize
            query = self._clean_media_query(query, action=action, media_kind=media_kind)
            if not query and not randomize:
                return action_failure(
                    code="missing_media_query",
                    message="I need a song, artist, album, genre, playlist, radio station, or random music request.",
                    needs=["Tell me what to play."],
                    say_hint="Ask what music the user wants to play.",
                )

        try:
            devices, inventory_message = self._integration_devices()
        except Exception as exc:
            return action_failure(
                code="roon_inventory_failed",
                message=f"Could not read music playback zones: {exc}",
                diagnosis=self._diagnosis(),
                needs=["Check Roon pairing and zone availability."],
                say_hint="Explain music playback zones are unavailable and suggest checking Roon pairing.",
            )

        zone, needs = self._resolve_zone(devices, payload, context)
        if not zone:
            return action_failure(
                code="zone_selection_failed",
                message="Could not select a music playback zone for this voice device.",
                needs=needs,
                data={"devices": devices, "inventory_message": inventory_message},
                say_hint="Ask to map this voice device or room to a music zone alias.",
            )

        action_fn = getattr(_roon_module(), "run_integration_device_action", None)
        if not callable(action_fn):
            return action_failure(
                code="roon_action_unavailable",
                message="Installed Roon integration does not expose playback actions.",
                diagnosis=self._diagnosis(),
                needs=["Update the Roon integration."],
                say_hint="Explain the Roon integration needs to be updated before playback can work.",
            )

        device_id = _text(zone.get("id"))
        try:
            if action in self.control_actions:
                integration_action = "play" if action == "play" else action
                result = action_fn(integration_action, device_id, {})
                if not self._integration_result_ok(result):
                    raise RuntimeError(_text(result.get("message")) or f"Roon did not complete {action}.")
                summary = f"Done ({'resume' if action == 'play' else action}) on {_text(zone.get('name')) or 'this device'}."
            else:
                result = action_fn(
                    action,
                    device_id,
                    {
                        "query": query,
                        "media_kind": media_kind,
                        "random": randomize,
                    },
                )
                if not self._integration_result_ok(result):
                    raise RuntimeError(_text(result.get("message")) or "Roon did not start playback.")
                label = self._selected_media_label(
                    result,
                    fallback=_text(result.get("query")) or ("random music" if randomize else "music"),
                )
                verb = "Queued" if action == "queue_media" else "Playing"
                summary = f"{verb} {label} on {_text(zone.get('name')) or 'this device'}."
        except Exception as exc:
            return action_failure(
                code="roon_playback_failed",
                message=f"Music playback failed: {exc}",
                diagnosis=self._diagnosis(),
                needs=["Check that Roon is paired, the zone is online, and the media exists in the library."],
                say_hint="Explain the music playback request failed and suggest trying another song, artist, album, or genre.",
            )

        return action_success(
            facts={
                "action": action,
                "query": query,
                "media_kind": media_kind,
                "random": randomize,
                "zone_id": zone.get("id"),
                "zone_name": zone.get("name"),
                "selected_media": self._selected_media_label(result, fallback=query),
                "playback_started": True,
            },
            data={"zone": zone, "result": self._success_result_data(result, playback_started=True)},
            summary_for_user=summary,
            say_hint="Confirm the music playback action briefly. If playback_started is true, do not say the song could not be played.",
        )

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        payload = self._normalize_handler_args(args)
        if not _text(payload.get("query")):
            payload_query = _text(unused_kwargs.get("query"))
            if payload_query:
                payload["query"] = payload_query
        return await self._handle(payload, llm_client=llm_client, context=context)


verba = RoonMusicPlugin()
