# verba/music_assistant.py
import json
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional

import requests
import aiohttp
from dotenv import load_dotenv

from verba_base import ToolVerba
from helpers import redis_client, get_tater_name
from verba_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from verba_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("music_assistant")
logger.setLevel(logging.INFO)


def _decode_redis_map(m: Dict[Any, Any]) -> Dict[str, str]:
    """redis hgetall often returns bytes; normalize to plain str."""
    out: Dict[str, str] = {}
    for k, v in (m or {}).items():
        if isinstance(k, (bytes, bytearray)):
            k = k.decode("utf-8", "ignore")
        else:
            k = str(k)

        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        else:
            v = "" if v is None else str(v)

        out[k] = v
    return out


class RoomPlayerNotFound(RuntimeError):
    """Raised when a requested room cannot be mapped to a media_player entity."""
    pass


class MusicAssistantPlugin(ToolVerba):
    name = "music_assistant"
    verba_name = "Music Assistant"
    version = "1.0.15"
    min_tater_version = "59"

    usage = '{"function":"music_assistant","arguments":{"query":"What the user wants to play (artist, album, track, playlist)."}}'

    description = (
        "Play music and control playback via Music Assistant in Home Assistant."
        "Supports play/queue/pause/resume/stop/next/previous/volume. "
        "Some devices provide room context in the system prompt, use it for room unless the user has specified a room"
        "If room is truly unknown, ask the user where to play."
    )

    when_to_use = "Use to control music playback with explicit action/query/room/volume inputs."
    common_needs = ["action", "query (for play/queue)", "room (optional)"]
    missing_info_prompts = [
        "What should I play or control, and which room should I use?",
    ]

    verba_dec = "Play music and control playback via Music Assistant in Home Assistant."
    pretty_name = "Controlling Music"
    settings_category = "Music Assistant"

    required_settings = {
        "MA_CONFIG_ENTRY_ID": {
            "label": "Music Assistant config_entry_id",
            "type": "string",
            "default": "",
        },
        "ROOM_MAP": {
            "label": "Room → Media Player Map (optional)",
            "type": "textarea",
            "default": "",
            "rows": 10,
            "description": (
                "Optional. One per line. Simplest format:\n"
                "Kitchen: media_player.sonos_kitchen\n"
                "Family Room: media_player.sonos_family_room\n\n"
                "Also accepts JSON formats for backwards compatibility."
            ),
            "placeholder": (
                "Kitchen: media_player.sonos_kitchen\n"
                "Family Room: media_player.sonos_family_room"
            ),
        },
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re controlling the music now. "
        "Only output that message."
    )

    platforms = ["webui", "macos", "homeassistant", "homekit", "xbmc", "discord", "telegram", "matrix", "irc"]

    # -------------------- HA helpers --------------------
    def _ha_settings(self) -> Dict[str, str]:
        raw = redis_client.hgetall("verba_settings:Music Assistant") or {}
        settings = _decode_redis_map(raw)

        # IMPORTANT: decode HA settings too (hgetall often returns bytes)
        ha_raw = redis_client.hgetall("homeassistant_settings") or {}
        ha_settings = _decode_redis_map(ha_raw)

        base_url = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (ha_settings.get("HA_TOKEN") or "").strip()
        entry_id = (settings.get("MA_CONFIG_ENTRY_ID") or "").strip()
        return {"base_url": base_url, "token": token, "entry_id": entry_id}

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _ha_call_service(
        self,
        domain: str,
        service: str,
        data: Dict[str, Any],
        return_response: bool = True,
        timeout: int = 20,
    ) -> Any:
        cfg = self._ha_settings()
        if not cfg["token"]:
            raise RuntimeError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        url = f'{cfg["base_url"]}/api/services/{domain}/{service}'
        if return_response:
            url += "?return_response"

        resp = requests.post(url, headers=self._ha_headers(cfg["token"]), json=data, timeout=timeout)
        try:
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"HA service call failed: {resp.status_code} {resp.text}") from e

        if not (resp.text or "").strip():
            return None
        try:
            return resp.json()
        except Exception:
            return resp.text

    async def _ha_states(self) -> List[Dict[str, Any]]:
        cfg = self._ha_settings()
        if not cfg["token"]:
            raise RuntimeError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
        url = f'{cfg["base_url"]}/api/states'
        headers = self._ha_headers(cfg["token"])

        def _get():
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            return r.json()

        return await asyncio.to_thread(_get)

    async def _ha_state(self, entity_id: str) -> Dict[str, Any]:
        cfg = self._ha_settings()
        if not cfg["token"]:
            raise RuntimeError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
        ent = str(entity_id or "").strip()
        if not ent:
            return {}
        url = f'{cfg["base_url"]}/api/states/{ent}'
        headers = self._ha_headers(cfg["token"])

        def _get():
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
            return r.json()

        try:
            return await asyncio.to_thread(_get)
        except Exception:
            return {}

    async def _ha_ws_call(self, msg_type: str) -> Any:
        """
        Call a Home Assistant WebSocket command using the SAME HA_TOKEN as REST.

        We do short-lived connect -> query -> disconnect (no subscriptions).
        """
        cfg = self._ha_settings()
        if not cfg["token"]:
            raise RuntimeError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        ws_url = f'{cfg["base_url"]}/api/websocket'
        timeout = aiohttp.ClientTimeout(total=20)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(ws_url, heartbeat=30) as ws:
                # 1) auth_required
                first = await ws.receive_json()
                if (first or {}).get("type") not in ("auth_required", "auth_ok"):
                    # Sometimes proxies / errors do weird things
                    logger.debug(f"[music_assistant] WS first msg: {first}")

                # 2) auth
                await ws.send_json({"type": "auth", "access_token": cfg["token"]})
                auth_ok = await ws.receive_json()
                if auth_ok.get("type") != "auth_ok":
                    raise RuntimeError(f"HA websocket auth failed: {auth_ok}")

                # 3) request
                await ws.send_json({"id": 1, "type": msg_type})
                resp = await ws.receive_json()

                if not resp.get("success", False):
                    raise RuntimeError(f"HA websocket call failed: {resp}")

                return resp.get("result")

    async def _media_players_for_room_area(self, room: str) -> List[Dict[str, str]]:
        """
        Try to reduce the candidate list by using HA Areas/Device/Entity registry via WebSocket.

        Returns a list of:
          [{"entity_id": "media_player.xyz", "friendly_name": "..."}]

        If the area can't be resolved or has no players assigned, returns [].
        """
        room_l = (room or "").strip().lower()
        if not room_l:
            return []

        try:
            areas = await self._ha_ws_call("config/area_registry/list")
            entities = await self._ha_ws_call("config/entity_registry/list")
            devices = await self._ha_ws_call("config/device_registry/list")
        except Exception as e:
            logger.warning(f"[music_assistant] area-scoped lookup failed (ws): {e}")
            return []

        # Find matching area_id by name
        area_id = None
        for a in (areas or []):
            if not isinstance(a, dict):
                continue
            if (a.get("name") or "").strip().lower() == room_l:
                area_id = a.get("area_id")
                break

        if not area_id:
            return []

        # Map device_id -> area_id
        device_area: Dict[str, str] = {}
        for d in (devices or []):
            if isinstance(d, dict) and d.get("id"):
                device_area[d["id"]] = d.get("area_id")

        in_area: set = set()

        for e in (entities or []):
            if not isinstance(e, dict):
                continue
            ent_id = (e.get("entity_id") or "").strip()
            if not ent_id.startswith("media_player."):
                continue

            ent_area = e.get("area_id")
            if ent_area == area_id:
                in_area.add(ent_id)
                continue

            dev_id = e.get("device_id")
            if dev_id and device_area.get(dev_id) == area_id:
                in_area.add(ent_id)

        if not in_area:
            return []

        # Enrich with friendly_name from /api/states (registry doesn't reliably include it)
        try:
            states = await self._ha_states()
        except Exception:
            states = []

        players: List[Dict[str, str]] = []
        for s in states:
            ent = s.get("entity_id") or ""
            if ent in in_area:
                players.append(
                    {
                        "entity_id": ent,
                        "friendly_name": ((s.get("attributes") or {}).get("friendly_name") or ent),
                    }
                )

        return players

    # -------------------- Room map parsing --------------------
    def _parse_room_map(self, raw: str) -> Dict[str, str]:
        """
        Accepts ROOM_MAP as:
          - Simple lines:
              Kitchen: media_player.sonos_kitchen
              Family Room: media_player.sonos_family_room

          - Legacy JSON-ish lines:
              "Kitchen": "media_player.sonos_kitchen"
              "Family Room": "media_player.sonos_family_room"

          - Full JSON object:
              { "Kitchen": "media_player.sonos_kitchen" }
        """
        raw = (raw or "").strip()
        if not raw:
            return {}

        # normalize smart quotes from iOS/macOS
        raw = (
            raw.replace("“", '"')
               .replace("”", '"')
               .replace("‘", "'")
               .replace("’", "'")
        )

        # 1) If it looks like a full JSON object, try it first
        if raw.lstrip().startswith("{") and raw.rstrip().endswith("}"):
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    out = {str(k).strip().lower(): str(v).strip() for k, v in data.items() if k and v}
                    logger.info(f"[music_assistant] ROOM_MAP parsed (json object) keys={list(out.keys())}")
                    return out
            except Exception as e:
                logger.error(f"[music_assistant] Invalid ROOM_MAP JSON object: {e}")

        # 2) Parse line-by-line (supports both `Kitchen: entity` and `"Kitchen": "entity"`)
        out: Dict[str, str] = {}
        jsonish_lines = []

        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # strip inline comments (simple)
            if " #" in line:
                line = line.split(" #", 1)[0].rstrip()

            # drop trailing commas
            if line.endswith(","):
                line = line[:-1].rstrip()

            if not line:
                continue

            # NEW simple format: Room: media_player.entity_id
            if ":" in line and not line.lstrip().startswith('"'):
                left, right = line.split(":", 1)
                room = left.strip()
                ent = right.strip()
                if room and ent:
                    out[room.lower()] = ent
                continue

            # Otherwise, treat it as legacy JSON-ish pair line: "Kitchen": "media_player.x"
            jsonish_lines.append(line)

        if out:
            logger.info(f"[music_assistant] ROOM_MAP parsed (simple) keys={list(out.keys())}")
            return out

        # 3) Legacy JSON-ish lines fallback (wrap into { ... })
        if not jsonish_lines:
            return {}

        text = "{\n" + ",\n".join(jsonish_lines) + "\n}"
        try:
            data = json.loads(text)
            if not isinstance(data, dict):
                return {}
            out = {str(k).strip().lower(): str(v).strip() for k, v in data.items() if k and v}
            logger.info(f"[music_assistant] ROOM_MAP parsed (legacy lines) keys={list(out.keys())}")
            return out
        except Exception as e:
            logger.error(f"[music_assistant] Invalid ROOM_MAP format: {e}")
            logger.error(f"[music_assistant] ROOM_MAP raw was:\n{raw}")
            return {}

    # -------------------- text helpers --------------------
    def _siri_flatten(self, text: Optional[str]) -> str:
        if not text:
            return "Okay."
        out = str(text)
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out[:450]

    @staticmethod
    def _as_item_list(raw: Any) -> List[Dict[str, Any]]:
        if isinstance(raw, list):
            out: List[Dict[str, Any]] = []
            for row in raw:
                if not isinstance(row, dict):
                    continue
                # Some MA responses wrap real item data under "item".
                wrapped = row.get("item")
                if isinstance(wrapped, dict):
                    merged = dict(wrapped)
                    if not merged.get("media_type") and row.get("media_type"):
                        merged["media_type"] = row.get("media_type")
                    if not merged.get("uri") and row.get("uri"):
                        merged["uri"] = row.get("uri")
                    out.append(merged)
                else:
                    out.append(row)
            return out
        if isinstance(raw, dict):
            return MusicAssistantPlugin._as_item_list(raw.get("items"))
        return []

    def _bucket_items(self, payload: Dict[str, Any], bucket: str) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []

        singular = bucket[:-1] if bucket.endswith("s") else bucket

        direct = self._as_item_list(payload.get(bucket))
        if direct:
            return direct
        direct = self._as_item_list(payload.get(singular))
        if direct:
            return direct

        # Some payloads have grouped data under "result"/"results".
        for parent in ("result", "results", "response", "search_results"):
            parent_val = payload.get(parent)
            if isinstance(parent_val, dict):
                grouped = self._as_item_list(parent_val.get(bucket))
                if grouped:
                    return grouped
                grouped = self._as_item_list(parent_val.get(singular))
                if grouped:
                    return grouped

        # Flat "items" payload: filter by media_type.
        flat = self._as_item_list(payload.get("items"))
        if flat:
            wanted = singular.lower()
            out = []
            for it in flat:
                mt = str(it.get("media_type") or it.get("type") or "").strip().lower()
                if mt == wanted:
                    out.append(it)
            if out:
                return out
        return []

    def _search_payload_has_results(self, payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False
        for key in ("artists", "albums", "tracks", "playlists", "radio"):
            if self._bucket_items(payload, key):
                return True
        return False

    @staticmethod
    def _ctx_text(context: Optional[Dict[str, Any]], *keys: str) -> str:
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

    async def _llm_build_search_queries(
        self,
        request_text: str,
        query_text: str,
        room: Optional[str],
        prefer: Optional[str],
        llm_client,
    ) -> Dict[str, Any]:
        """
        LLM-first query normalization:
        produce ordered search queries with no regex fallback path.
        """
        first, last = get_tater_name()
        sys = (
            f"You are {first} {last}. Rewrite the user's music request into Music Assistant search queries.\n"
            "Output ONLY valid JSON.\n"
            "Schema:\n"
            '{"queries":[string], "prefer": null|"artist"|"album"|"track"|"playlist"|"radio"}\n'
            "Rules:\n"
            "- Remove command words and filler words.\n"
            "- Remove room/location phrases.\n"
            "- Keep artist/song/album names intact.\n"
            "- Return 1 to 4 queries, best to broadest.\n"
            "- If artist-only request, first query should be just the artist name.\n"
        )
        user = json.dumps(
            {
                "request_text": request_text,
                "query_text": query_text,
                "room_hint": room,
                "prefer_hint": prefer,
            },
            ensure_ascii=False,
        )
        resp = await llm_client.chat(messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}])
        content = (resp.get("message") or {}).get("content", "").strip()

        try:
            data = json.loads(content)
        except Exception as e:
            raise RuntimeError("I couldn't interpret that music request clearly. Please try again.") from e

        if not isinstance(data, dict):
            raise RuntimeError("I couldn't interpret that music request clearly. Please try again.")

        raw_queries = data.get("queries")
        if not isinstance(raw_queries, list):
            raise RuntimeError("I couldn't build a valid music search query. Please try again.")

        queries: List[str] = []
        seen = set()
        for q in raw_queries[:8]:
            q2 = " ".join(str(q or "").split()).strip()
            if len(q2) < 2:
                continue
            low = q2.lower()
            if low in seen:
                continue
            seen.add(low)
            queries.append(q2)

        if not queries:
            raise RuntimeError("I couldn't build a valid music search query. Please try again.")

        allowed = {"artist", "album", "track", "playlist", "radio"}
        prefer_out = str(data.get("prefer") or "").strip().lower()
        if prefer_out not in allowed:
            prefer_out = str(prefer or "").strip().lower()
            if prefer_out not in allowed:
                prefer_out = None

        return {"queries": queries, "prefer": prefer_out}

    # -------------------- LLM planning / choosing --------------------
    async def _llm_plan(self, request_text: str, room: Optional[str], llm_client) -> Dict[str, Any]:
        """
        Convert user request into plan JSON:
        action: play|queue|pause|resume|stop|next|previous|volume
        query: search string for play/queue
        prefer: artist|album|playlist|track|radio (optional)
        volume: 0-100
        random: true|false (optional)
        room: optional room name
        """
        first, last = get_tater_name()
        sys = (
            f"You are {first} {last}. Convert the user's music request into JSON.\n"
            "Output ONLY valid JSON.\n"
            "Allowed actions: play, queue, pause, resume, stop, next, previous, volume.\n"
            "If playing/queueing, include 'query'.\n"
            "If user asks for random/shuffle/something different each time, set random=true.\n"
            "If user says a genre (reggae, lo-fi, jazz), prefer='playlist' or 'radio'.\n"
            "If user says an artist name, prefer='artist'. If they include a song title, prefer='track'.\n"
            "Schema:\n"
            '{"action":"play|queue|pause|resume|stop|next|previous|volume",'
            '"query":null|string,"prefer":null|string,"volume":null|int,'
            '"random":null|bool,"room":null|string}\n'
        )
        user = f"User request: {request_text}\nRoom hint: {room or ''}".strip()
        resp = await llm_client.chat(messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}])
        content = (resp.get("message") or {}).get("content", "").strip()

        try:
            plan = json.loads(content)
        except Exception:
            plan = {}

        if not isinstance(plan, dict):
            plan = {}

        if not plan.get("action"):
            plan["action"] = "play"
        if plan.get("query") is None:
            plan["query"] = request_text
        if plan.get("room") is None:
            plan["room"] = room

        return plan

    def _condense_search_for_llm(self, payload: Dict[str, Any], max_each: int = 8) -> Dict[str, Any]:
        """
        Keep tokens under control: send the LLM a small, stable view of results.
        """
        def take(items: List[Dict[str, Any]], fallback_media_type: str) -> List[Dict[str, Any]]:
            out = []
            for it in (items or [])[:max_each]:
                if not isinstance(it, dict):
                    continue
                artists: List[str] = []
                raw_artists = it.get("artists")
                if isinstance(raw_artists, list):
                    for a in raw_artists[:2]:
                        if isinstance(a, dict) and a.get("name"):
                            artists.append(str(a.get("name")))
                        elif isinstance(a, str) and a.strip():
                            artists.append(a.strip())
                elif isinstance(raw_artists, dict) and raw_artists.get("name"):
                    artists.append(str(raw_artists.get("name")))
                elif isinstance(raw_artists, str) and raw_artists.strip():
                    artists.append(raw_artists.strip())

                artist_name = str(it.get("artist") or "").strip()
                if artist_name and artist_name not in artists:
                    artists.append(artist_name)

                out.append(
                    {
                        "name": it.get("name"),
                        "uri": it.get("uri"),
                        "media_type": it.get("media_type") or fallback_media_type,
                        "artists": artists,
                    }
                )
            return out

        return {
            "artists": take(self._bucket_items(payload, "artists"), "artist"),
            "albums": take(self._bucket_items(payload, "albums"), "album"),
            "tracks": take(self._bucket_items(payload, "tracks"), "track"),
            "playlists": take(self._bucket_items(payload, "playlists"), "playlist"),
            "radio": take(self._bucket_items(payload, "radio"), "radio"),
        }

    async def _llm_choose_item(
        self,
        request_text: str,
        cleaned_query: str,
        prefer: Optional[str],
        search_payload: Dict[str, Any],
        llm_client,
    ) -> Optional[Dict[str, Any]]:
        """
        Ask LLM to choose the best playable item from the search results.

        Output schema:
          {"uri": "...", "media_type": "track|album|artist|playlist|radio", "name": "..."}
        """
        condensed = self._condense_search_for_llm(search_payload, max_each=10)
        first, last = get_tater_name()
        sys = (
            f"You are {first} {last}. Pick the best item to play from Music Assistant search results.\n"
            "Rules:\n"
            "- Only output valid JSON.\n"
            "- Prefer an exact track match if the user gave artist+song.\n"
            "- Prefer an album match if the user gave artist+album.\n"
            "- If user gave only an artist, choose the artist item if present; otherwise choose a track by that artist.\n"
            "- If user asked for a genre/vibe, prefer playlist or radio.\n"
            "- Choose from the provided results only.\n"
            'Schema: {"uri":string,"media_type":"track|album|artist|playlist|radio","name":string}\n'
        )
        user = (
            "User request:\n"
            f"- raw: {request_text}\n"
            f"- cleaned_query: {cleaned_query}\n"
            f"- prefer: {prefer or ''}\n\n"
            "Search results (condensed):\n"
            f"{json.dumps(condensed, ensure_ascii=False)}"
        )
        resp = await llm_client.chat(messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}])
        content = (resp.get("message") or {}).get("content", "").strip()

        try:
            j = json.loads(content)
        except Exception:
            return None

        if not isinstance(j, dict):
            return None
        uri = (j.get("uri") or "").strip()
        mtype = (j.get("media_type") or "").strip()
        name = (j.get("name") or "").strip()
        if not uri or not mtype:
            return None
        return {"uri": uri, "media_type": mtype, "name": name or cleaned_query}

    # -------------------- Music Assistant wrappers --------------------
    async def _ma_search(self, name: str, limit: int = 25, library_only: bool = False) -> Dict[str, Any]:
        cfg = self._ha_settings()
        if not cfg["entry_id"]:
            raise RuntimeError("Music Assistant config_entry_id is missing. Set MA_CONFIG_ENTRY_ID in plugin settings.")

        data: Dict[str, Any] = {
            "name": name,
            "limit": int(limit),
            "library_only": bool(library_only),
            "config_entry_id": cfg["entry_id"],
        }

        result = await asyncio.to_thread(self._ha_call_service, "music_assistant", "search", data, True, 25)

        # Normalize HA wrappers: [{"response": {...}}] or [{"result": {...}}]
        if isinstance(result, list) and result:
            first = result[0]
            if isinstance(first, dict):
                if isinstance(first.get("response"), dict):
                    return first["response"]
                if isinstance(first.get("result"), dict):
                    return first["result"]
                return first
        if isinstance(result, dict):
            if isinstance(result.get("response"), dict):
                return result["response"]
            if isinstance(result.get("result"), dict):
                return result["result"]
            return result
        return {}

    async def _ma_get_random_track_uri(self, search_text: str) -> Optional[str]:
        """
        Use music_assistant.get_library with order_by=random (per MA docs).
        """
        cfg = self._ha_settings()
        if not cfg["entry_id"]:
            raise RuntimeError("Music Assistant config_entry_id is missing. Set MA_CONFIG_ENTRY_ID in plugin settings.")

        data: Dict[str, Any] = {
            "media_type": "track",
            "search": search_text,
            "limit": 1,
            "order_by": "random",
            "config_entry_id": cfg["entry_id"],
        }
        result = await asyncio.to_thread(self._ha_call_service, "music_assistant", "get_library", data, True, 25)

        payload = None
        if isinstance(result, list) and result and isinstance(result[0], dict):
            payload = result[0].get("response") or result[0].get("result") or result[0]
        elif isinstance(result, dict):
            payload = result.get("response") or result.get("result") or result

        items = (payload or {}).get("items") or []
        if items and isinstance(items[0], dict):
            return items[0].get("uri")
        return None

    async def _ma_play_uri(
        self,
        media_player: str,
        uri: str,
        media_type: str,
        enqueue: str = "play",
        radio_mode: bool = False,
    ) -> None:
        """
        Use music_assistant.play_media action with explicit uri/type.
        """
        data = {
            "entity_id": media_player,
            "media_id": uri,
            "media_type": media_type,
            "enqueue": enqueue,  # play | add
            "radio_mode": bool(radio_mode),
        }
        await asyncio.to_thread(self._ha_call_service, "music_assistant", "play_media", data, False, 25)

    async def _speaker_label(self, media_player: str) -> str:
        st = await self._ha_state(media_player)
        attrs = st.get("attributes") if isinstance(st, dict) else {}
        friendly = (attrs or {}).get("friendly_name") if isinstance(attrs, dict) else ""
        label = str(friendly or "").strip()
        return label or str(media_player or "").strip()

    async def _verify_playback_started(self, media_player: str, timeout_s: float = 6.0) -> bool:
        """
        Best-effort playback verification after issuing a play command.
        """
        checks = max(1, int(timeout_s / 0.75))
        for _ in range(checks):
            st = await self._ha_state(media_player)
            state = str((st or {}).get("state") or "").strip().lower()
            attrs = (st or {}).get("attributes") if isinstance(st, dict) else {}
            attrs = attrs if isinstance(attrs, dict) else {}
            has_media_meta = bool(
                str(attrs.get("media_title") or "").strip()
                or str(attrs.get("media_artist") or "").strip()
                or str(attrs.get("media_content_id") or "").strip()
            )
            if state in {"playing", "buffering"}:
                return True
            if state == "paused" and has_media_meta:
                return True
            await asyncio.sleep(0.75)
        return False

    async def _player_control(self, media_player: str, action: str, volume: Optional[int] = None) -> None:
        if action == "volume":
            if volume is None:
                raise RuntimeError("Volume action requested but no volume provided.")
            vol = max(0, min(100, int(volume))) / 100.0
            data = {"entity_id": media_player, "volume_level": vol}
            await asyncio.to_thread(self._ha_call_service, "media_player", "volume_set", data, False, 15)
            return

        svc_map = {
            "pause": ("media_player", "media_pause"),
            "resume": ("media_player", "media_play"),
            "stop": ("media_player", "media_stop"),
            "next": ("media_player", "media_next_track"),
            "previous": ("media_player", "media_previous_track"),
        }
        if action not in svc_map:
            raise RuntimeError(f"Unsupported action: {action}")

        domain, service = svc_map[action]
        await asyncio.to_thread(self._ha_call_service, domain, service, {"entity_id": media_player}, False, 15)

    async def _media_players_for_device_id(self, device_id: str) -> List[Dict[str, str]]:
        """
        Return media_player entities that belong to the specified HA device_id.
        """
        did = (device_id or "").strip()
        if not did:
            return []

        try:
            entities = await self._ha_ws_call("config/entity_registry/list")
        except Exception as e:
            logger.warning(f"[music_assistant] device-scoped lookup failed (ws): {e}")
            return []

        ids: List[str] = []
        for row in (entities or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("device_id") or "").strip() != did:
                continue
            if row.get("disabled_by") not in (None, ""):
                continue
            ent = str(row.get("entity_id") or "").strip()
            if ent.startswith("media_player."):
                ids.append(ent)

        if not ids:
            return []

        try:
            states = await self._ha_states()
        except Exception:
            states = []

        state_map = {}
        for s in (states or []):
            eid = str((s or {}).get("entity_id") or "").strip()
            if eid:
                state_map[eid] = s or {}

        out: List[Dict[str, str]] = []
        for eid in sorted(set(ids)):
            st = state_map.get(eid) or {}
            out.append(
                {
                    "entity_id": eid,
                    "friendly_name": ((st.get("attributes") or {}).get("friendly_name") or eid),
                }
            )
        return out

    def _pick_best_device_player(
        self,
        players: List[Dict[str, str]],
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        if not players:
            return None
        if len(players) == 1:
            return players[0].get("entity_id")

        device_name = self._ctx_text(context, "device_name", "device")
        device_slug = self._slugify(device_name)

        best_id = None
        best_score = -999
        for p in players:
            eid = str(p.get("entity_id") or "").strip()
            friendly = str(p.get("friendly_name") or "").strip().lower()
            low = eid.lower()
            score = 0
            if device_slug and device_slug in low:
                score += 20
            if device_name and device_name.lower() in friendly:
                score += 16
            if low.endswith("_media_player"):
                score += 8
            if low.endswith("_speaker"):
                score += 5
            if score > best_score:
                best_score = score
                best_id = eid

        return best_id or players[0].get("entity_id")

    # -------------------- Room -> media_player resolution --------------------
    async def _resolve_media_player(
        self,
        room: Optional[str],
        args: Dict[str, Any],
        llm_client,
        *,
        context: Optional[Dict[str, Any]] = None,
        prefer_context_device: bool = False,
    ) -> str:
        explicit = (args or {}).get("media_player")
        if explicit:
            return explicit

        # optional ROOM_MAP override
        if room:
            room_map: Dict[str, str] = {}
            try:
                raw = redis_client.hgetall("verba_settings:Music Assistant") or {}
                settings = _decode_redis_map(raw)
                room_map = self._parse_room_map(settings.get("ROOM_MAP", ""))
            except Exception:
                room_map = {}
            mapped = room_map.get(room.strip().lower())
            if mapped:
                logger.info(f"[music_assistant] ROOM_MAP override: room='{room}' -> {mapped}")
                return mapped

        # Home Assistant default: when room is NOT specified, target the same speaking device.
        if prefer_context_device and not room:
            device_id = self._ctx_text(context, "device_id")
            if not device_id:
                raise RoomPlayerNotFound(
                    "I couldn't determine which speaker you are talking to. "
                    "Please specify a room, or pass a media_player entity."
                )
            players = await self._media_players_for_device_id(device_id)
            chosen = self._pick_best_device_player(players, context=context)
            if chosen:
                logger.info(f"[music_assistant] same-device resolve: device_id={device_id} -> {chosen}")
                return chosen
            raise RoomPlayerNotFound(
                "I couldn't find a media_player linked to this device. "
                "Please specify a room, or pass a media_player entity."
            )

        # NEW: If we have a room, try to reduce candidates to only players in that HA Area
        players: List[Dict[str, str]] = []
        if room:
            players = await self._media_players_for_room_area(room)

        # Fallback: old behavior (all media_players)
        if not players:
            states = await self._ha_states()
            for s in states:
                ent = s.get("entity_id") or ""
                if ent.startswith("media_player."):
                    players.append(
                        {
                            "entity_id": ent,
                            "friendly_name": ((s.get("attributes") or {}).get("friendly_name") or ent),
                        }
                    )

        if not players:
            raise RoomPlayerNotFound("No media_player entities found in Home Assistant.")

        if not room:
            raise RoomPlayerNotFound("Which room should I play it in? (e.g., Kitchen, Family Room)")

        # Ask LLM to pick best match (now from a smaller list when area lookup succeeds)
        first, last = get_tater_name()
        sys = (
            f"You are {first} {last}. Pick the best media_player entity for the requested room.\n"
            "Only output JSON: {\"entity_id\":\"media_player.xyz\"}\n"
        )
        listing = "\n".join([f'- {p["entity_id"]} ({p["friendly_name"]})' for p in players])
        user = f"Room: {room}\nAvailable players:\n{listing}"
        resp = await llm_client.chat(messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}])
        content = (resp.get("message") or {}).get("content", "").strip()

        try:
            j = json.loads(content)
            ent = j.get("entity_id")
            if ent and any(p["entity_id"] == ent for p in players):
                return ent
        except Exception:
            pass

        # fallback: contains match on friendly name
        room_l = room.lower()
        for p in players:
            if room_l in (p.get("friendly_name") or "").lower():
                return p["entity_id"]

        choices = ", ".join([p["friendly_name"] for p in players[:10]])
        raise RoomPlayerNotFound(f"I couldn’t find a player for '{room}'. Available players include: {choices}")

    # -------------------- Main runner --------------------
    async def _run(
        self,
        args: Dict[str, Any],
        llm_client,
        *,
        context: Optional[Dict[str, Any]] = None,
        prefer_context_device: bool = False,
    ) -> str:
        args = args or {}
        request_text = (args.get("request") or "").strip()
        action = (args.get("action") or "").strip().lower()
        query = (args.get("query") or "").strip()
        room = (args.get("room") or "").strip() or None
        prefer = args.get("prefer")
        vol = args.get("volume")
        want_random = args.get("random")

        if isinstance(want_random, str):
            want_random = want_random.strip().lower() in ("1", "true", "yes", "on")
        elif want_random is None:
            want_random = False

        explicit_args = any(k in args for k in ("action", "query", "room", "volume", "prefer", "random"))

        plan = None
        need_plan = (
            (not action)
            or (action in {"play", "queue"} and not query)
            or (action == "volume" and vol is None)
        )
        if need_plan and request_text:
            plan = await self._llm_plan(request_text, room, llm_client)

        if plan:
            if not action:
                action = (plan.get("action") or "").strip().lower()
            if not query:
                query = (plan.get("query") or "").strip()
            if prefer is None and plan.get("prefer") is not None:
                prefer = plan.get("prefer")
            if vol is None and plan.get("volume") is not None:
                vol = plan.get("volume")
            if args.get("random") is None and plan.get("random") is not None:
                want_random = bool(plan.get("random"))
            if not room:
                room = (plan.get("room") or room)

        if not action and query:
            action = "play"

        if not action:
            if explicit_args:
                return "Missing action. Specify play, queue, pause, resume, stop, next, previous, or volume."
            return "No request provided."

        if action in {"play", "queue"} and not query:
            if request_text:
                query = request_text
            else:
                return "What should I play or queue?"

        if action == "volume" and vol is None:
            return "What volume should I set (0-100)?"

        plan_room = room

        try:
            media_player = await self._resolve_media_player(
                plan_room,
                args,
                llm_client,
                context=context,
                prefer_context_device=prefer_context_device,
            )
        except RoomPlayerNotFound as e:
            return str(e)

        where_label = plan_room or await self._speaker_label(media_player) or media_player

        # Controls
        if action in {"pause", "resume", "stop", "next", "previous", "volume"}:
            await self._player_control(media_player, action, vol)
            if action == "volume":
                return f"Set volume to {max(0, min(100, int(vol or 0)))} on {where_label}."
            return f"Done ({action}) on {where_label}."

        if action not in {"play", "queue"}:
            return "Sorry — I’m not sure what to do with that request."

        if not request_text:
            request_text = query or action

        # LLM-first query rewriting (no regex fallback matching path).
        try:
            rewrite = await self._llm_build_search_queries(
                request_text=request_text,
                query_text=query,
                room=plan_room,
                prefer=prefer,
                llm_client=llm_client,
            )
        except Exception as e:
            return self._siri_flatten(str(e))

        search_queries = rewrite.get("queries") or []
        prefer = rewrite.get("prefer")
        cleaned = str(search_queries[0] if search_queries else (query or request_text)).strip()

        # If random requested: try MA random library pick first
        if want_random:
            uri = await self._ma_get_random_track_uri(cleaned)
            if uri:
                await self._ma_play_uri(
                    media_player,
                    uri,
                    "track",
                    enqueue=("add" if action == "queue" else "play"),
                    radio_mode=True,
                )
                if action == "play":
                    verified = await self._verify_playback_started(media_player)
                    if not verified:
                        return f"I sent a random track based on '{cleaned}' to {where_label}, but couldn't confirm playback yet."
                if action == "queue":
                    return f"Queued something random based on '{cleaned}' on {where_label}."
                return f"Playing something random based on '{cleaned}' on {where_label}."

        # Search with LLM-produced query variants before giving up.
        search_payload: Dict[str, Any] = {}
        for q in search_queries:
            search_payload = await self._ma_search(name=q, limit=25, library_only=False)
            if self._search_payload_has_results(search_payload):
                if q != cleaned:
                    logger.info(f"[music_assistant] search variant matched: '{cleaned}' -> '{q}'")
                cleaned = q
                break

        # Let the LLM choose the correct item from the results
        chosen = await self._llm_choose_item(request_text, cleaned, prefer, search_payload, llm_client)
        if not chosen:
            return (
                f"I found results for '{cleaned}', but couldn't confidently pick a single match. "
                "Try saying artist + song, artist + album, or a playlist name."
            )

        # If user likely asked for ONLY an artist and LLM chose artist, start with a random track by that artist
        if chosen and chosen.get("media_type") == "artist":
            seed = chosen.get("name") or cleaned
            uri = await self._ma_get_random_track_uri(seed)
            if uri:
                await self._ma_play_uri(
                    media_player,
                    uri,
                    "track",
                    enqueue=("add" if action == "queue" else "play"),
                    radio_mode=True,
                )
                if action == "play":
                    verified = await self._verify_playback_started(media_player)
                    if not verified:
                        return f"I sent a random {seed} track to {where_label}, but couldn't confirm playback yet."
                if action == "queue":
                    return f"Queued a random {seed} track on {where_label}."
                return f"Playing a random {seed} track on {where_label}."

            enqueue = "add" if action == "queue" else "play"
            await self._ma_play_uri(media_player, chosen["uri"], "artist", enqueue=enqueue, radio_mode=True)
            if action == "play":
                verified = await self._verify_playback_started(media_player)
                if not verified:
                    return f"I sent {chosen.get('name') or cleaned} (artist) to {where_label}, but couldn't confirm playback yet."
            if action == "queue":
                return f"Queued {chosen.get('name') or cleaned} (artist) on {where_label}."
            return f"Playing {chosen.get('name') or cleaned} (artist) on {where_label}."

        if chosen and chosen.get("uri") and chosen.get("media_type"):
            uri = chosen["uri"]
            mtype = chosen["media_type"]
            title = chosen.get("name") or cleaned
            logger.info(f"[music_assistant] play resolve: media_player={media_player} item={title} type={mtype} uri={uri}")
            enqueue = "add" if action == "queue" else "play"
            radio_mode = True if mtype in {"artist", "track", "radio"} else False
            await self._ma_play_uri(media_player, uri, mtype, enqueue=enqueue, radio_mode=radio_mode)
            if action == "queue":
                return f"Queued {title} ({mtype}) on {where_label}."
            verified = await self._verify_playback_started(media_player)
            if not verified:
                return f"I sent {title} ({mtype}) to {where_label}, but couldn't confirm playback yet."
            return f"Playing {title} ({mtype}) on {where_label}."

        return f"I searched Music Assistant for '{cleaned}' but didn’t find anything playable. Want to try a different search?"

    def _diagnosis(self) -> Dict[str, str]:
        music_diag = diagnose_hash_fields(
            "verba_settings:Music Assistant",
            fields={"ma_config_entry_id": "MA_CONFIG_ENTRY_ID"},
            validators={"ma_config_entry_id": lambda v: len(v.strip()) >= 3},
        )
        ha_diag = diagnose_hash_fields(
            "homeassistant_settings",
            fields={"ha_base_url": "HA_BASE_URL", "ha_token": "HA_TOKEN"},
            validators={
                "ha_base_url": lambda v: v.startswith("http://") or v.startswith("https://"),
                "ha_token": lambda v: len(v.strip()) >= 10,
            },
        )
        ha_key_diag = diagnose_redis_keys(
            keys={"ha_base_url": "tater:homeassistant:base_url", "ha_token": "tater:homeassistant:token"},
            validators={
                "ha_base_url": lambda v: v.startswith("http://") or v.startswith("https://"),
                "ha_token": lambda v: len(v.strip()) >= 10,
            },
        )
        return combine_diagnosis(music_diag, ha_diag, ha_key_diag)

    def _to_contract(self, raw: str, args: Dict[str, Any]) -> Dict[str, Any]:
        msg = (raw or "").strip()
        low = msg.lower()
        request = (args.get("request") or "").strip()
        action = (args.get("action") or "").strip().lower()
        query = (args.get("query") or "").strip()
        room = (args.get("room") or "").strip()

        if not msg:
            return action_failure(
                code="empty_result",
                message="Music Assistant did not return a response.",
                diagnosis=self._diagnosis(),
                needs=["What should I play or control?"],
                say_hint="Explain that no result was returned and ask for the intended playback action.",
            )

        if "token is not set" in low or "service call failed" in low or "websocket auth failed" in low:
            diagnosis = self._diagnosis()
            needs = needs_from_diagnosis(
                diagnosis,
                {
                    "ha_base_url": "Please set your Home Assistant base URL in settings.",
                    "ha_token": "Please set your Home Assistant long-lived access token in settings.",
                    "ma_config_entry_id": "Please set your Music Assistant config_entry_id in settings.",
                },
            )
            return action_failure(
                code="music_assistant_config_missing",
                message="Music Assistant configuration is incomplete.",
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain setup is incomplete and ask for the missing configuration values.",
            )

        is_failure = (
            low.startswith("no request provided")
            or "missing action" in low
            or "what should i play" in low
            or "which room should i play it in" in low
            or "couldn't determine which speaker you are talking to" in low
            or "couldn't find a media_player linked to this device" in low
            or "couldn’t find a player" in low
            or "couldn't find a player" in low
            or "i searched music assistant" in low
            or "not sure what to do" in low
            or low.startswith("error")
        )
        if is_failure:
            needs = []
            if "room" in low:
                needs.append("Which room should I use? (for example: Kitchen)")
            if "request" in low or "missing action" in low or "what should i play" in low:
                needs.append("What should I play or control?")
            if "didn’t find" in low or "didn't find" in low:
                needs.append("Try a different artist, song, album, or playlist name.")
            return action_failure(
                code="music_assistant_request_failed",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=needs,
                say_hint="Explain why the request failed and ask only for missing playback details.",
            )

        return action_success(
            facts={
                "action": action or "auto",
                "query": query or request,
                "room": room or "auto",
                "result": msg,
            },
            summary_for_user=msg[:300],
            say_hint="Confirm what happened in Music Assistant using only these facts.",
            suggested_followups=["Want me to queue something else?"],
        )

    async def _run_structured(
        self,
        args: Dict[str, Any],
        llm_client,
        *,
        context: Optional[Dict[str, Any]] = None,
        prefer_context_device: bool = False,
    ):
        out = await self._run(
            args or {},
            llm_client,
            context=context,
            prefer_context_device=prefer_context_device,
        )
        if isinstance(out, dict) and "ok" in out:
            return out
        return self._to_contract(str(out), args or {})

    # -------------------- Platform handlers --------------------
    async def handle_webui(self, args, llm_client):
        async def inner():
            try:
                return await self._run_structured(args or {}, llm_client)
            except Exception as e:
                logger.error(f"[music_assistant:webui] {e}")
                return action_failure(
                    code="music_assistant_exception",
                    message=str(e),
                    diagnosis=self._diagnosis(),
                    say_hint="Explain that the request failed and suggest retrying.",
                )

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
    async def handle_homeassistant(self, args, llm_client, context: Optional[Dict[str, Any]] = None):
        try:
            return await self._run_structured(
                args or {},
                llm_client,
                context=context,
                prefer_context_device=True,
            )
        except Exception as e:
            logger.error(f"[music_assistant:ha] {e}")
            return action_failure(
                code="music_assistant_exception",
                message=str(e),
                diagnosis=self._diagnosis(),
                say_hint="Explain that playback control failed and ask if the user wants to retry.",
            )

    async def handle_homekit(self, args, llm_client):
        try:
            return await self._run_structured(args or {}, llm_client)
        except Exception as e:
            logger.error(f"[music_assistant:homekit] {e}")
            return action_failure(
                code="music_assistant_exception",
                message=str(e),
                diagnosis=self._diagnosis(),
                say_hint="Explain the failure briefly and ask for a retry if needed.",
            )

    async def handle_xbmc(self, args, llm_client):
        try:
            return await self._run_structured(args or {}, llm_client)
        except Exception as e:
            logger.error(f"[music_assistant:xbmc] {e}")
            return action_failure(
                code="music_assistant_exception",
                message=str(e),
                diagnosis=self._diagnosis(),
                say_hint="Explain that playback control failed and suggest retrying.",
            )

    async def handle_discord(self, message, args, llm_client):
        return await self.handle_webui(args, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self.handle_webui(args, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self.handle_webui(args, llm_client)


verba = MusicAssistantPlugin()
