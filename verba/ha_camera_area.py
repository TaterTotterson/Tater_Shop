import asyncio
import base64
import json
import logging
import mimetypes
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

from helpers import extract_json, redis_client
from verba_base import ToolVerba
from verba_result import action_failure, action_success
from vision_settings import get_vision_settings as get_shared_vision_settings

logger = logging.getLogger("ha_camera_area")
logger.setLevel(logging.INFO)


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


def _to_data_url(image_bytes: bytes, filename: str = "image.jpg") -> str:
    mime = mimetypes.guess_type(filename)[0] or "image/jpeg"
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    return f"data:{mime};base64,{b64}"


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

    return ""


def _platform_supports_media(platform: str) -> bool:
    return (platform or "").strip().lower() in {"webui", "discord", "telegram", "matrix"}


def _coerce_text(value: Any) -> str:
    return "" if value is None else str(value)


class HAClient:
    """Simple Home Assistant REST helper."""

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
        self.timeout = 25
        self.verify_ssl = True
        if self.base_url.startswith("https://") and ".local" in self.base_url:
            self.verify_ssl = False
        if not self.verify_ssl:
            try:
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

    def _req(self, method: str, path: str, *, json_body: Any = None, stream: bool = False) -> Any:
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.base_url}{path}"
        resp = requests.request(
            method,
            url,
            headers=self.headers,
            json=json_body,
            timeout=self.timeout,
            verify=self.verify_ssl,
            stream=stream,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

        ctype = (resp.headers.get("Content-Type") or "").lower()
        if stream or "image/" in ctype:
            return resp.content, resp.headers
        try:
            return resp.json()
        except Exception:
            return resp.text

    def list_states(self) -> List[dict]:
        data = self._req("GET", "/api/states")
        return data if isinstance(data, list) else []

    def camera_proxy(self, entity_id: str) -> Tuple[bytes, str]:
        data, headers = self._req(
            "GET",
            f"/api/camera_proxy/{entity_id}",
            stream=True,
        )
        ctype = (headers.get("Content-Type") or "image/jpeg").split(";")[0].strip().lower()
        if not isinstance(data, (bytes, bytearray)) or len(data) < 500:
            raise RuntimeError(f"Empty/invalid camera image for {entity_id}")
        return bytes(data), ctype or "image/jpeg"


class HACameraAreaPlugin(ToolVerba):
    name = "ha_camera_area"
    verba_name = "Home Assistant Camera Area"
    version = "1.0.2"
    min_tater_version = "59"
    pretty_name = "HA Camera Area"
    settings_category = "Home Assistant Control"
    platforms = ["homeassistant", "webui", "macos", "xbmc", "homekit", "discord", "telegram", "matrix", "irc"]
    tags = ["homeassistant", "camera", "area"]

    usage = '{"function":"ha_camera_area","arguments":{"query":"what is happening in the front yard"}}'
    description = "Uses Home Assistant camera snapshots to report what is currently happening in requested areas."
    verba_dec = "Find cameras in a Home Assistant area and describe what they see."
    when_to_use = "Use when the user asks what cameras in an area are seeing right now."
    how_to_use = "Pass a natural-language area request in query, or provide area directly."
    common_needs = ["Area request in query, or area/target argument."]
    missing_info_prompts = []
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you are checking Home Assistant area cameras now. "
        "Only output that message."
    )
    required_settings = {}

    def __init__(self):
        vision_settings = get_shared_vision_settings(
            default_api_base="http://127.0.0.1:1234",
            default_model="qwen2.5-vl-7b-instruct",
        )
        self.vision_api_base = str(vision_settings.get("api_base") or "http://127.0.0.1:1234").strip().rstrip("/")
        self.vision_model = str(vision_settings.get("model") or "qwen2.5-vl-7b-instruct").strip()
        self.vision_api_key = str(vision_settings.get("api_key") or "").strip()

    def _get_client(self) -> Optional[HAClient]:
        try:
            return HAClient()
        except Exception as exc:
            logger.error("[ha_camera_area] Failed to initialize HA client: %s", exc)
            return None

    @staticmethod
    def _normalize_area_name(text: str) -> str:
        return re.sub(r"\s+", " ", _coerce_text(text).strip().lower())

    def _camera_rows_from_states(self, states: List[dict]) -> List[dict]:
        rows: List[dict] = []
        seen: set[str] = set()
        for st in states or []:
            if not isinstance(st, dict):
                continue
            eid = _coerce_text(st.get("entity_id")).strip().lower()
            if not eid.startswith("camera.") or eid in seen:
                continue
            seen.add(eid)
            attrs = st.get("attributes") if isinstance(st.get("attributes"), dict) else {}
            name = _coerce_text(attrs.get("friendly_name") or eid).strip()
            rows.append({"entity_id": eid, "name": name or eid})
        return rows

    def _area_match_terms(self, area_hint: str) -> List[str]:
        hint = self._normalize_area_name(area_hint)
        if not hint:
            return []

        hint_words = [w for w in re.split(r"\s+", hint) if w]
        # Area aliases let broad requests (e.g., "front") include nearby camera names.
        alias_map: Dict[str, List[str]] = {
            "front": ["front", "entry", "door", "doorbell", "porch", "driveway", "garage"],
            "back": ["back", "rear", "yard", "backyard", "patio", "deck"],
            "side": ["side", "gate", "path"],
            "garage": ["garage", "driveway", "front"],
            "driveway": ["driveway", "garage", "front"],
            "yard": ["yard", "lawn", "front yard", "back yard"],
        }

        terms: List[str] = [hint]
        for word in hint_words:
            terms.append(word)
            terms.extend(alias_map.get(word, []))

        deduped: List[str] = []
        seen: set[str] = set()
        for term in terms:
            norm = self._normalize_area_name(term)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            deduped.append(norm)
        return deduped

    def _match_cameras_by_name(self, camera_rows: List[dict], area_hint: str) -> List[dict]:
        hint = self._normalize_area_name(area_hint)
        if not hint:
            return []

        hint_words = [w for w in re.split(r"\s+", hint) if w]
        terms = self._area_match_terms(hint)
        scored: List[Tuple[int, str, dict]] = []
        for cam in camera_rows or []:
            if not isinstance(cam, dict):
                continue
            name_norm = self._normalize_area_name(cam.get("name") or cam.get("entity_id"))
            if not name_norm:
                continue
            score = 0
            if name_norm == hint:
                score += 120
            if hint and hint in name_norm:
                score += 80
            for term in terms:
                if term and term in name_norm:
                    score += 20
            for word in hint_words:
                if word and word in name_norm:
                    score += 10
            if score > 0:
                scored.append((score, name_norm, cam))

        scored.sort(key=lambda item: (-item[0], item[1]))

        matched: List[dict] = []
        seen_ids: set[str] = set()
        for _, _, cam in scored:
            eid = _coerce_text(cam.get("entity_id")).strip().lower()
            if not eid or eid in seen_ids:
                continue
            seen_ids.add(eid)
            matched.append(cam)
        return matched

    async def _match_cameras_with_ai(
        self,
        *,
        area_hint: str,
        camera_rows: List[dict],
        llm_client: Any,
    ) -> List[dict]:
        hint = _coerce_text(area_hint).strip()
        if not hint:
            return []
        if llm_client is None or not hasattr(llm_client, "chat"):
            return []

        candidates: List[dict] = []
        by_id: Dict[str, dict] = {}
        for cam in camera_rows or []:
            if not isinstance(cam, dict):
                continue
            eid = _coerce_text(cam.get("entity_id")).strip().lower()
            if not eid.startswith("camera."):
                continue
            name = _coerce_text(cam.get("name") or eid).strip() or eid
            row = {"entity_id": eid, "name": name}
            candidates.append(row)
            by_id[eid] = row
        if not candidates:
            return []

        prompt = (
            "Select Home Assistant camera entity_ids that best match the requested area.\n"
            "Return strict JSON only: {\"camera_entity_ids\":[\"camera.one\",\"camera.two\"]}\n"
            "Rules:\n"
            "- Use only entity_ids from candidates.\n"
            "- Match by camera names to the requested area phrase.\n"
            "- Include nearby synonyms when reasonable (example: front can include driveway/garage/porch).\n"
            "- Prefer precision over recall.\n"
            "- No prose."
        )
        payload = {"requested_area": hint, "candidates": candidates}

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                max_tokens=220,
                temperature=0.0,
            )
        except Exception as exc:
            logger.info("[ha_camera_area] AI camera match failed: %s", exc)
            return []

        content = _coerce_text((resp.get("message") or {}).get("content")).strip()
        if not content:
            return []

        parsed = None
        try:
            parsed = json.loads(content)
        except Exception:
            blob = extract_json(content)
            if blob:
                try:
                    parsed = json.loads(blob)
                except Exception:
                    parsed = None
        if not isinstance(parsed, dict):
            return []

        ids_raw = parsed.get("camera_entity_ids")
        if not isinstance(ids_raw, list):
            return []

        out: List[dict] = []
        seen: set[str] = set()
        for item in ids_raw:
            eid = _coerce_text(item).strip().lower()
            if not eid.startswith("camera.") or eid in seen:
                continue
            row = by_id.get(eid)
            if not isinstance(row, dict):
                continue
            seen.add(eid)
            out.append(row)
        return out

    async def _extract_area_from_query(self, query: str, llm_client: Any) -> str:
        text = _coerce_text(query).strip()
        if not text:
            return ""
        if llm_client is None or not hasattr(llm_client, "chat"):
            return ""

        prompt = (
            "Extract the Home Assistant area from the request.\n"
            "Return strict JSON only: {\"area\":\"<area>|\"}\n"
            "Rules:\n"
            "- area should be a short area name phrase (for example: front yard, driveway, living room).\n"
            "- If no clear area is present, return empty string.\n"
            "- No prose."
        )
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": text},
                ],
                max_tokens=80,
                temperature=0.0,
            )
        except Exception as exc:
            logger.info("[ha_camera_area] area extraction failed: %s", exc)
            return ""

        content = _coerce_text((resp.get("message") or {}).get("content")).strip()
        if not content:
            return ""
        parsed = None
        try:
            parsed = json.loads(content)
        except Exception:
            blob = extract_json(content)
            if blob:
                try:
                    parsed = json.loads(blob)
                except Exception:
                    parsed = None
        if not isinstance(parsed, dict):
            return ""
        return _coerce_text(parsed.get("area")).strip()

    def _vision_prompt(self, query: str, camera_name: str) -> str:
        return (
            f"Camera: {camera_name}\n"
            f"User request: {query}\n\n"
            "Describe only what is visible right now. "
            "Mention people, vehicles, packages, animals, doors/gates, and notable activity. "
            "If nothing notable is happening, say so. Keep it concise."
        )

    def _call_vision(self, image_bytes: bytes, *, prompt: str, filename: str) -> str:
        url = f"{self.vision_api_base}/v1/chat/completions"
        payload = {
            "model": self.vision_model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt or "Describe this image."},
                        {"type": "image_url", "image_url": {"url": _to_data_url(image_bytes, filename)}},
                    ],
                }
            ],
            "temperature": 0.2,
            "max_tokens": 500,
        }
        headers = {"Content-Type": "application/json"}
        if self.vision_api_key:
            headers["Authorization"] = f"Bearer {self.vision_api_key}"

        resp = requests.post(url, json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            return f"(vision error {resp.status_code}) {resp.text[:200]}"
        try:
            data = resp.json()
            return _coerce_text(((data.get("choices") or [{}])[0].get("message") or {}).get("content")).strip()
        except Exception:
            return "(vision error) unexpected response"

    @staticmethod
    def _summary_from_descriptions(rows: List[dict]) -> str:
        valid = [r for r in rows if isinstance(r, dict)]
        if not valid:
            return "No camera descriptions were available."
        if len(valid) == 1:
            first = valid[0]
            return f"{_coerce_text(first.get('camera'))}: {_coerce_text(first.get('vision'))}"
        first = valid[0]
        return (
            f"Captured {len(valid)} camera snapshots. "
            f"First: {_coerce_text(first.get('camera'))}: {_coerce_text(first.get('vision'))}"
        )

    async def handle_webui(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="webui", context=context)

    async def handle_macos(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="macos", context=context)

    async def handle_homeassistant(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, platform="homeassistant", context=context)

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

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client, context=None):
        ctx = context or {}
        if "raw_message" not in ctx:
            ctx["raw_message"] = raw_message
        return await self._handle(args, llm_client, platform="irc", context=ctx)

    async def _handle(self, args, llm_client, *, platform: str, context: Optional[dict]):
        client = self._get_client()
        if not client:
            return action_failure(
                code="ha_camera_area_not_configured",
                message=(
                    "Home Assistant is not configured. Set HA_BASE_URL and HA_TOKEN in "
                    "WebUI -> Settings -> Home Assistant Settings."
                ),
                say_hint="Explain required Home Assistant settings are missing.",
            )

        payload = args or {}
        query = _coerce_text(payload.get("query")).strip()
        if not query:
            query = _extract_context_text(context or {})

        area_hint = await self._extract_area_from_query(query, llm_client) if query else ""
        if not area_hint:
            return action_failure(
                code="missing_area",
                message="Please tell me which area to check (for example: front yard, driveway, living room).",
                needs=["Provide an area name to inspect."],
                say_hint="Ask the user for a specific area name.",
            )

        try:
            all_states = client.list_states()
        except Exception as exc:
            logger.info("[ha_camera_area] failed to list camera states: %s", exc)
            all_states = []

        all_camera_rows = self._camera_rows_from_states(all_states)
        camera_rows = await self._match_cameras_with_ai(
            area_hint=area_hint,
            camera_rows=all_camera_rows,
            llm_client=llm_client,
        )
        if not camera_rows:
            camera_rows = self._match_cameras_by_name(all_camera_rows, area_hint)

        if not camera_rows:
            return action_failure(
                code="area_not_found",
                message=f"No Home Assistant camera entities matched area: {area_hint}",
                needs=["Try a different area name or confirm cameras are assigned to an area in Home Assistant."],
                say_hint="Explain no cameras matched the requested area.",
            )

        picked = camera_rows[:6]
        camera_descriptions: List[dict] = []
        artifacts: List[dict] = []
        snapshot_results: List[dict] = []
        for cam in picked:
            eid = _coerce_text(cam.get("entity_id")).strip().lower()
            name = _coerce_text(cam.get("name") or eid).strip() or eid
            try:
                img_bytes, mimetype = client.camera_proxy(eid)
                prompt = self._vision_prompt(query or f"Check area {area_hint}", name)
                vision_text = await asyncio.to_thread(
                    self._call_vision,
                    img_bytes,
                    prompt=prompt,
                    filename=f"{name}.jpg",
                )
                camera_descriptions.append({"camera": name, "entity_id": eid, "vision": vision_text})
                snapshot_results.append({"camera": name, "ok": True, "bytes": len(img_bytes)})

                if _platform_supports_media(platform):
                    artifacts.append(
                        _build_media_metadata(
                            img_bytes,
                            media_type="image",
                            name=f"{name}.jpg",
                            mimetype=mimetype,
                        )
                    )
            except Exception as exc:
                camera_descriptions.append({"camera": name, "entity_id": eid, "vision": f"(snapshot error) {exc}"})
                snapshot_results.append({"camera": name, "ok": False, "error": str(exc)})

        facts = {
            "area": area_hint,
            "cameras_used": [r.get("name") for r in picked],
            "camera_descriptions": camera_descriptions,
            "snapshot_results": snapshot_results,
        }
        summary = self._summary_from_descriptions(camera_descriptions)

        if _platform_supports_media(platform):
            return action_success(
                facts=facts,
                summary_for_user=summary,
                say_hint="Summarize what cameras in the requested area currently show.",
                artifacts=artifacts,
            )
        return action_success(
            facts=facts,
            summary_for_user=summary,
            say_hint="Summarize what cameras in the requested area currently show.",
        )


verba = HACameraAreaPlugin()
