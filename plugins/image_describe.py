import asyncio
import base64
import json
import logging
import mimetypes
import os
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis
import requests

from plugin_base import ToolPlugin
from plugin_result import action_failure, action_success
from vision_settings import (
    DEFAULT_VISION_API_BASE,
    DEFAULT_VISION_MODEL,
    get_vision_settings,
)
from helpers import redis_client

logger = logging.getLogger("image_describe")
logger.setLevel(logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent
_agent_root_env = str(os.getenv("TATER_AGENT_ROOT", "") or "").strip()
if _agent_root_env:
    _agent_root_path = Path(_agent_root_env).expanduser()
    if not _agent_root_path.is_absolute():
        _agent_root_path = BASE_DIR / _agent_root_path
else:
    _agent_root_path = BASE_DIR / "agent_lab"
AGENT_LAB_DIR = _agent_root_path.resolve()

WEBUI_FILE_BLOB_KEY_PREFIX = "webui:file:"
VISION_MAX_IMAGE_BYTES = int(os.getenv("TATER_VISION_MAX_IMAGE_BYTES", str(12 * 1024 * 1024)))
VISION_ALLOWED_MIMETYPES = {
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/webp",
    "image/gif",
    "image/bmp",
    "image/tiff",
}
VISION_DEFAULT_PROMPT = (
    "Describe this image clearly and concisely. Mention important objects, people, actions, "
    "and any visible text."
)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    return str(value)


def _to_data_url(image_bytes: bytes, filename: str) -> str:
    mime = mimetypes.guess_type(filename or "")[0] or "image/png"
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    return f"data:{mime};base64,{b64}"


def _normalize_filename(name: Any, mimetype: Any = "") -> str:
    text = _as_text(name).strip()
    if text:
        return text
    mime = _as_text(mimetype).strip().lower()
    if mime in {"image/jpeg", "image/jpg"}:
        return "image.jpg"
    if mime == "image/webp":
        return "image.webp"
    if mime == "image/gif":
        return "image.gif"
    if mime == "image/bmp":
        return "image.bmp"
    if mime == "image/tiff":
        return "image.tiff"
    return "image.png"


def _normalize_agent_path(raw_path: Any) -> Optional[Path]:
    raw = _as_text(raw_path).strip()
    if not raw:
        return None

    normalized = raw.replace("\\", "/").strip()

    if normalized in {"download", "downloads"}:
        normalized = "downloads"
    elif normalized.startswith("download/"):
        normalized = "downloads/" + normalized[len("download/") :]
    elif normalized in {"document", "documents"}:
        normalized = "documents"
    elif normalized.startswith("document/"):
        normalized = "documents/" + normalized[len("document/") :]
    elif normalized in {"/download", "/downloads"}:
        normalized = "/downloads"
    elif normalized.startswith("/download/"):
        normalized = "/downloads/" + normalized[len("/download/") :]
    elif normalized in {"/document", "/documents"}:
        normalized = "/documents"
    elif normalized.startswith("/document/"):
        normalized = "/documents/" + normalized[len("/document/") :]

    if normalized in {"", ".", "/"}:
        candidate = AGENT_LAB_DIR
    elif normalized.startswith("/"):
        candidate = AGENT_LAB_DIR / normalized.lstrip("/")
    else:
        candidate = AGENT_LAB_DIR / normalized

    try:
        resolved = candidate.resolve()
        root = AGENT_LAB_DIR.resolve()
        if not (resolved == root or root in resolved.parents):
            return None
        return resolved
    except Exception:
        return None


class ImageDescribePlugin(ToolPlugin):
    name = "image_describe"
    plugin_name = "Image Describe"
    version = "2.0.0"
    min_tater_version = "50"

    usage = (
        '{"function":"image_describe","arguments":{'
        '"prompt":"Optional description focus",'
        '"url":"Optional direct image URL (http/https)",'
        '"path":"Optional /downloads/... or /documents/... path",'
        '"blob_key":"Optional Redis blob key",'
        '"file_id":"Optional legacy file id",'
        '"image_ref":{"type":"image","url":"optional","blob_key":"optional","name":"optional","mimetype":"optional"},'
        '"media_ref":{"type":"image","url":"optional","blob_key":"optional"},'
        '"media_refs":[{"type":"image","url":"optional","blob_key":"optional"}]'
        '}}'
    )

    description = (
        "Describe an image using the configured vision model. "
        "If no explicit image source is provided, it uses the latest image from chat history."
    )
    plugin_dec = "Describe an image from chat history or explicit image reference."
    pretty_name = "Analyzing Your Image"
    settings_category = "Vision"

    when_to_use = (
        "Use when the user asks what is in an image, wants visual details, "
        "or asks to read visible text from an image."
    )
    common_needs = [
        "image source (optional if image was just uploaded)",
        "what to focus on (optional prompt)",
    ]
    required_args = []
    optional_args = [
        "prompt",
        "query",
        "url",
        "path",
        "source",
        "file",
        "blob_key",
        "file_id",
        "image_ref",
        "media_ref",
        "media_refs",
        "history_key",
    ]
    missing_info_prompts = [
        "Which image should I describe? You can upload one, provide an image URL, or give /downloads/... or /documents/... path.",
    ]

    waiting_prompt_template = (
        "Write a short message telling {mention} you're analyzing the image now. "
        "Only output that message."
    )

    platforms = ["discord", "webui", "matrix", "telegram", "automation"]

    required_settings = {
        "api_base": {
            "label": "API Base URL",
            "description": "OpenAI-compatible base URL (for example: http://127.0.0.1:1234).",
            "type": "text",
            "default": DEFAULT_VISION_API_BASE,
        },
        "model": {
            "label": "Vision Model",
            "description": "OpenAI-compatible vision model name.",
            "type": "text",
            "default": DEFAULT_VISION_MODEL,
        },
        "api_key": {
            "label": "API Key",
            "description": "Optional API key if your vision endpoint requires authorization.",
            "type": "text",
            "default": "",
        },
    }

    @staticmethod
    def _get_blob_client() -> redis.Redis:
        host = os.getenv("REDIS_HOST", "127.0.0.1")
        port = int(os.getenv("REDIS_PORT", "6379"))
        return redis.Redis(host=host, port=port, db=0, decode_responses=False)

    @staticmethod
    def _blob_key_candidates(*, blob_key: Any = None, file_id: Any = None) -> List[str]:
        out: List[str] = []
        blob = _as_text(blob_key).strip()
        if blob:
            out.append(blob)

        fid = _as_text(file_id).strip()
        if fid:
            if fid.startswith((WEBUI_FILE_BLOB_KEY_PREFIX, "tater:blob:", "tater:matrix:")):
                out.append(fid)
            else:
                out.append(f"{WEBUI_FILE_BLOB_KEY_PREFIX}{fid}")
                out.append(fid)

        unique: List[str] = []
        seen = set()
        for key in out:
            if key and key not in seen:
                unique.append(key)
                seen.add(key)
        return unique

    @classmethod
    def _load_blob_bytes(cls, blob_client: redis.Redis, *, blob_key: Any = None, file_id: Any = None) -> Optional[bytes]:
        for key in cls._blob_key_candidates(blob_key=blob_key, file_id=file_id):
            try:
                data = blob_client.get(key)
            except Exception:
                data = None
            if data is None:
                continue
            if isinstance(data, (bytes, bytearray)):
                return bytes(data)
            if isinstance(data, str):
                return data.encode("utf-8", errors="replace")
        return None

    @staticmethod
    def _decode_base64_payload(data: Any) -> Optional[bytes]:
        text = _as_text(data).strip()
        if not text:
            return None
        if text.startswith("data:") and "," in text:
            text = text.split(",", 1)[1]
        pad = len(text) % 4
        if pad:
            text += "=" * (4 - pad)
        try:
            decoded = base64.b64decode(text)
        except Exception:
            return None
        return bytes(decoded) if decoded else None

    @staticmethod
    def _mime_allowed(mimetype: Any) -> bool:
        mime = _as_text(mimetype).strip().lower()
        if not mime:
            return False
        return mime in VISION_ALLOWED_MIMETYPES

    @staticmethod
    def _looks_like_http_url(value: Any) -> bool:
        text = _as_text(value).strip()
        if not text:
            return False
        parsed = urllib.parse.urlparse(text)
        return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)

    @classmethod
    def _download_image_url(cls, value: Any) -> Tuple[Optional[bytes], Optional[str], Optional[str], Optional[str]]:
        raw_url = _as_text(value).strip()
        if not raw_url:
            return None, None, None, "url_empty"
        if not cls._looks_like_http_url(raw_url):
            return None, None, None, "url_invalid"

        try:
            with requests.get(
                raw_url,
                timeout=30,
                stream=True,
                allow_redirects=True,
                headers={"User-Agent": "Tater/1.0"},
            ) as response:
                if response.status_code >= 300:
                    return None, None, None, "url_http_error"

                content_type = _as_text(response.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
                chunks: List[bytes] = []
                total = 0
                for chunk in response.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > VISION_MAX_IMAGE_BYTES:
                        return None, None, None, "url_too_large"
                    chunks.append(chunk)
                data = b"".join(chunks)
                final_url = _as_text(response.url).strip() or raw_url
        except requests.RequestException:
            return None, None, None, "url_request_failed"
        except Exception:
            return None, None, None, "url_request_failed"

        if not data:
            return None, None, None, "url_empty_response"

        parsed = urllib.parse.urlparse(final_url)
        guessed_name = Path(parsed.path).name if parsed.path else ""
        filename = _normalize_filename(guessed_name, content_type)
        guessed_mime = _as_text(mimetypes.guess_type(filename)[0]).strip().lower()
        mimetype = content_type or guessed_mime or "image/png"

        if content_type and not content_type.startswith("image/"):
            return None, None, None, "url_not_image"
        if not content_type and (not guessed_mime or not guessed_mime.startswith("image/")):
            return None, None, None, "url_not_image"
        if mimetype.startswith("image/") and not cls._mime_allowed(mimetype):
            return None, None, None, "url_unsupported_type"

        return data, filename, mimetype, None

    @classmethod
    def _extract_image_from_payload(
        cls,
        blob_client: redis.Redis,
        payload: Any,
    ) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        if payload is None:
            return None, None, None

        if isinstance(payload, dict) and payload.get("marker") == "plugin_response":
            return cls._extract_image_from_payload(blob_client, payload.get("content"))

        if isinstance(payload, list):
            for item in payload:
                raw, name, mime = cls._extract_image_from_payload(blob_client, item)
                if raw:
                    return raw, name, mime
            return None, None, None

        if not isinstance(payload, dict):
            return None, None, None

        media_type = _as_text(payload.get("type")).strip().lower()
        mimetype = _as_text(payload.get("mimetype")).strip().lower()
        if not mimetype:
            guessed_name = _as_text(payload.get("name")).strip()
            mimetype = _as_text(mimetypes.guess_type(guessed_name)[0]).strip().lower()

        if media_type in {"image", "file"}:
            if media_type == "file" and (not mimetype or not mimetype.startswith("image/")):
                return None, None, None
            if mimetype and not cls._mime_allowed(mimetype):
                return None, None, None

            filename = _normalize_filename(payload.get("name"), mimetype)

            if isinstance(payload.get("bytes"), (bytes, bytearray)):
                return bytes(payload["bytes"]), filename, mimetype or "image/png"
            if isinstance(payload.get("data"), (bytes, bytearray)):
                return bytes(payload["data"]), filename, mimetype or "image/png"

            decoded = cls._decode_base64_payload(payload.get("data"))
            if decoded:
                return decoded, filename, mimetype or "image/png"

            blob = cls._load_blob_bytes(
                blob_client,
                blob_key=payload.get("blob_key"),
                file_id=payload.get("id") or payload.get("file_id"),
            )
            if blob:
                mm = mimetype or _as_text(mimetypes.guess_type(filename)[0]).strip().lower() or "image/png"
                return blob, filename, mm

            ref_url = payload.get("url")
            if cls._looks_like_http_url(ref_url):
                raw, remote_name, remote_mime, err = cls._download_image_url(ref_url)
                if raw and not err:
                    final_name = _normalize_filename(payload.get("name") or remote_name, mimetype or remote_mime)
                    final_mime = mimetype or remote_mime or _as_text(mimetypes.guess_type(final_name)[0]).strip().lower() or "image/png"
                    return raw, final_name, final_mime

            return None, None, None

        # Fall back to generic blob shape
        if payload.get("blob_key") or payload.get("file_id") or payload.get("id"):
            blob = cls._load_blob_bytes(
                blob_client,
                blob_key=payload.get("blob_key"),
                file_id=payload.get("file_id") or payload.get("id"),
            )
            if blob:
                filename = _normalize_filename(payload.get("name"), payload.get("mimetype"))
                mimetype = _as_text(payload.get("mimetype")).strip().lower() or _as_text(mimetypes.guess_type(filename)[0]).strip().lower()
                if not mimetype:
                    mimetype = "image/png"
                if mimetype.startswith("image/"):
                    return blob, filename, mimetype

        if cls._looks_like_http_url(payload.get("url")):
            raw, filename, mimetype, err = cls._download_image_url(payload.get("url"))
            if raw and not err:
                return raw, filename, mimetype

        return None, None, None

    def _find_latest_image_in_history(self, history_key: str, scan_limit: int = 120) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        blob_client = self._get_blob_client()

        try:
            raw_items = redis_client.lrange(history_key, -scan_limit, -1)
        except Exception:
            raw_items = []

        for raw in reversed(raw_items or []):
            try:
                msg = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            if not isinstance(msg, dict):
                continue

            content = msg.get("content")
            image_bytes, filename, mimetype = self._extract_image_from_payload(blob_client, content)
            if image_bytes:
                return image_bytes, filename, mimetype

        return None, None, None

    @staticmethod
    async def _find_latest_matrix_image_via_client(client, room_id: str, limit: int = 30) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        if client is None or not room_id:
            return None, None, None

        try:
            resp = await client.room_messages(room_id=room_id, start=None, direction="b", limit=limit)
        except TypeError:
            try:
                resp = await client.room_messages(room_id, None, "b", limit)
            except Exception:
                return None, None, None
        except Exception:
            return None, None, None

        chunk = getattr(resp, "chunk", None) or getattr(resp, "events", None) or []
        if not chunk:
            return None, None, None

        for event in chunk:
            content = getattr(event, "content", None) or (event.get("content") if isinstance(event, dict) else None) or {}
            msgtype = content.get("msgtype") or content.get("msg_type")
            if msgtype != "m.image":
                continue

            mxc_url = content.get("url")
            if not mxc_url:
                continue

            filename = _normalize_filename(content.get("body"), content.get("mimetype"))
            mimetype = _as_text(content.get("info", {}).get("mimetype") or content.get("mimetype")).strip().lower()

            try:
                dl = await client.download(mxc_url)
                body = getattr(dl, "body", None)
                if body is None:
                    body = getattr(dl, "data", None)
                if body is None and isinstance(dl, dict):
                    body = dl.get("body") or dl.get("data")
                if body:
                    return bytes(body), filename, (mimetype or "image/png")
            except Exception:
                continue

        return None, None, None

    def _resolve_explicit_image(self, args: Dict[str, Any]) -> Tuple[Optional[bytes], Optional[str], Optional[str], Optional[str]]:
        blob_client = self._get_blob_client()

        for key in ("image_ref", "media_ref"):
            ref = args.get(key)
            if isinstance(ref, dict):
                image_bytes, filename, mimetype = self._extract_image_from_payload(blob_client, ref)
                if image_bytes:
                    return image_bytes, filename, mimetype, key

        media_refs = args.get("media_refs")
        if isinstance(media_refs, list):
            for idx, item in enumerate(media_refs):
                if not isinstance(item, dict):
                    continue
                image_bytes, filename, mimetype = self._extract_image_from_payload(blob_client, item)
                if image_bytes:
                    return image_bytes, filename, mimetype, f"media_refs[{idx}]"

        explicit_url = args.get("url")
        explicit_url_text = _as_text(explicit_url).strip()
        if explicit_url_text:
            if not self._looks_like_http_url(explicit_url_text):
                return None, None, None, "url_invalid"
            explicit_url = explicit_url_text
        else:
            source_hint = args.get("source") or args.get("file")
            if self._looks_like_http_url(source_hint):
                explicit_url = source_hint
            else:
                explicit_url = None

        if explicit_url:
            data, filename, mimetype, err = self._download_image_url(explicit_url)
            if err:
                return None, None, None, err
            if data:
                return data, filename, mimetype, "url"

        image_path = args.get("path")
        if not image_path:
            source_hint = args.get("source") or args.get("file")
            if not self._looks_like_http_url(source_hint):
                image_path = source_hint
        if _as_text(image_path).strip():
            resolved = _normalize_agent_path(image_path)
            if resolved is None:
                return None, None, None, "path_outside_workspace"
            if not resolved.exists() or not resolved.is_file():
                return None, None, None, "path_missing"
            try:
                data = resolved.read_bytes()
            except Exception:
                return None, None, None, "path_read_failed"
            if not data:
                return None, None, None, "path_empty"
            filename = resolved.name or "image.png"
            mimetype = _as_text(mimetypes.guess_type(filename)[0]).strip().lower() or "image/png"
            if mimetype and not mimetype.startswith("image/"):
                return None, None, None, "path_not_image"
            return data, filename, mimetype, "path"

        blob = self._load_blob_bytes(
            blob_client,
            blob_key=args.get("blob_key"),
            file_id=args.get("file_id"),
        )
        if blob:
            filename = _normalize_filename(args.get("name"), args.get("mimetype"))
            mimetype = _as_text(args.get("mimetype")).strip().lower() or _as_text(mimetypes.guess_type(filename)[0]).strip().lower() or "image/png"
            if mimetype and not mimetype.startswith("image/"):
                return None, None, None, "blob_not_image"
            return blob, filename, mimetype, "blob"

        return None, None, None, None

    def _call_vision_api(self, *, image_bytes: bytes, filename: str, prompt: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        settings = get_vision_settings()
        api_base = _as_text(settings.get("api_base")).strip().rstrip("/")
        model = _as_text(settings.get("model")).strip()
        api_key = _as_text(settings.get("api_key")).strip()

        if not api_base or not model:
            return None, None, "Vision settings are incomplete. Configure API base and model in Settings."

        url = f"{api_base}/v1/chat/completions"
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": _as_text(prompt).strip() or VISION_DEFAULT_PROMPT},
                        {"type": "image_url", "image_url": {"url": _to_data_url(image_bytes, filename)}},
                    ],
                }
            ],
            "temperature": 0.2,
        }

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=90)
        except Exception as exc:
            return None, model, f"Vision API request failed: {exc}"

        if response.status_code >= 300:
            detail = _as_text(response.text).strip()
            if detail:
                detail = detail[:400]
                return None, model, f"Vision API request failed with HTTP {response.status_code}: {detail}"
            return None, model, f"Vision API request failed with HTTP {response.status_code}."

        try:
            parsed = response.json()
        except Exception:
            return None, model, "Vision API returned non-JSON output."

        try:
            content = parsed["choices"][0]["message"]["content"]
        except Exception:
            return None, model, "Vision API response did not include a valid assistant message."

        description = ""
        if isinstance(content, str):
            description = content.strip()
        elif isinstance(content, list):
            chunks: List[str] = []
            for item in content:
                if isinstance(item, str) and item.strip():
                    chunks.append(item.strip())
                elif isinstance(item, dict) and _as_text(item.get("text")).strip():
                    chunks.append(_as_text(item.get("text")).strip())
            description = "\n".join(chunks).strip()

        if not description:
            return None, model, "Vision API returned an empty description."

        return description, model, None

    async def _describe(
        self,
        args: Dict[str, Any],
        *,
        history_key: str,
        matrix_client: Any = None,
        matrix_room_id: str = "",
    ) -> Dict[str, Any]:
        request_args = dict(args or {})
        prompt = _as_text(request_args.get("prompt") or request_args.get("query")).strip() or VISION_DEFAULT_PROMPT

        image_bytes, filename, mimetype, source = self._resolve_explicit_image(request_args)

        if image_bytes is None and source in {
            "path_outside_workspace",
            "path_missing",
            "path_read_failed",
                "path_empty",
                "path_not_image",
                "blob_not_image",
                "url_empty",
                "url_invalid",
                "url_request_failed",
                "url_http_error",
                "url_empty_response",
                "url_not_image",
                "url_unsupported_type",
                "url_too_large",
            }:
            msg_map = {
                "path_outside_workspace": "Image path is outside the allowed workspace root.",
                "path_missing": "Image path does not exist.",
                "path_read_failed": "Failed to read image from the provided path.",
                "path_empty": "The provided image file is empty.",
                "path_not_image": "The provided path is not an image file.",
                "blob_not_image": "The provided blob/file reference is not an image.",
                "url_empty": "Image URL is empty.",
                "url_invalid": "Image URL must be a valid http/https URL.",
                "url_request_failed": "Failed to download the image URL.",
                "url_http_error": "Image URL request returned an HTTP error.",
                "url_empty_response": "Image URL returned no data.",
                "url_not_image": "URL did not resolve to an image.",
                "url_unsupported_type": "Image URL returned an unsupported image type.",
                "url_too_large": f"Image URL payload is too large. Max size is {VISION_MAX_IMAGE_BYTES} bytes.",
            }
            return action_failure(
                code="invalid_image_source",
                message=msg_map.get(source, "Invalid image source."),
                needs=["Use an image from this chat, provide an image URL, or provide /downloads/... or /documents/... path."],
                say_hint="Ask for a valid image source and keep guidance brief.",
            )

        if image_bytes is None:
            image_bytes, filename, mimetype = self._find_latest_image_in_history(history_key)
            source = source or "history"

        if image_bytes is None and matrix_client is not None and matrix_room_id:
            matrix_bytes, matrix_name, matrix_mime = await self._find_latest_matrix_image_via_client(
                matrix_client,
                matrix_room_id,
                limit=40,
            )
            if matrix_bytes:
                image_bytes = matrix_bytes
                filename = matrix_name
                mimetype = matrix_mime
                source = "matrix_recent"

        if image_bytes is None:
            return action_failure(
                code="no_image_found",
                message="No image was found. Upload an image first, provide an image URL, or provide a path in /downloads or /documents.",
                needs=["Please provide an image source to describe."],
                say_hint="Ask for an image upload, URL, or a path in /downloads or /documents.",
            )

        if len(image_bytes) > VISION_MAX_IMAGE_BYTES:
            return action_failure(
                code="image_too_large",
                message=f"Image is too large ({len(image_bytes)} bytes). Max supported size is {VISION_MAX_IMAGE_BYTES} bytes.",
                needs=["Use a smaller image file."],
                say_hint="Explain the size limit and ask for a smaller image.",
            )

        filename = _normalize_filename(filename, mimetype)
        description, model, error = await asyncio.to_thread(
            self._call_vision_api,
            image_bytes=image_bytes,
            filename=filename,
            prompt=prompt,
        )
        if error:
            return action_failure(
                code="vision_request_failed",
                message=error,
                say_hint="Explain the vision request failure and ask whether to retry.",
            )

        text = _as_text(description).strip()
        return action_success(
            data={
                "description": text,
                "text": text,
                "filename": filename,
                "mimetype": mimetype or _as_text(mimetypes.guess_type(filename)[0]).strip() or "image/png",
                "model": model or "",
                "source": source or "unknown",
                "history_key": history_key,
            },
            summary_for_user=text,
            say_hint="Return the image description directly and do not invent extra visual details.",
        )

    async def handle_discord(self, message, args, llm_client):
        del llm_client
        channel_id = _as_text(getattr(getattr(message, "channel", None), "id", "")).strip()
        history_key = f"tater:channel:{channel_id}:history" if channel_id else "tater:channel:unknown:history"
        return await self._describe(args or {}, history_key=history_key)

    async def handle_webui(self, args, llm_client):
        del llm_client
        history_key = _as_text((args or {}).get("history_key")).strip() or "webui:chat_history"
        return await self._describe(args or {}, history_key=history_key)

    async def handle_telegram(self, update, args, llm_client):
        del llm_client
        chat_id = ""
        if isinstance(update, dict):
            msg = update.get("message") or {}
            chat = msg.get("chat") or {}
            chat_id = _as_text(chat.get("id")).strip()
        history_key = f"tater:telegram:{chat_id}:history" if chat_id else "tater:telegram:unknown:history"
        return await self._describe(args or {}, history_key=history_key)

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        del sender, body, llm_client, kwargs
        room_id = getattr(room, "room_id", None) or (room.get("room_id") if isinstance(room, dict) else None)
        room_id = _as_text(room_id).strip()
        history_key = f"tater:matrix:{room_id}:history" if room_id else "tater:matrix:unknown:history"
        return await self._describe(
            args or {},
            history_key=history_key,
            matrix_client=client,
            matrix_room_id=room_id,
        )

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        del bot, channel, user, raw_message, args, llm_client
        return action_failure(
            code="unsupported_platform",
            message="image_describe is not available on IRC.",
            available_on=["discord", "webui", "matrix", "telegram", "automation"],
            say_hint="Explain this tool is not available on IRC and ask whether to continue on a supported platform.",
        )


plugin = ImageDescribePlugin()
