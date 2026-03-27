import aiohttp
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from helpers import redis_client
from verba_base import ToolVerba
from verba_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from verba_result import action_failure, action_success

logger = logging.getLogger("premiumize_add_transfer")
logger.setLevel(logging.INFO)


class PremiumizeAddTransferPlugin(ToolVerba):
    name = "premiumize_add_transfer"
    verba_name = "Premiumize Add Transfer"
    version = "1.0.0"
    min_tater_version = "59"
    pretty_name = "Premiumize Add Transfer"
    settings_category = "Premiumize"
    tags = ["premiumize", "add_transfer"]
    fixed_action = "add_transfer"
    usage = (
        '{"function":"premiumize_add_transfer","arguments":{"query":"add this to Premiumize magnet:?xt=urn:btih:0000000000000000000000000000000000000000"}}'
    )
    description = (
        "Add a magnet or URL as a new Premiumize transfer."
    )
    verba_dec = (
        "Create Premiumize transfers from explicit magnet or HTTP(S) source links."
    )
    waiting_prompt_template = (
        "Tell {mention} you are checking Premiumize now and will report transfer status or links shortly. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "macos", "irc", "matrix", "telegram", "homeassistant", "homekit", "xbmc"]
    required_settings = {
        "PREMIUMIZE_API_KEY": {
            "label": "Premiumize API Key",
            "type": "password",
            "default": "",
            "description": "Your Premiumize.me API key.",
        },
        "PREMIUMIZE_API_BASE": {
            "label": "Premiumize API Base",
            "type": "string",
            "default": "https://www.premiumize.me/api",
            "description": "Premiumize API base URL.",
        },
        "PREMIUMIZE_TIMEOUT_SECONDS": {
            "label": "HTTP Timeout Seconds",
            "type": "string",
            "default": "30",
            "description": "Timeout for Premiumize API requests.",
        },
    }
    when_to_use = (
        "Use when the request is to add/send/download a magnet or URL in Premiumize."
    )
    how_to_use = (
        "Set `query` to an add-transfer request and include the full literal `magnet:?` or `http(s)://...` URI."
    )
    common_needs = [
        "An add-transfer request in query.",
        "The exact `magnet:?` or `http(s)://` URI.",
    ]
    missing_info_prompts = ["What exact magnet or URL should I send to Premiumize?"]
    example_calls = [
        '{"function":"premiumize_add_transfer","arguments":{"query":"add this to Premiumize magnet:?xt=urn:btih:0000000000000000000000000000000000000000"}}',
        '{"function":"premiumize_add_transfer","arguments":{"query":"send this URL to Premiumize https://example.com/file.torrent"}}',
    ]
    routing_keywords = [
        "premiumize",
        "add",
        "send",
        "download",
        "transfer",
        "magnet",
        "torrent",
    ]
    argument_schema = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The add-transfer request containing a full magnet:? or http(s):// URI.",
            },
            "source": {
                "type": "string",
                "description": "Optional explicit source URI (magnet or URL).",
            },
            "src": {
                "type": "string",
                "description": "Alias of source URI.",
            },
            "url": {
                "type": "string",
                "description": "Optional HTTP(S) source URI.",
            },
            "magnet": {
                "type": "string",
                "description": "Optional magnet URI.",
            },
            "link": {
                "type": "string",
                "description": "Optional source URI alias.",
            },
        },
        "required": [],
    }

    CACHE_KEY = "tater:premiumize:last_context"
    CACHE_TTL_SECONDS = 6 * 60 * 60
    SOURCE_URL_RE = re.compile(r"(https?://[^\s\"'<>]+)", re.IGNORECASE)
    SOURCE_MAGNET_RE = re.compile(r"(magnet:\?[^\s\"'<>]+)", re.IGNORECASE)

    def _diagnosis(self) -> Dict[str, str]:
        hash_diag = diagnose_hash_fields(
            "verba_settings:Premiumize",
            fields={
                "premiumize_api_key": "PREMIUMIZE_API_KEY",
                "premiumize_api_base": "PREMIUMIZE_API_BASE",
            },
            validators={
                "premiumize_api_key": lambda v: len(str(v or "").strip()) >= 10,
                "premiumize_api_base": lambda v: str(v or "").strip().startswith(("http://", "https://")),
            },
        )
        key_diag = diagnose_redis_keys(
            keys={"premiumize_api_key": "tater:premiumize:api_key"},
            validators={"premiumize_api_key": lambda v: len(str(v or "").strip()) >= 10},
        )
        return combine_diagnosis(hash_diag, key_diag)

    @staticmethod
    def _safe_text(value: Any, default: str = "") -> str:
        if value is None:
            return default
        if isinstance(value, (bytes, bytearray)):
            try:
                return value.decode("utf-8", "ignore").strip()
            except Exception:
                return default
        return str(value).strip() or default

    @staticmethod
    def _clamp_int(value: Any, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            return default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        return [tok for tok in re.findall(r"[a-z0-9]+", str(text or "").lower()) if len(tok) >= 2]

    @staticmethod
    def _slug(text: Any) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())

    @staticmethod
    def _normalize_action(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"add_transfer", "add", "download", "send", "create_transfer", "transfer"}:
            return "add_transfer"
        if text in {"list_transfers", "transfers", "list", "show_transfers"}:
            return "list_transfers"
        if text in {"check_transfer", "status", "progress", "check_status"}:
            return "check_transfer"
        if text in {"list_files", "files", "browse", "cloud_files", "folder"}:
            return "list_files"
        if text in {"get_links", "links", "link", "download_link", "stream_link"}:
            return "get_links"
        return ""

    @staticmethod
    def _normalize_status_filter(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"active", "running", "queued", "in_progress"}:
            return "active"
        if text in {"finished", "complete", "completed", "done", "success"}:
            return "finished"
        if text in {"failed", "error"}:
            return "failed"
        if text in {"all", "any"}:
            return "all"
        return ""

    def _settings(self) -> Dict[str, Any]:
        raw = redis_client.hgetall("verba_settings:Premiumize") or {}
        api_key = self._safe_text(raw.get("PREMIUMIZE_API_KEY"), "")
        api_base = self._safe_text(raw.get("PREMIUMIZE_API_BASE"), "https://www.premiumize.me/api").rstrip("/")
        try:
            timeout_seconds = float(self._safe_text(raw.get("PREMIUMIZE_TIMEOUT_SECONDS"), "30") or "30")
        except Exception:
            timeout_seconds = 30.0
        timeout_seconds = float(max(8.0, min(120.0, timeout_seconds)))
        return {"api_key": api_key, "api_base": api_base, "timeout_seconds": timeout_seconds}

    @staticmethod
    def _query_from_args(args: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
        data = args or {}
        for key in ("query", "request", "text", "message", "content", "prompt", "instruction"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        origin = data.get("origin")
        if isinstance(origin, dict):
            for key in ("request_text", "query", "text", "content", "message"):
                value = origin.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

        return ""

    def _extract_first_source(self, text: str) -> str:
        query = str(text or "")
        m = self.SOURCE_MAGNET_RE.search(query)
        if m:
            return m.group(1).strip()
        m = self.SOURCE_URL_RE.search(query)
        if m:
            return m.group(1).strip()
        return ""

    @staticmethod
    def _source_type(source: str) -> str:
        src = str(source or "").strip().lower()
        if src.startswith("magnet:?"):
            return "magnet"
        if src.startswith("http://") or src.startswith("https://"):
            return "url"
        return "unknown"

    def _explicit_source_from_args(self, args: Dict[str, Any]) -> str:
        data = args or {}
        for key in ("source", "src", "url", "magnet", "link"):
            value = self._safe_text(data.get(key))
            if not value:
                continue
            extracted = self._extract_first_source(value)
            if extracted:
                return extracted
            if self._source_type(value) != "unknown":
                return value
        return ""

    @staticmethod
    def _first_match(text: str, patterns: List[str]) -> str:
        for pattern in patterns:
            m = re.search(pattern, text or "", flags=re.IGNORECASE)
            if m:
                return str(m.group(1) or "").strip()
        return ""

    def _infer_status_filter_from_query(self, query: str) -> str:
        text = str(query or "").lower()
        if any(token in text for token in ("active", "running", "queued", "in progress", "downloading")):
            return "active"
        if any(token in text for token in ("finished", "complete", "completed", "done", "success")):
            return "finished"
        if any(token in text for token in ("failed", "error")):
            return "failed"
        if "all" in text:
            return "all"
        return ""

    def _extract_index_from_query(self, query: str) -> int:
        raw = self._first_match(
            query,
            [
                r"\b(?:item|file|transfer|entry)\s*#?\s*(\d{1,4})\b",
                r"\b(?:number|index)\s*(\d{1,4})\b",
                r"\b(\d{1,4})(?:st|nd|rd|th)\b",
            ],
        )
        return self._clamp_int(raw, 0, 500, 0)

    def _extract_transfer_id_from_query(self, query: str) -> str:
        return self._first_match(
            query,
            [
                r"\btransfer(?:\s+id)?\s*[:#]?\s*([a-zA-Z0-9_-]{4,})\b",
                r"\bid\s*[:#]?\s*([a-zA-Z0-9_-]{6,})\b",
            ],
        )

    def _extract_folder_id_from_query(self, query: str) -> str:
        return self._first_match(
            query,
            [
                r"\bfolder(?:\s+id)?\s*[:#]?\s*([a-zA-Z0-9_-]{4,})\b",
            ],
        )

    def _extract_item_id_from_query(self, query: str) -> str:
        return self._first_match(
            query,
            [
                r"\bitem(?:\s+id)?\s*[:#]?\s*([a-zA-Z0-9_-]{4,})\b",
                r"\bfile(?:\s+id)?\s*[:#]?\s*([a-zA-Z0-9_-]{4,})\b",
            ],
        )

    async def _resolve_intent(self, query: str, args: Dict[str, Any], llm_client) -> Dict[str, Any]:
        _ = llm_client
        action = self._normalize_action(getattr(self, "fixed_action", "")) or self._normalize_action((args or {}).get("action"))
        status_filter = self._normalize_status_filter((args or {}).get("status_filter")) or self._infer_status_filter_from_query(
            query
        )

        source = self._extract_first_source(query)
        if not source:
            source = self._safe_text((args or {}).get("source"))
            source = self._extract_first_source(source) or source

        source_type = self._source_type(source) if source else ""
        name_query = self._safe_text((args or {}).get("name_query"))
        transfer_id = self._safe_text((args or {}).get("transfer_id")) or self._extract_transfer_id_from_query(query)
        folder_id = self._safe_text((args or {}).get("folder_id")) or self._extract_folder_id_from_query(query)
        item_id = self._safe_text((args or {}).get("item_id")) or self._extract_item_id_from_query(query)
        index = self._clamp_int((args or {}).get("index"), 0, 500, self._extract_index_from_query(query))

        # Accept explicit source args but do not force route changes; action is fixed by this dedicated verba.
        explicit_source = self._explicit_source_from_args(args or {})
        if explicit_source and not source:
            source = explicit_source
            source_type = self._source_type(source)

        return {
            "action": action,
            "status_filter": status_filter or "all",
            "source": source,
            "source_type": source_type,
            "name_query": name_query,
            "transfer_id": transfer_id,
            "folder_id": folder_id,
            "item_id": item_id,
            "index": index,
        }

    def _load_cache(self) -> Dict[str, Any]:
        try:
            raw = redis_client.get(self.CACHE_KEY)
            if not raw:
                return {}
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_cache(self, updates: Dict[str, Any]) -> None:
        cache = self._load_cache()
        cache.update(updates or {})
        cache["updated_at"] = datetime.now(timezone.utc).isoformat()
        cache["last_transfers"] = list(cache.get("last_transfers") or [])[:40]
        cache["last_files"] = list(cache.get("last_files") or [])[:80]
        try:
            redis_client.set(self.CACHE_KEY, json.dumps(cache, ensure_ascii=False), ex=self.CACHE_TTL_SECONDS)
        except Exception as exc:
            logger.debug("[premiumize] cache save skipped: %s", exc)

    async def _resolve_source(self, args: Dict[str, Any], intent: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        src = self._safe_text(intent.get("source"))
        if src:
            return {"source": src, "source_type": intent.get("source_type") or self._source_type(src)}

        explicit = self._explicit_source_from_args(args or {})
        if explicit:
            return {"source": explicit, "source_type": self._source_type(explicit)}

        return {}

    async def _pm_request(
        self,
        *,
        method: str,
        endpoint: str,
        settings: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        url = f"{settings['api_base'].rstrip('/')}/{endpoint.lstrip('/')}"
        params = dict(params or {})
        data = dict(data or {})
        params["apikey"] = settings["api_key"]
        data["apikey"] = settings["api_key"]

        timeout = aiohttp.ClientTimeout(total=settings["timeout_seconds"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                if method.upper() == "GET":
                    async with session.get(url, params=params) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            return None, f"Premiumize API HTTP {resp.status}: {text[:220]}"
                else:
                    async with session.post(url, data=data) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            return None, f"Premiumize API HTTP {resp.status}: {text[:220]}"
            except Exception as exc:
                return None, f"Premiumize request failed: {exc}"

        try:
            payload = json.loads(text)
            if not isinstance(payload, dict):
                return None, "Premiumize returned a non-object JSON response."
            return payload, None
        except Exception:
            return None, f"Premiumize returned non-JSON data: {text[:220]}"

    @staticmethod
    def _api_success(payload: Dict[str, Any]) -> bool:
        status = str(payload.get("status") or "").strip().lower()
        if not status:
            return True
        return status == "success"

    @staticmethod
    def _api_error(payload: Dict[str, Any], default_message: str) -> str:
        message = str(payload.get("message") or payload.get("error") or "").strip()
        return message or default_message

    async def _api_transfer_create(self, settings: Dict[str, Any], source: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        payload, err = await self._pm_request(method="POST", endpoint="transfer/create", settings=settings, data={"src": source})
        if err:
            return None, err
        if not self._api_success(payload):
            return None, self._api_error(payload, "Premiumize transfer/create failed.")
        return payload, None

    async def _api_transfer_list(self, settings: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        payload, err = await self._pm_request(method="GET", endpoint="transfer/list", settings=settings)
        if err:
            return [], err
        if not self._api_success(payload):
            return [], self._api_error(payload, "Premiumize transfer/list failed.")
        raw = payload.get("transfers") or payload.get("content") or payload.get("items") or []
        if not isinstance(raw, list):
            raw = []
        return [x for x in raw if isinstance(x, dict)], None

    async def _api_folder_list(self, settings: Dict[str, Any], folder_id: str = "") -> Tuple[Dict[str, Any], Optional[str]]:
        params = {"id": folder_id} if folder_id else {}
        payload, err = await self._pm_request(method="GET", endpoint="folder/list", settings=settings, params=params)
        if err:
            return {}, err
        if not self._api_success(payload):
            return {}, self._api_error(payload, "Premiumize folder/list failed.")
        return payload, None

    async def _api_item_details(self, settings: Dict[str, Any], item_id: str) -> Tuple[Dict[str, Any], Optional[str]]:
        payload, err = await self._pm_request(method="GET", endpoint="item/details", settings=settings, params={"id": item_id})
        if err:
            return {}, err
        if not self._api_success(payload):
            return {}, self._api_error(payload, "Premiumize item/details failed.")
        return payload, None

    async def _api_directdl(self, settings: Dict[str, Any], source: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        payload, err = await self._pm_request(method="POST", endpoint="transfer/directdl", settings=settings, data={"src": source})
        if err:
            return [], err
        if not self._api_success(payload):
            return [], self._api_error(payload, "Premiumize transfer/directdl failed.")
        content = payload.get("content") or []
        if not isinstance(content, list):
            content = []
        return [x for x in content if isinstance(x, dict)], None

    @staticmethod
    def _coerce_progress(raw: Any) -> Optional[float]:
        if raw is None:
            return None
        text = str(raw).strip().lower().replace("%", "")
        try:
            value = float(text)
        except Exception:
            return None
        if 0 <= value <= 1:
            value *= 100.0
        value = max(0.0, min(100.0, value))
        return round(value, 2)

    @staticmethod
    def _status_bucket(status: str, progress: Optional[float]) -> str:
        s = str(status or "").strip().lower()
        if any(x in s for x in ("error", "fail")):
            return "failed"
        if any(x in s for x in ("finished", "complete", "done", "success")):
            return "finished"
        if any(x in s for x in ("queued", "running", "download", "wait", "active", "seed")):
            return "active"
        if progress is not None and progress >= 100:
            return "finished"
        return "active"

    def _normalize_transfer(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        transfer_id = self._safe_text(raw.get("id") or raw.get("transfer_id"))
        name = (
            self._safe_text(raw.get("name"))
            or self._safe_text(raw.get("filename"))
            or self._safe_text(raw.get("src"))
            or f"transfer_{transfer_id or 'unknown'}"
        )
        progress = self._coerce_progress(raw.get("progress") or raw.get("percent_done") or raw.get("percentage"))
        status_raw = self._safe_text(raw.get("status") or raw.get("state") or "")
        status = self._status_bucket(status_raw, progress)
        return {
            "transfer_id": transfer_id,
            "name": name,
            "status": status,
            "status_raw": status_raw,
            "progress": progress,
            "eta": raw.get("eta"),
            "source": self._safe_text(raw.get("src")),
            "folder_id": self._safe_text(raw.get("folder_id") or raw.get("folderId")),
            "file_id": self._safe_text(raw.get("file_id") or raw.get("fileId")),
            "size": raw.get("size"),
            "message": self._safe_text(raw.get("message") or raw.get("error")),
            "raw": raw,
        }

    def _normalize_file_item(self, raw: Dict[str, Any], parent_path: str = "") -> Dict[str, Any]:
        name = self._safe_text(raw.get("name"), "")
        item_id = self._safe_text(raw.get("id") or raw.get("item_id"), "")
        type_text = self._safe_text(raw.get("type"), "")
        is_folder = bool(raw.get("is_dir") or raw.get("folder")) or type_text.lower() == "folder"
        path = self._safe_text(raw.get("path"))
        if not path:
            path = f"{parent_path}/{name}".strip("/") if name else parent_path.strip("/")
        return {
            "item_id": item_id,
            "name": name or item_id or "item",
            "path": path,
            "is_folder": is_folder,
            "size": raw.get("size"),
            "modified": self._safe_text(raw.get("modified") or raw.get("created_at") or raw.get("created")),
            "download_link": self._safe_text(raw.get("link") or raw.get("url")),
            "stream_link": self._safe_text(raw.get("stream_link") or raw.get("stream")),
            "type": "folder" if is_folder else "file",
            "raw": raw,
        }

    def _extract_folder_items(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates = []
        content = payload.get("content")
        if isinstance(content, list):
            candidates = content
        elif isinstance(payload.get("folder"), dict):
            folder_content = payload.get("folder", {}).get("content")
            if isinstance(folder_content, list):
                candidates = folder_content
        elif isinstance(payload.get("item"), dict):
            item_content = payload.get("item", {}).get("content")
            if isinstance(item_content, list):
                candidates = item_content
        elif isinstance(payload.get("files"), list):
            candidates = payload.get("files")
        elif isinstance(payload.get("items"), list):
            candidates = payload.get("items")

        out = []
        for entry in candidates or []:
            if not isinstance(entry, dict):
                continue
            out.append(self._normalize_file_item(entry))
        return out

    def _normalize_direct_links(self, content: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for entry in content or []:
            if not isinstance(entry, dict):
                continue
            path = self._safe_text(entry.get("path"), "")
            link = self._safe_text(entry.get("link") or entry.get("url"), "")
            stream = self._safe_text(entry.get("stream_link") or entry.get("stream"), "")
            if not link and not stream:
                continue
            out.append(
                {
                    "name": path or self._safe_text(entry.get("name"), "file"),
                    "path": path,
                    "download_link": link,
                    "stream_link": stream,
                    "size": entry.get("size"),
                }
            )
        return out

    def _filter_transfers(self, transfers: List[Dict[str, Any]], status_filter: str, name_query: str) -> List[Dict[str, Any]]:
        results = list(transfers or [])
        if status_filter and status_filter != "all":
            results = [t for t in results if t.get("status") == status_filter]
        nq_tokens = self._tokenize(name_query)
        if nq_tokens:
            filtered: List[Dict[str, Any]] = []
            for t in results:
                hay = self._slug(t.get("name")) + self._slug(t.get("source"))
                hits = len([tok for tok in nq_tokens if tok in hay])
                if hits > 0:
                    filtered.append(t)
            results = filtered
        return results

    def _select_transfer(self, transfers: List[Dict[str, Any]], intent: Dict[str, Any], cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        transfer_id = self._safe_text(intent.get("transfer_id"))
        if transfer_id:
            for t in transfers:
                if self._safe_text(t.get("transfer_id")) == transfer_id:
                    return t

        index = self._clamp_int(intent.get("index"), 0, 500, 0)
        if index > 0 and index <= len(transfers):
            return transfers[index - 1]

        name_query = self._safe_text(intent.get("name_query"))
        if name_query:
            tokens = self._tokenize(name_query)
            scored = []
            for t in transfers:
                hay = self._slug(t.get("name")) + self._slug(t.get("source"))
                score = len([tok for tok in tokens if tok in hay])
                if score > 0:
                    scored.append((score, t))
            if scored:
                scored.sort(key=lambda x: x[0], reverse=True)
                return scored[0][1]

        last_id = self._safe_text(cache.get("last_transfer_id"))
        if last_id:
            for t in transfers:
                if self._safe_text(t.get("transfer_id")) == last_id:
                    return t

        for t in transfers:
            if t.get("status") == "active":
                return t
        return transfers[0] if transfers else None

    def _select_file(self, files: List[Dict[str, Any]], intent: Dict[str, Any], cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        item_id = self._safe_text(intent.get("item_id"))
        if item_id:
            for f in files:
                if self._safe_text(f.get("item_id")) == item_id:
                    return f

        index = self._clamp_int(intent.get("index"), 0, 500, 0)
        if index > 0 and index <= len(files):
            return files[index - 1]

        name_query = self._safe_text(intent.get("name_query"))
        if name_query:
            tokens = self._tokenize(name_query)
            scored = []
            for f in files:
                hay = self._slug(f.get("name")) + self._slug(f.get("path"))
                score = len([tok for tok in tokens if tok in hay])
                if score > 0:
                    scored.append((score, f))
            if scored:
                scored.sort(key=lambda x: x[0], reverse=True)
                return scored[0][1]

        last_item_id = self._safe_text(cache.get("last_item_id"))
        if last_item_id:
            for f in files:
                if self._safe_text(f.get("item_id")) == last_item_id:
                    return f

        for f in files:
            if not f.get("is_folder"):
                return f
        return files[0] if files else None

    async def _act_add_transfer(
        self,
        *,
        args: Dict[str, Any],
        intent: Dict[str, Any],
        settings: Dict[str, Any],
        context: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        source_info = await self._resolve_source(args, intent, context)
        source = self._safe_text(source_info.get("source"))
        source_type = self._safe_text(source_info.get("source_type"), "unknown")
        if not source:
            return action_failure(
                code="missing_source",
                message="No explicit magnet or URL was provided for Premiumize transfer.",
                needs=["Pass the full magnet:? or http(s):// URI directly in query or source arguments."],
                say_hint="Explain that a literal source URI is required and paraphrases like 'this magnet' are not valid.",
            )

        created, create_err = await self._api_transfer_create(settings, source)
        if create_err:
            return action_failure(
                code="premiumize_transfer_create_failed",
                message=create_err,
                diagnosis=self._diagnosis(),
                say_hint="Explain transfer creation failed and ask whether to retry.",
            )

        transfer_id = self._safe_text(created.get("id") or created.get("transfer_id"))
        transfer_name = self._safe_text(created.get("name") or created.get("filename"))
        links, link_err = await self._api_directdl(settings, source)
        normalized_links = self._normalize_direct_links(links) if not link_err else []

        self._save_cache(
            {
                "last_action": "add_transfer",
                "last_source": {"source": source, "source_type": source_type},
                "last_transfer_id": transfer_id,
                "last_transfer_name": transfer_name,
            }
        )

        summary = f"Sent {source_type or 'source'} to Premiumize."
        if transfer_id:
            summary += f" Transfer id: {transfer_id}."
        if normalized_links:
            summary += f" Direct links are already available for {len(normalized_links)} file(s)."

        return action_success(
            facts={
                "action": "add_transfer",
                "source": source,
                "source_type": source_type,
                "transfer_id": transfer_id,
            },
            data={
                "source": source_info,
                "transfer_create": created,
                "transfer_id": transfer_id,
                "transfer_name": transfer_name,
                "direct_links": normalized_links,
                "directdl_error": link_err or "",
            },
            summary_for_user=summary,
            say_hint=(
                "Confirm the transfer was sent to Premiumize, include transfer id when present, "
                "and mention if direct links are already available."
            ),
            suggested_followups=[
                "Want me to check transfer progress now?",
                "Want me to list your Premiumize cloud files next?",
            ],
        )

    async def _act_list_transfers(self, *, intent: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
        raw_transfers, err = await self._api_transfer_list(settings)
        if err:
            return action_failure(
                code="premiumize_transfer_list_failed",
                message=err,
                diagnosis=self._diagnosis(),
                say_hint="Explain Premiumize transfer listing failed and suggest retrying shortly.",
            )

        transfers = [self._normalize_transfer(x) for x in raw_transfers]
        filtered = self._filter_transfers(transfers, intent.get("status_filter") or "all", intent.get("name_query") or "")

        active_count = len([t for t in transfers if t.get("status") == "active"])
        finished_count = len([t for t in transfers if t.get("status") == "finished"])
        failed_count = len([t for t in transfers if t.get("status") == "failed"])

        self._save_cache(
            {
                "last_action": "list_transfers",
                "last_transfers": transfers,
                "last_transfer_id": filtered[0].get("transfer_id") if filtered else "",
                "last_transfer_name": filtered[0].get("name") if filtered else "",
            }
        )

        summary = (
            f"Premiumize has {len(transfers)} transfer(s): "
            f"{active_count} active, {finished_count} finished, {failed_count} failed."
        )
        has_filter = (intent.get("status_filter") or "all") != "all" or bool(intent.get("name_query"))
        if has_filter:
            summary += f" {len(filtered)} match your filter."

        return action_success(
            facts={
                "action": "list_transfers",
                "transfer_count": len(transfers),
                "active_count": active_count,
                "finished_count": finished_count,
                "failed_count": failed_count,
                "filtered_count": len(filtered),
            },
            data={
                "transfers": filtered,
                "all_transfers": transfers,
                "status_filter": intent.get("status_filter") or "all",
                "name_query": intent.get("name_query") or "",
            },
            summary_for_user=summary,
            say_hint="Summarize transfer counts and show relevant active or requested transfers with progress.",
        )

    async def _act_check_transfer(self, *, intent: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
        raw_transfers, err = await self._api_transfer_list(settings)
        if err:
            return action_failure(
                code="premiumize_transfer_status_failed",
                message=err,
                diagnosis=self._diagnosis(),
                say_hint="Explain transfer status check failed and suggest retrying shortly.",
            )

        transfers = [self._normalize_transfer(x) for x in raw_transfers]
        cache = self._load_cache()
        target = self._select_transfer(transfers, intent, cache)
        if not target:
            return action_failure(
                code="no_transfers_found",
                message="No Premiumize transfers were found.",
                say_hint="Explain there are currently no transfers.",
            )

        files: List[Dict[str, Any]] = []
        folder_id = self._safe_text(target.get("folder_id"))
        if folder_id:
            folder_payload, folder_err = await self._api_folder_list(settings, folder_id)
            if not folder_err:
                files = self._extract_folder_items(folder_payload)

        self._save_cache(
            {
                "last_action": "check_transfer",
                "last_transfers": transfers,
                "last_transfer_id": target.get("transfer_id"),
                "last_transfer_name": target.get("name"),
                "last_folder_id": folder_id,
                "last_files": files[:80],
            }
        )

        progress = target.get("progress")
        summary = f"Transfer '{target.get('name')}' is {target.get('status')}."
        if progress is not None:
            summary += f" Progress: {progress}%."
        if target.get("status") == "finished" and files:
            ready = len([f for f in files if f.get("download_link") or f.get("stream_link")])
            summary += f" {ready} file(s) have links ready."

        return action_success(
            facts={
                "action": "check_transfer",
                "transfer_id": target.get("transfer_id"),
                "name": target.get("name"),
                "status": target.get("status"),
                "progress": progress,
                "file_count": len(files),
            },
            data={
                "transfer": target,
                "files": files,
            },
            summary_for_user=summary,
            say_hint="Report transfer status and progress; if finished, mention available file links.",
        )

    async def _act_list_files(self, *, intent: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
        cache = self._load_cache()
        folder_id = self._safe_text(intent.get("folder_id") or cache.get("last_folder_id"))

        if not folder_id and intent.get("name_query"):
            raw_transfers, err = await self._api_transfer_list(settings)
            if not err:
                transfers = [self._normalize_transfer(x) for x in raw_transfers]
                target = self._select_transfer(transfers, intent, cache)
                if target:
                    folder_id = self._safe_text(target.get("folder_id"))

        payload, err = await self._api_folder_list(settings, folder_id)
        if err:
            return action_failure(
                code="premiumize_folder_list_failed",
                message=err,
                diagnosis=self._diagnosis(),
                say_hint="Explain Premiumize file browsing failed and suggest retrying.",
            )

        files = self._extract_folder_items(payload)
        if intent.get("name_query"):
            tokens = self._tokenize(intent.get("name_query"))
            if tokens:
                files = [
                    f for f in files
                    if len([tok for tok in tokens if tok in (self._slug(f.get("name")) + self._slug(f.get("path")))]) > 0
                ]

        self._save_cache(
            {
                "last_action": "list_files",
                "last_folder_id": folder_id,
                "last_files": files[:80],
                "last_item_id": files[0].get("item_id") if files else "",
            }
        )

        folder_count = len([f for f in files if f.get("is_folder")])
        file_count = len(files) - folder_count
        summary = f"Premiumize folder has {file_count} file(s) and {folder_count} folder(s)."
        if not files:
            summary = "No Premiumize files were found for that folder/filter."

        return action_success(
            facts={
                "action": "list_files",
                "folder_id": folder_id,
                "entry_count": len(files),
                "file_count": file_count,
                "folder_count": folder_count,
            },
            data={
                "folder_id": folder_id,
                "files": files,
                "name_query": intent.get("name_query") or "",
            },
            summary_for_user=summary,
            say_hint="Summarize available cloud files/folders and include notable link-ready items.",
        )

    async def _act_get_links(
        self,
        *,
        args: Dict[str, Any],
        intent: Dict[str, Any],
        settings: Dict[str, Any],
        context: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        source_info = await self._resolve_source(args, intent, context)
        source = self._safe_text(source_info.get("source"))
        if source:
            links, err = await self._api_directdl(settings, source)
            if err:
                return action_failure(
                    code="premiumize_directdl_failed",
                    message=err,
                    diagnosis=self._diagnosis(),
                    say_hint="Explain direct link retrieval failed and suggest retrying.",
                )
            normalized_links = self._normalize_direct_links(links)
            if not normalized_links:
                return action_failure(
                    code="no_links_available",
                    message="Premiumize did not return direct links for that source yet.",
                    say_hint="Explain no direct links are available yet and suggest checking transfer progress.",
                )
            self._save_cache({"last_action": "get_links", "last_files": normalized_links[:80]})
            summary = f"Found {len(normalized_links)} direct link(s) from Premiumize."
            return action_success(
                facts={
                    "action": "get_links",
                    "source_type": source_info.get("source_type"),
                    "link_count": len(normalized_links),
                },
                data={"links": normalized_links, "source": source_info},
                summary_for_user=summary,
                say_hint="Return the direct/stream links and identify the best link-ready file.",
            )

        cache = self._load_cache()
        files = [f for f in cache.get("last_files") or [] if isinstance(f, dict)]
        target = self._select_file(files, intent, cache) if files else None

        if target and target.get("item_id") and not (target.get("download_link") or target.get("stream_link")):
            payload, err = await self._api_item_details(settings, self._safe_text(target.get("item_id")))
            if not err:
                item = payload.get("item") if isinstance(payload.get("item"), dict) else payload
                if isinstance(item, dict):
                    target = self._normalize_file_item(item)

        if not target:
            return action_failure(
                code="link_target_not_found",
                message="No Premiumize file or source could be resolved for link retrieval.",
                needs=["Provide a file name/item id, or pass the exact magnet/URL URI in the request."],
                say_hint="Explain a specific file target or literal source URI is required to fetch links.",
            )

        self._save_cache(
            {
                "last_action": "get_links",
                "last_item_id": target.get("item_id"),
                "last_files": files[:80] if files else [target],
            }
        )

        summary = f"Retrieved links for '{target.get('name')}'."
        if target.get("stream_link"):
            summary += " Stream link is available."
        elif target.get("download_link"):
            summary += " Download link is available."
        else:
            summary += " No direct link found yet."

        return action_success(
            facts={
                "action": "get_links",
                "item_id": target.get("item_id"),
                "name": target.get("name"),
                "has_download_link": bool(target.get("download_link")),
                "has_stream_link": bool(target.get("stream_link")),
            },
            data={"file": target},
            summary_for_user=summary,
            say_hint="Return the requested file links and mention stream vs direct availability.",
        )

    async def _run(self, args: Dict[str, Any], llm_client, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        settings = self._settings()
        diagnosis = self._diagnosis()
        if not settings["api_key"]:
            needs = needs_from_diagnosis(
                diagnosis,
                {"premiumize_api_key": "Please set your Premiumize API key in plugin settings."},
            )
            return action_failure(
                code="premiumize_config_missing",
                message="Premiumize API key is not configured.",
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain missing Premiumize API key and ask to configure it.",
            )

        query = self._query_from_args(args or {}, context=context)
        explicit_source = self._explicit_source_from_args(args or {})
        if not query and explicit_source:
            query = explicit_source
        if not query:
            return action_failure(
                code="missing_request",
                message="No Premiumize request text was provided.",
                needs=["Provide a Premiumize request. For transfers/links by source, include the exact magnet or URL URI."],
                say_hint="Ask for a concrete Premiumize request and require a literal magnet/URL URI when needed.",
            )

        intent = await self._resolve_intent(query, args or {}, llm_client)

        action = self._safe_text(intent.get("action"))
        if not action:
            return action_failure(
                code="intent_unresolved",
                message="Could not determine the Premiumize action from the request.",
                needs=[
                    "Say one clear action: add transfer, list transfers, check transfer status, list files, or get links.",
                ],
                say_hint="Explain the request could not be routed and ask for a clearer Premiumize action.",
            )

        if action == "add_transfer":
            return await self._act_add_transfer(args=args or {}, intent=intent, settings=settings, context=context)
        if action == "list_transfers":
            return await self._act_list_transfers(intent=intent, settings=settings)
        if action == "check_transfer":
            return await self._act_check_transfer(intent=intent, settings=settings)
        if action == "list_files":
            return await self._act_list_files(intent=intent, settings=settings)
        if action == "get_links":
            return await self._act_get_links(args=args or {}, intent=intent, settings=settings, context=context)

        return action_failure(
            code="unsupported_intent",
            message=f"Unsupported Premiumize intent '{action}'.",
            say_hint="Explain that request intent could not be mapped and ask for a clearer Premiumize task.",
        )

    # Platform handlers
    async def handle_discord(self, message, args, llm_client):
        ctx = {}
        try:
            content = getattr(message, "content", None)
            if isinstance(content, str) and content.strip():
                ctx["request_text"] = content.strip()
            channel = getattr(message, "channel", None)
            channel_id = getattr(channel, "id", None)
            if channel_id is not None:
                ctx["channel_id"] = str(channel_id)
        except Exception:
            pass
        try:
            return await self._run(args or {}, llm_client, context=ctx)
        except Exception as exc:
            logger.exception("[premiumize handle_discord] %s", exc)
            return action_failure(code="premiumize_exception", message=f"Premiumize request failed: {exc}")

    async def handle_webui(self, args, llm_client):
        try:
            return await self._run(args or {}, llm_client, context={})
        except Exception as exc:
            logger.exception("[premiumize handle_webui] %s", exc)
            return action_failure(code="premiumize_exception", message=f"Premiumize request failed: {exc}")

    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self._run(args or {}, llm_client, context=context or {})
        except Exception as exc:
            logger.exception("[premiumize handle_macos] %s", exc)
            return action_failure(code="premiumize_exception", message=f"Premiumize request failed: {exc}")

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        ctx = {}
        if isinstance(raw_message, str) and raw_message.strip():
            ctx["request_text"] = raw_message.strip()
        try:
            return await self._run(args or {}, llm_client, context=ctx)
        except Exception as exc:
            logger.exception("[premiumize handle_irc] %s", exc)
            return action_failure(code="premiumize_exception", message=f"Premiumize request failed: {exc}")

    async def handle_homeassistant(self, args, llm_client):
        try:
            return await self._run(args or {}, llm_client, context={})
        except Exception as exc:
            logger.exception("[premiumize handle_homeassistant] %s", exc)
            return action_failure(code="premiumize_exception", message="Premiumize request failed.")

    async def handle_homekit(self, args, llm_client):
        try:
            return await self._run(args or {}, llm_client, context={})
        except Exception as exc:
            logger.exception("[premiumize handle_homekit] %s", exc)
            return action_failure(code="premiumize_exception", message="Premiumize request failed.")

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        ctx = {}
        if isinstance(body, str) and body.strip():
            ctx["request_text"] = body.strip()
        room_id = getattr(room, "room_id", None)
        if room_id is not None:
            ctx["channel_id"] = str(room_id)
        try:
            return await self._run(args or {}, llm_client, context=ctx)
        except Exception as exc:
            logger.exception("[premiumize handle_matrix] %s", exc)
            return action_failure(code="premiumize_exception", message=f"Premiumize request failed: {exc}")

    async def handle_telegram(self, update, args, llm_client):
        ctx = {}
        try:
            if isinstance(update, dict):
                msg = update.get("message") or {}
                text = msg.get("text") or msg.get("caption") or ""
                if isinstance(text, str) and text.strip():
                    ctx["request_text"] = text.strip()
        except Exception:
            pass
        try:
            return await self._run(args or {}, llm_client, context=ctx)
        except Exception as exc:
            logger.exception("[premiumize handle_telegram] %s", exc)
            return action_failure(code="premiumize_exception", message=f"Premiumize request failed: {exc}")

    async def handle_xbmc(self, args, llm_client):
        try:
            return await self._run(args or {}, llm_client, context={})
        except Exception as exc:
            logger.exception("[premiumize handle_xbmc] %s", exc)
            return action_failure(code="premiumize_exception", message="Premiumize request failed.")


verba = PremiumizeAddTransferPlugin()
