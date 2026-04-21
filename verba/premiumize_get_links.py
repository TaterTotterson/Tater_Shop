import aiohttp
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlsplit

from helpers import redis_blob_client, redis_client
from verba_base import ToolVerba
from verba_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from verba_result import action_failure, action_success

logger = logging.getLogger("premiumize_get_links")
logger.setLevel(logging.INFO)


class PremiumizeGetLinksPlugin(ToolVerba):
    name = "premiumize_get_links"
    verba_name = "Premiumize Get Links"
    version = "1.0.8"
    min_tater_version = "59"
    pretty_name = "Premiumize Get Links"
    settings_category = "Premiumize"
    tags = ["premiumize", "get_links"]
    fixed_action = "get_links"
    usage = (
        '{"function":"premiumize_get_links","arguments":{"query":"get direct links for magnet:?xt=urn:btih:0000000000000000000000000000000000000000"}}'
    )
    description = (
        "Retrieve Premiumize direct/stream links for cached magnets, remote .torrent URLs, attached .torrent files, or local .torrent file paths."
    )
    verba_dec = (
        "Get Premiumize stream/download links by magnet links, remote HTTP(S) .torrent/torrent download URLs, attached .torrent files, or local .torrent file paths."
    )
    waiting_prompt_template = (
        "Tell {mention} you are checking Premiumize now and will report transfer status or links shortly. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "macos", "irc", "matrix", "telegram"]
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
        "Use when the request is for Premiumize download/stream links from a magnet, a remote .torrent link, an attached .torrent file, or a local .torrent file."
    )
    how_to_use = (
        "Set `query` to a link request and include a full magnet, remote URL, an attached `.torrent` file, or a local `.torrent` file path source."
    )
    common_needs = [
        "A link-retrieval request with a literal magnet, remote URL, attached .torrent file, or local .torrent file path source.",
    ]
    missing_info_prompts = []
    example_calls = [
        '{"function":"premiumize_get_links","arguments":{"query":"get direct links for magnet:?xt=urn:btih:0000000000000000000000000000000000000000"}}',
        '{"function":"premiumize_get_links","arguments":{"query":"get direct links for https://example.com/file.torrent"}}',
        '{"function":"premiumize_get_links","arguments":{"query":"get direct links for /downloads/c_0.torrent"}}',
        '{"function":"premiumize_get_links","arguments":{"query":"get direct links for this attached torrent file"}}',
    ]
    routing_keywords = [
        "premiumize",
        "links",
        "link",
        "download",
        "stream",
        "directdl",
        "torrent",
        "file",
    ]
    argument_schema = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The link request (for example: get links for magnet..., https://.../file.torrent, an attached torrent file, or /downloads/file.torrent).",
            },
            "source": {
                "type": "string",
                "description": "Optional explicit source value (magnet, URL, attached/local .torrent file path).",
            },
            "src": {
                "type": "string",
                "description": "Alias of source value.",
            },
            "url": {
                "type": "string",
                "description": "Optional HTTP(S) source URI, including .torrent links.",
            },
            "magnet": {
                "type": "string",
                "description": "Optional magnet URI.",
            },
            "link": {
                "type": "string",
                "description": "Optional source alias.",
            },
            "torrent_file": {
                "type": "string",
                "description": "Optional local .torrent file path.",
            },
            "torrent_path": {
                "type": "string",
                "description": "Alias of local .torrent file path.",
            },
            "file_path": {
                "type": "string",
                "description": "Optional local .torrent file path using Tater's common exported-artifact field name.",
            },
            "artifact_path": {
                "type": "string",
                "description": "Optional local .torrent file path using Tater's artifact status field name.",
            },
            "item_id": {
                "type": "string",
                "description": "Optional explicit Premiumize item id.",
            },
            "name_query": {
                "type": "string",
                "description": "Optional file name search text.",
            },
            "index": {
                "type": "integer",
                "description": "Optional 1-based file index from cached files.",
            },
        },
        "required": [],
    }

    CACHE_KEY = "tater:premiumize:last_context"
    CACHE_TTL_SECONDS = 6 * 60 * 60
    SOURCE_URL_RE = re.compile(r"(https?://[^\s\"'<>]+)", re.IGNORECASE)
    SOURCE_MAGNET_RE = re.compile(r"(magnet:\?[^\s\"'<>]+)", re.IGNORECASE)
    LOCAL_TORRENT_PATH_RE = re.compile(r"((?:/|\./|\.\./)[^\s\"'<>]+\.torrent)\b", re.IGNORECASE)

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
    def _looks_like_remote_torrent_url(value: str) -> bool:
        text = str(value or "").strip()
        if not text.lower().startswith(("http://", "https://")):
            return False
        parts = urlsplit(text)
        path = (parts.path or "").lower()
        query = (parts.query or "").lower()
        return path.endswith(".torrent") or ".torrent" in query

    @staticmethod
    def _source_type(source: str) -> str:
        src = str(source or "").strip().lower()
        if src.startswith("magnet:?"):
            return "magnet"
        if src.startswith("http://") or src.startswith("https://"):
            return "torrent url" if PremiumizeGetLinksPlugin._looks_like_remote_torrent_url(src) else "url"
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
    def _looks_like_local_torrent_path(value: str) -> bool:
        text = str(value or "").strip()
        if not text or "://" in text or not text.lower().endswith(".torrent"):
            return False
        return text.startswith("/") or text.startswith("./") or text.startswith("../")

    def _extract_first_local_torrent_path(self, text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        m = self.LOCAL_TORRENT_PATH_RE.search(raw)
        if not m:
            return ""
        path = (m.group(1) or "").strip().rstrip(".,);]>")
        return path if self._looks_like_local_torrent_path(path) else ""

    def _explicit_torrent_path_from_args(self, args: Dict[str, Any]) -> str:
        data = args or {}
        for key in ("torrent_file", "torrent_path", "path", "file_path", "artifact_path", "source", "src", "link"):
            value = self._safe_text(data.get(key))
            if not value:
                continue
            extracted = self._extract_first_local_torrent_path(value)
            if extracted:
                return extracted
            if self._looks_like_local_torrent_path(value):
                return value
        return ""

    @staticmethod
    def _blob_client():
        return redis_blob_client

    @classmethod
    def _read_blob_bytes(cls, blob_key: str) -> bytes:
        key = str(blob_key or "").strip()
        if not key:
            return b""
        try:
            raw = cls._blob_client().get(key.encode("utf-8"))
        except Exception:
            return b""
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw)
        return b""

    @staticmethod
    def _artifact_is_torrent(artifact: Dict[str, Any]) -> bool:
        name = str(artifact.get("name") or "").strip().lower()
        if name.endswith(".torrent"):
            return True
        mimetype = str(artifact.get("mimetype") or "").strip().lower()
        if mimetype in {
            "application/x-bittorrent",
            "application/x-torrent",
            "application/torrent",
        }:
            return True
        return False

    @staticmethod
    def _artifact_bytes(artifact: Dict[str, Any]) -> bytes:
        if not isinstance(artifact, dict):
            return b""
        if isinstance(artifact.get("bytes"), (bytes, bytearray)):
            return bytes(artifact.get("bytes"))
        blob_key = str(artifact.get("blob_key") or "").strip()
        if blob_key:
            return PremiumizeGetLinksPlugin._read_blob_bytes(blob_key)
        path_value = str(artifact.get("path") or artifact.get("file_path") or artifact.get("artifact_path") or "").strip()
        if path_value and os.path.isfile(path_value):
            try:
                with open(path_value, "rb") as handle:
                    return handle.read()
            except Exception:
                return b""
        return b""

    @staticmethod
    def _bdecode(data: bytes, idx: int = 0) -> Tuple[Any, int]:
        if idx >= len(data):
            raise ValueError("Unexpected end of bencode data.")
        token = data[idx : idx + 1]
        if token == b"i":
            end = data.index(b"e", idx)
            return int(data[idx + 1 : end]), end + 1
        if token == b"l":
            idx += 1
            out: List[Any] = []
            while data[idx : idx + 1] != b"e":
                item, idx = PremiumizeGetLinksPlugin._bdecode(data, idx)
                out.append(item)
            return out, idx + 1
        if token == b"d":
            idx += 1
            out: Dict[bytes, Any] = {}
            while data[idx : idx + 1] != b"e":
                key, idx = PremiumizeGetLinksPlugin._bdecode(data, idx)
                if not isinstance(key, (bytes, bytearray)):
                    raise ValueError("Invalid bencode dictionary key.")
                value, idx = PremiumizeGetLinksPlugin._bdecode(data, idx)
                out[bytes(key)] = value
            return out, idx + 1
        if token.isdigit():
            colon = data.index(b":", idx)
            size = int(data[idx:colon])
            start = colon + 1
            end = start + size
            return data[start:end], end
        raise ValueError("Invalid bencode token.")

    @staticmethod
    def _flatten_tracker_values(value: Any) -> List[str]:
        trackers: List[str] = []
        if isinstance(value, (bytes, bytearray)):
            text = bytes(value).decode("utf-8", "ignore").strip()
            if text:
                trackers.append(text)
            return trackers
        if isinstance(value, list):
            for item in value:
                trackers.extend(PremiumizeGetLinksPlugin._flatten_tracker_values(item))
        return trackers

    def _parse_torrent_file(self, raw: bytes) -> Tuple[Dict[bytes, Any], bytes, str]:
        try:
            if not raw.startswith(b"d"):
                return {}, b"", "Torrent file did not decode to a dictionary."
            idx = 1
            root: Dict[bytes, Any] = {}
            info_bytes = b""
            while raw[idx : idx + 1] != b"e":
                key, idx = self._bdecode(raw, idx)
                if not isinstance(key, (bytes, bytearray)):
                    return {}, b"", "Invalid torrent dictionary key."
                value_start = idx
                value, idx = self._bdecode(raw, idx)
                key_bytes = bytes(key)
                root[key_bytes] = value
                if key_bytes == b"info":
                    info_bytes = raw[value_start:idx]
            if idx + 1 != len(raw):
                return {}, b"", "Unexpected trailing data."
            if not info_bytes:
                return {}, b"", "Torrent file is missing an info dictionary."
            return root, info_bytes, ""
        except Exception as exc:
            return {}, b"", f"Could not parse torrent file: {exc}"

    def _magnet_from_torrent_bytes(self, raw: bytes, *, origin_label: str = "", origin_path: str = "") -> Tuple[str, Dict[str, Any], str]:
        if not isinstance(raw, (bytes, bytearray)) or not raw:
            return "", {}, "No torrent file bytes were provided."
        root, info_bytes, err = self._parse_torrent_file(raw)
        if err:
            return "", {}, err
        info = root.get(b"info")
        if not isinstance(info, dict):
            return "", {}, "Torrent file is missing an info dictionary."
        info_hash = hashlib.sha1(info_bytes).hexdigest()
        name = self._safe_text(info.get(b"name"), "")
        trackers: List[str] = []
        trackers.extend(self._flatten_tracker_values(root.get(b"announce")))
        trackers.extend(self._flatten_tracker_values(root.get(b"announce-list")))
        deduped_trackers: List[str] = []
        seen = set()
        for tracker in trackers:
            if tracker in seen:
                continue
            seen.add(tracker)
            deduped_trackers.append(tracker)

        magnet_parts = [f"magnet:?xt=urn:btih:{info_hash}"]
        if name:
            magnet_parts.append(f"dn={quote(name, safe='')}")
        for tracker in deduped_trackers[:20]:
            magnet_parts.append(f"tr={quote(tracker, safe='')}")

        metadata = {
            "torrent_path": origin_path,
            "torrent_label": origin_label,
            "info_hash": info_hash,
            "display_name": name,
            "tracker_count": len(deduped_trackers),
        }
        return "&".join(magnet_parts), metadata, ""

    def _magnet_from_torrent_file(self, torrent_path: str) -> Tuple[str, Dict[str, Any], str]:
        path = self._safe_text(torrent_path)
        if not path:
            return "", {}, "No local .torrent file path was provided."
        if not os.path.isfile(path):
            return "", {}, f"Torrent file was not found: {path}"
        try:
            with open(path, "rb") as handle:
                raw = handle.read()
        except Exception as exc:
            return "", {}, f"Could not read torrent file: {exc}"
        return self._magnet_from_torrent_bytes(raw, origin_label=os.path.basename(path), origin_path=path)

    async def _remote_torrent_to_magnet(self, torrent_url: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        url = self._safe_text(torrent_url)
        if not url:
            return {"source": "", "source_type": "torrent url", "resolve_error": "No remote .torrent URL was provided."}

        timeout = aiohttp.ClientTimeout(total=settings["timeout_seconds"])
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, allow_redirects=True) as resp:
                    raw = await resp.read()
                    if resp.status != 200:
                        preview = raw[:220].decode("utf-8", "ignore")
                        return {
                            "source": "",
                            "source_type": "torrent url",
                            "remote_torrent_url": url,
                            "resolve_error": f"Could not download remote torrent file: HTTP {resp.status}: {preview}",
                        }
        except Exception as exc:
            return {
                "source": "",
                "source_type": "torrent url",
                "remote_torrent_url": url,
                "resolve_error": f"Could not download remote torrent file: {exc}",
            }

        if not raw:
            return {
                "source": "",
                "source_type": "torrent url",
                "remote_torrent_url": url,
                "resolve_error": "Remote torrent download returned no bytes.",
            }

        label = os.path.basename(urlsplit(url).path) or url
        magnet, torrent_meta, err = self._magnet_from_torrent_bytes(raw, origin_label=label, origin_path=url)
        if magnet:
            return {
                "source": magnet,
                "source_type": "torrent url",
                "remote_torrent_url": url,
                "torrent_name": label,
                "info_hash": torrent_meta.get("info_hash"),
                "display_name": torrent_meta.get("display_name"),
                "tracker_count": torrent_meta.get("tracker_count"),
                "resolved_from": "remote_torrent_url",
            }
        return {
            "source": "",
            "source_type": "torrent url",
            "remote_torrent_url": url,
            "torrent_name": label,
            "resolve_error": err or "Could not parse remote torrent file.",
        }

    def _torrent_artifacts_from_inputs(self, args: Dict[str, Any], context: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        refs: List[Dict[str, Any]] = []
        for source in (
            args if isinstance(args, dict) else {},
            (args or {}).get("origin") if isinstance((args or {}).get("origin"), dict) else {},
            context if isinstance(context, dict) else {},
            (context or {}).get("origin") if isinstance((context or {}).get("origin"), dict) else {},
        ):
            items = source.get("input_artifacts") if isinstance(source, dict) else None
            if not isinstance(items, list):
                continue
            for item in items:
                if isinstance(item, dict):
                    refs.append(item)
        return [item for item in refs if self._artifact_is_torrent(item)]

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
        local_torrent_path = self._extract_first_local_torrent_path(query)
        if not local_torrent_path:
            local_torrent_path = self._explicit_torrent_path_from_args(args or {})
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
            "local_torrent_path": local_torrent_path,
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

    async def _resolve_source(
        self,
        args: Dict[str, Any],
        intent: Dict[str, Any],
        context: Optional[Dict[str, Any]],
        settings: Dict[str, Any],
    ) -> Dict[str, Any]:
        src = self._safe_text(intent.get("source"))
        if src:
            source_type = self._safe_text(intent.get("source_type")) or self._source_type(src)
            if source_type == "torrent url" and self._looks_like_remote_torrent_url(src):
                return await self._remote_torrent_to_magnet(src, settings)
            return {"source": src, "source_type": source_type}

        explicit = self._explicit_source_from_args(args or {})
        if explicit:
            source_type = self._source_type(explicit)
            if source_type == "torrent url" and self._looks_like_remote_torrent_url(explicit):
                return await self._remote_torrent_to_magnet(explicit, settings)
            return {"source": explicit, "source_type": source_type}

        torrent_path = self._safe_text(intent.get("local_torrent_path")) or self._explicit_torrent_path_from_args(args or {})
        if not torrent_path:
            torrent_path = self._extract_first_local_torrent_path(self._query_from_args(args or {}, context=context))
        if torrent_path:
            magnet, torrent_meta, err = self._magnet_from_torrent_file(torrent_path)
            if magnet:
                return {
                    "source": magnet,
                    "source_type": "torrent file",
                    "torrent_path": torrent_meta.get("torrent_path"),
                    "info_hash": torrent_meta.get("info_hash"),
                    "display_name": torrent_meta.get("display_name"),
                    "tracker_count": torrent_meta.get("tracker_count"),
                    "resolved_from": "local_torrent_file",
                }
            return {"source": "", "source_type": "torrent file", "torrent_path": torrent_path, "resolve_error": err}

        for artifact in self._torrent_artifacts_from_inputs(args or {}, context):
            raw = self._artifact_bytes(artifact)
            magnet, torrent_meta, err = self._magnet_from_torrent_bytes(
                raw,
                origin_label=self._safe_text(artifact.get("name")),
                origin_path=self._safe_text(artifact.get("path")),
            )
            if magnet:
                return {
                    "source": magnet,
                    "source_type": "torrent attachment",
                    "torrent_name": self._safe_text(artifact.get("name")),
                    "mimetype": self._safe_text(artifact.get("mimetype")),
                    "blob_key": self._safe_text(artifact.get("blob_key")),
                    "info_hash": torrent_meta.get("info_hash"),
                    "display_name": torrent_meta.get("display_name"),
                    "tracker_count": torrent_meta.get("tracker_count"),
                    "resolved_from": "input_artifact",
                }
            if err:
                return {
                    "source": "",
                    "source_type": "torrent attachment",
                    "torrent_name": self._safe_text(artifact.get("name")),
                    "blob_key": self._safe_text(artifact.get("blob_key")),
                    "resolve_error": err,
                }

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
        source_info = await self._resolve_source(args, intent, context, settings)
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
        source_info = await self._resolve_source(args, intent, context, settings)
        source = self._safe_text(source_info.get("source"))
        if not source:
            resolve_error = self._safe_text(source_info.get("resolve_error"))
            return action_failure(
                code="missing_source",
                message=resolve_error or "No explicit magnet, URL, attached .torrent file, or local .torrent file path was provided for link retrieval.",
                needs=["Pass the full magnet:? URI, http(s):// URI, attach a .torrent file, or provide a local .torrent file path directly in query or source arguments."],
                say_hint="Explain that this action requires a literal magnet, URL, attached .torrent file, or local .torrent file path source.",
            )

        links, err = await self._api_directdl(settings, source)
        if err:
            err_l = err.lower()
            if any(token in err_l for token in ("not cached", "not in cache", "cache")):
                return action_failure(
                    code="torrent_not_cached",
                    message="Torrent is not cached on Premiumize.",
                    needs=["Use Premiumize add transfer to start caching this torrent, then check links again after it finishes."],
                    say_hint="Explain that this torrent is not currently cached on Premiumize, offer to add it as a transfer to start caching, and suggest checking links again after the transfer completes.",
                )
            if "unsupported link for direct download" in err_l:
                return action_failure(
                    code="premiumize_source_unsupported",
                    message="Premiumize rejected that source for direct link retrieval.",
                    needs=["Use Premiumize add transfer if this torrent needs to be cached first, or retry with a literal magnet, attached .torrent file, local .torrent file path, or supported torrent URL."],
                    say_hint="Explain that Premiumize rejected the source itself for direct link retrieval; do not claim the API key or base URL is missing.",
                )
            return action_failure(
                code="premiumize_directdl_failed",
                message=err,
                diagnosis=self._diagnosis(),
                say_hint="Explain direct link retrieval failed and suggest retrying.",
            )

        normalized_links = self._normalize_direct_links(links)
        if not normalized_links:
            return action_failure(
                code="torrent_not_cached",
                message="Torrent is not cached on Premiumize.",
                needs=["Use Premiumize add transfer to start caching this torrent, then check links again after it finishes."],
                say_hint="Explain that this torrent is not currently cached on Premiumize, offer to add it as a transfer to start caching, and suggest checking links again after the transfer completes.",
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
        explicit_torrent_path = self._explicit_torrent_path_from_args(args or {})
        if not query:
            query = explicit_source or explicit_torrent_path
        if not query:
            return action_failure(
                code="missing_request",
                message="No Premiumize request text was provided.",
                needs=["Provide a link request and include the exact magnet, URL, attached .torrent file, or local .torrent file path."],
                say_hint="Ask for a concrete link request with a literal magnet, URL, attached .torrent file, or local .torrent file path source.",
            )

        intent = await self._resolve_intent(query, args or {}, llm_client)
        intent["action"] = "get_links"
        return await self._act_get_links(args=args or {}, intent=intent, settings=settings, context=context)

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

verba = PremiumizeGetLinksPlugin()
