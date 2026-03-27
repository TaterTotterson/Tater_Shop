import asyncio
import hashlib
import json
import logging
import math
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote
from xml.etree import ElementTree as ET

import httpx

from helpers import redis_client
from verba_base import ToolVerba
from verba_diagnostics import combine_diagnosis, diagnose_hash_fields, needs_from_diagnosis
from verba_result import action_failure, action_success

logger = logging.getLogger("jackett_search_torrents")
logger.setLevel(logging.INFO)


class JackettSearchTorrentsPlugin(ToolVerba):
    name = "jackett_search_torrents"
    verba_name = "Jackett Search Torrents"
    version = "1.0.0"
    min_tater_version = "59"
    pretty_name = "Jackett Search Torrents"
    settings_category = "Jackett"
    tags = ['jackett', 'search']
    fixed_action = "search"
    usage = (
        '{"function":"jackett_search_torrents","arguments":{"query":"find Ubuntu 24.04 torrents from public trackers"}}'
    )
    description = (
        'Search Jackett for torrents and return ranked results.'
    )
    verba_dec = 'Run Jackett torrent searches with filters and ranked results.'
    waiting_prompt_template = (
        "Tell {mention} you are querying Jackett now and will return ranked torrent results shortly. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "macos", "irc", "homeassistant", "homekit", "matrix", "telegram", "xbmc"]
    required_settings = {
        "JACKETT_BASE_URL": {
            "label": "Jackett Base URL",
            "type": "string",
            "default": "http://127.0.0.1:9117",
            "description": "Jackett URL, for example http://jackett.local:9117",
        },
        "JACKETT_API_KEY": {
            "label": "Jackett API Key",
            "type": "password",
            "default": "",
            "description": "API key from Jackett web UI.",
        },
        "JACKETT_TIMEOUT_SECONDS": {
            "label": "HTTP Timeout Seconds",
            "type": "string",
            "default": "20",
            "description": "Request timeout in seconds.",
        },
        "JACKETT_VERIFY_SSL": {
            "label": "Verify TLS Certs",
            "type": "string",
            "default": "false",
            "description": "Set true when Jackett has a valid TLS certificate.",
        },
        "JACKETT_DEFAULT_LIMIT": {
            "label": "Default Result Limit",
            "type": "string",
            "default": "25",
            "description": "Default number of ranked results to return.",
        },
    }
    when_to_use = (
        'Use when the user asks to search Jackett for torrents by title/content.'
    )
    how_to_use = (
        'Set `query` to the torrent search request. Optional filters: search_query, scope, indexers, categories, sort_by, sort_direction, min_seeders, max_age_days, min_size, max_size, and limit.'
    )
    common_needs = ['A Jackett search request in query.']
    missing_info_prompts = ['What should I search for in Jackett?']
    example_calls = ['{"function":"jackett_search_torrents","arguments":{"query":"search Jackett for Ubuntu 24.04 torrents"}}', '{"function":"jackett_search_torrents","arguments":{"query":"find Oppenheimer 4k torrents from private trackers","scope":"private"}}']
    routing_keywords = ['jackett', 'search', 'torrent', 'find', 'results', 'seeders']
    argument_schema = {'type': 'object', 'properties': {'query': {'type': 'string', 'description': 'The Jackett search request (for example: find Ubuntu 24.04 torrents).'}, 'search_query': {'type': 'string', 'description': 'Optional explicit Jackett query text.'}, 'scope': {'type': 'string', 'description': 'Optional scope: all, public, or private.'}, 'indexers': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Optional list of indexer names/ids.'}, 'categories': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Optional category labels or IDs.'}, 'sort_by': {'type': 'string', 'description': 'Optional sort: best_match, seeders, newest, or size.'}, 'sort_direction': {'type': 'string', 'description': 'Optional sort direction: desc or asc.'}, 'min_seeders': {'type': 'integer', 'description': 'Optional minimum seeder count.'}, 'max_age_days': {'type': 'integer', 'description': 'Optional maximum age in days.'}, 'min_size': {'type': 'string', 'description': 'Optional minimum size (bytes or text like 2GB).'}, 'max_size': {'type': 'string', 'description': 'Optional maximum size (bytes or text like 20GB).'}, 'limit': {'type': 'integer', 'description': 'Optional max results to return (1-100).'}}, 'required': []}

    MAX_LIMIT = 100
    CACHE_TTL_SECONDS = 6 * 60 * 60
    CACHE_KEY = "tater:jackett:last_results"
    TORZNAB_NS = {"torznab": "http://torznab.com/schemas/2015/feed"}
    ORDINAL_MAP = {
        "first": 1,
        "second": 2,
        "third": 3,
        "fourth": 4,
        "fifth": 5,
        "sixth": 6,
        "seventh": 7,
        "eighth": 8,
        "ninth": 9,
        "tenth": 10,
    }
    CATEGORY_KEYWORDS = {
        "movie": 2000,
        "movies": 2000,
        "film": 2000,
        "films": 2000,
        "tv": 5000,
        "show": 5000,
        "shows": 5000,
        "series": 5000,
        "episode": 5000,
        "episodes": 5000,
        "anime": 5070,
        "music": 3000,
        "album": 3000,
        "albums": 3000,
        "audio": 3000,
        "flac": 3000,
        "mp3": 3000,
        "game": 1000,
        "games": 1000,
        "software": 4000,
        "app": 4000,
        "apps": 4000,
        "linux": 4000,
        "iso": 4000,
        "book": 7000,
        "books": 7000,
        "ebook": 7000,
        "ebooks": 7000,
        "xxx": 6000,
    }

    def _diagnosis(self) -> Dict[str, str]:
        hash_diag = diagnose_hash_fields(
            "verba_settings:Jackett",
            fields={
                "jackett_base_url": "JACKETT_BASE_URL",
                "jackett_api_key": "JACKETT_API_KEY",
            },
            validators={
                "jackett_base_url": lambda v: str(v or "").startswith(("http://", "https://")),
                "jackett_api_key": lambda v: len(str(v or "").strip()) >= 8,
            },
        )
        return combine_diagnosis(hash_diag)

    @staticmethod
    def _clamp_int(value: Any, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            return int(default)
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _to_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        if not text:
            return default
        return text in {"1", "true", "yes", "on", "y"}

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

    def _settings(self) -> Dict[str, Any]:
        raw = redis_client.hgetall("verba_settings:Jackett") or {}
        base_url = self._safe_text(raw.get("JACKETT_BASE_URL"), "http://127.0.0.1:9117").rstrip("/")
        api_key = self._safe_text(raw.get("JACKETT_API_KEY"), "")
        try:
            timeout_seconds = float(self._safe_text(raw.get("JACKETT_TIMEOUT_SECONDS"), "20") or "20")
        except Exception:
            timeout_seconds = 20.0
        timeout_seconds = float(max(5, min(120, timeout_seconds)))
        verify_ssl = self._to_bool(raw.get("JACKETT_VERIFY_SSL"), default=False)
        default_limit = self._clamp_int(raw.get("JACKETT_DEFAULT_LIMIT"), 5, self.MAX_LIMIT, 25)
        return {
            "base_url": base_url,
            "api_key": api_key,
            "timeout_seconds": timeout_seconds,
            "verify_ssl": verify_ssl,
            "default_limit": default_limit,
        }

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

        if isinstance(context, dict):
            for key in ("request_text", "query", "text", "content", "message", "raw_message", "body"):
                value = context.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ""

    @staticmethod
    def _normalize_action(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"[\s\-]+", "_", text)
        if text in {"search", "recent", "list_indexers", "inspect_indexer", "which_indexer", "handoff"}:
            return text
        if text in {"list", "indexers", "listindexers", "list_indexer", "list_trackers", "list_sources"}:
            return "list_indexers"
        if text in {"inspect", "indexer_details", "indexer_info", "inspect_indexers", "inspect_tracker"}:
            return "inspect_indexer"
        if text in {"source", "find_source", "which_tracker", "which_source", "source_lookup"}:
            return "which_indexer"
        if text in {"send", "handoff_prepare", "dispatch", "send_to_downloader", "handoff_to_downloader"}:
            return "handoff"
        return "search"

    @staticmethod
    def _normalize_scope(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"all", "public", "private"}:
            return text
        if text in {"public_only", "public trackers"}:
            return "public"
        if text in {"private_only", "private trackers"}:
            return "private"
        return "all"

    @staticmethod
    def _normalize_sort_by(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"seeders", "seeds", "best_seeded"}:
            return "seeders"
        if text in {"newest", "recent", "latest", "date"}:
            return "newest"
        if text in {"size"}:
            return "size"
        return "best_match"

    @staticmethod
    def _normalize_sort_direction(value: Any, sort_by: str) -> str:
        text = str(value or "").strip().lower()
        if text in {"asc", "ascending", "smallest", "oldest"}:
            return "asc"
        if text in {"desc", "descending", "largest", "newest"}:
            return "desc"
        if sort_by == "size" and text in {"small", "smallest"}:
            return "asc"
        return "desc"

    @staticmethod
    def _slug(value: Any) -> str:
        text = str(value or "").strip().lower()
        return re.sub(r"[^a-z0-9]+", "", text)

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        return [tok for tok in re.findall(r"[a-z0-9]+", str(text or "").lower()) if len(tok) >= 2]

    @staticmethod
    def _parse_size_to_bytes(text: str) -> Optional[int]:
        m = re.search(r"(\d+(?:\.\d+)?)\s*(kb|mb|gb|tb|kib|mib|gib|tib)\b", str(text or "").lower())
        if not m:
            return None
        value = float(m.group(1))
        unit = m.group(2)
        power = {
            "kb": 1,
            "kib": 1,
            "mb": 2,
            "mib": 2,
            "gb": 3,
            "gib": 3,
            "tb": 4,
            "tib": 4,
        }.get(unit, 0)
        return int(value * (1024 ** power))

    @staticmethod
    def _human_size(size_bytes: Any) -> str:
        try:
            value = int(size_bytes)
        except Exception:
            return ""
        if value <= 0:
            return ""
        units = ["B", "KB", "MB", "GB", "TB"]
        idx = 0
        amount = float(value)
        while amount >= 1024 and idx < len(units) - 1:
            amount /= 1024.0
            idx += 1
        if idx == 0:
            return f"{int(amount)} {units[idx]}"
        return f"{amount:.2f} {units[idx]}"

    def _extract_categories(self, values: Any) -> List[int]:
        items: List[str] = []
        if isinstance(values, list):
            items.extend([self._safe_text(v) for v in values if self._safe_text(v)])
        elif isinstance(values, str):
            items.extend([x.strip() for x in re.split(r"[,;/]+", values) if x.strip()])
        elif values is not None:
            items.append(self._safe_text(values))

        out: List[int] = []
        for item in items:
            text = item.lower().strip()
            if not text:
                continue
            if text.isdigit():
                cid = int(text)
                if cid not in out:
                    out.append(cid)
                continue
            cid = self.CATEGORY_KEYWORDS.get(text)
            if cid is not None and cid not in out:
                out.append(cid)
        return out

    def _extract_limit(self, value: Any, default_limit: int) -> int:
        return self._clamp_int(value, 1, self.MAX_LIMIT, default_limit)

    @staticmethod
    def _extract_min_seeders(value: Any) -> int:
        try:
            return max(0, int(value))
        except Exception:
            return 0

    @staticmethod
    def _extract_max_age_days(value: Any) -> Optional[int]:
        try:
            days = int(value)
        except Exception:
            return None
        return days if days > 0 else None

    def _extract_size_filters(self, min_value: Any, max_value: Any) -> Tuple[Optional[int], Optional[int]]:
        min_size: Optional[int] = None
        max_size: Optional[int] = None
        if min_value is not None:
            try:
                min_size = int(min_value)
            except Exception:
                min_size = self._parse_size_to_bytes(str(min_value))
        if max_value is not None:
            try:
                max_size = int(max_value)
            except Exception:
                max_size = self._parse_size_to_bytes(str(max_value))
        return min_size, max_size

    def _extract_downloader_target(self, value: Any) -> str:
        text = self._safe_text(value).lower()
        if not text:
            return "downloader"
        if "qb" == text:
            return "qbittorrent"
        return text

    def _extract_result_reference(self, data: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key in ("result_id", "result_title", "result_guid", "selection", "index"):
            if data.get(key) is not None:
                out[key] = data.get(key)
        # Accept llm aliases.
        if out.get("result_title") is None and data.get("title") is not None:
            out["result_title"] = data.get("title")
        if out.get("index") is None and data.get("rank") is not None:
            out["index"] = data.get("rank")
        return out

    def _extract_indexer_mentions(self, raw: Any, indexers: List[Dict[str, Any]]) -> List[str]:
        mentioned: List[str] = []
        if isinstance(raw, list):
            mentioned.extend([self._safe_text(x) for x in raw if self._safe_text(x)])
        elif raw is not None:
            text = self._safe_text(raw)
            if text:
                mentioned.extend([part.strip() for part in re.split(r"[,;/]+", text) if part.strip()])

        if not mentioned:
            return []

        canonical: List[str] = []
        for name in mentioned:
            key = self._slug(name)
            resolved = ""
            for idx in indexers:
                if key == self._slug(idx.get("id")) or key == self._slug(idx.get("name")):
                    resolved = self._safe_text(idx.get("id") or idx.get("name"))
                    break
            canonical.append(resolved or name)

        dedup: List[str] = []
        seen = set()
        for item in canonical:
            key = self._slug(item)
            if not key or key in seen:
                continue
            seen.add(key)
            dedup.append(item)
        return dedup

    def _infer_scope_from_query(self, query_text: str) -> str:
        q = self._safe_text(query_text).lower()
        if "private" in q:
            return "private"
        if "public" in q:
            return "public"
        return "all"

    def _infer_sort_by_from_query(self, query_text: str, action: str) -> str:
        q = self._safe_text(query_text).lower()
        if any(token in q for token in ("seeders", "seeds", "most seeded")):
            return "seeders"
        if any(token in q for token in ("newest", "latest", "recent")):
            return "newest" if action == "recent" else "best_match"
        if any(token in q for token in ("size", "largest", "smallest", "biggest")):
            return "size"
        return "newest" if action == "recent" else "best_match"

    def _infer_sort_direction_from_query(self, query_text: str, sort_by: str) -> str:
        q = self._safe_text(query_text).lower()
        if "ascending" in q or "asc" in q or "oldest" in q or "smallest" in q:
            return "asc"
        if "descending" in q or "desc" in q or "largest" in q or "biggest" in q:
            return "desc"
        if sort_by == "size" and "small" in q:
            return "asc"
        return "desc"

    def _extract_categories_from_query(self, query_text: str) -> List[int]:
        q = self._safe_text(query_text).lower()
        found: List[int] = []
        for token, cid in self.CATEGORY_KEYWORDS.items():
            if re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", q):
                if cid not in found:
                    found.append(cid)
        return found

    def _clean_search_query(self, query_text: str, action: str) -> str:
        text = self._safe_text(query_text)
        if not text:
            return ""
        lowered = text.lower()
        if action == "search":
            lowered = re.sub(r"^\s*(search|find|look\s*up|lookup|show)\s+(?:jackett\s+)?(?:for\s+)?", "", lowered)
            lowered = re.sub(r"\b(from|on)\s+(public|private)\s+(indexers?|trackers?)\b", "", lowered)
            lowered = re.sub(r"\b(?:in|from|on)\s+jackett\b", "", lowered)
            lowered = re.sub(r"\b(torrents?|magnet|trackers?)\b", "", lowered)
            lowered = re.sub(r"\s+", " ", lowered).strip(" ,.;:-")
            return lowered

        if action == "recent":
            lowered = re.sub(r"\b(show|list|find|give me|what are|any)\b", "", lowered)
            lowered = re.sub(r"\b(new|newest|latest|recent|uploads?|torrents?|from|on|jackett)\b", "", lowered)
            lowered = re.sub(r"\s+", " ", lowered).strip(" ,.;:-")
            return lowered

        return ""

    def _infer_result_ref_from_query(self, query_text: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        q = self._safe_text(query_text).lower()
        if not q:
            return out

        rid = re.search(r"\b(jkt_[a-f0-9]{8,})\b", q)
        if rid:
            out["result_id"] = rid.group(1)

        idx_match = re.search(r"\b(?:result|item|rank|index|#)\s*(\d{1,3})\b", q)
        if idx_match:
            out["index"] = int(idx_match.group(1))
        else:
            for word, val in self.ORDINAL_MAP.items():
                if re.search(rf"(?<![a-z0-9]){re.escape(word)}(?![a-z0-9])", q):
                    out["index"] = int(val)
                    break

        title_match = re.search(r"\"([^\"]{3,220})\"", self._safe_text(query_text))
        if title_match:
            out["result_title"] = title_match.group(1).strip()
        return out

    @staticmethod
    def _merge_result_refs(primary: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(fallback or {})
        out.update({k: v for k, v in (primary or {}).items() if v not in {None, "", 0}})
        return out

    def _infer_indexers_from_query(self, query_text: str, known_indexers: List[Dict[str, Any]]) -> List[str]:
        q_slug = self._slug(query_text)
        if not q_slug:
            return []
        hits: List[str] = []
        seen = set()
        for idx in known_indexers:
            idx_id = self._safe_text(idx.get("id"))
            idx_name = self._safe_text(idx.get("name"))
            for token in (idx_id, idx_name):
                slug = self._slug(token)
                if not slug or slug in seen:
                    continue
                if slug in q_slug:
                    seen.add(slug)
                    hits.append(idx_id or idx_name)
                    break
        return hits

    def _infer_downloader_from_query(self, query_text: str) -> str:
        q = self._safe_text(query_text).lower()
        if "qbittorrent" in q or re.search(r"\bqb\b", q):
            return "qbittorrent"
        if "transmission" in q:
            return "transmission"
        if "deluge" in q:
            return "deluge"
        if "sabnzbd" in q or "sab" in q:
            return "sabnzbd"
        return "downloader"

    async def _resolve_plan(
        self,
        *,
        query_text: str,
        args: Dict[str, Any],
        llm_client: Any,
        known_indexers: List[Dict[str, Any]],
        default_limit: int,
    ) -> Dict[str, Any]:
        _ = llm_client
        if isinstance(known_indexers, list):
            # no-op to satisfy type checker style consistency
            pass

        action = self._normalize_action(getattr(self, "fixed_action", "") or "search")
        scope = self._normalize_scope(args.get("scope")) or self._infer_scope_from_query(query_text)
        sort_by = self._normalize_sort_by(args.get("sort_by")) or self._infer_sort_by_from_query(query_text, action)
        sort_direction = self._normalize_sort_direction(args.get("sort_direction"), sort_by)
        if "sort_direction" not in args:
            sort_direction = self._infer_sort_direction_from_query(query_text, sort_by)

        search_query = self._safe_text(args.get("search_query")) or self._clean_search_query(query_text, action)
        categories = self._extract_categories(args.get("categories")) or self._extract_categories_from_query(query_text)
        limit = self._extract_limit(args.get("limit"), default_limit)
        min_seeders = self._extract_min_seeders(args.get("min_seeders"))
        max_age_days = self._extract_max_age_days(args.get("max_age_days"))
        min_size, max_size = self._extract_size_filters(
            args.get("min_size", args.get("min_size_bytes")),
            args.get("max_size", args.get("max_size_bytes")),
        )

        if min_seeders <= 0:
            seeded = re.search(r"\b(?:at least|min)\s*(\d{1,6})\s*seeders?\b", self._safe_text(query_text).lower())
            if seeded:
                min_seeders = max(0, int(seeded.group(1)))
        if max_age_days is None:
            age = re.search(r"\b(?:within|last|past)\s*(\d{1,4})\s*days?\b", self._safe_text(query_text).lower())
            if age:
                max_age_days = max(1, int(age.group(1)))
        if min_size is None:
            m = re.search(r"\b(?:over|larger than|bigger than|min(?:imum)? size(?: of)?)\s*([0-9.]+\s*(?:kb|mb|gb|tb|kib|mib|gib|tib))\b", self._safe_text(query_text).lower())
            if m:
                min_size = self._parse_size_to_bytes(m.group(1))
        if max_size is None:
            m = re.search(r"\b(?:under|smaller than|less than|max(?:imum)? size(?: of)?)\s*([0-9.]+\s*(?:kb|mb|gb|tb|kib|mib|gib|tib))\b", self._safe_text(query_text).lower())
            if m:
                max_size = self._parse_size_to_bytes(m.group(1))

        indexer_raw = (
            args.get("indexers")
            or args.get("indexer")
            or args.get("trackers")
            or args.get("tracker")
        )
        indexers = self._extract_indexer_mentions(indexer_raw, known_indexers)
        if not indexers:
            indexers = self._infer_indexers_from_query(query_text, known_indexers)
        result_ref = self._merge_result_refs(
            self._extract_result_reference(args),
            self._infer_result_ref_from_query(query_text),
        )
        target_downloader = self._extract_downloader_target(
            args.get("target_downloader") or args.get("downloader") or args.get("target") or self._infer_downloader_from_query(query_text)
        )

        return {
            "action": action,
            "scope": scope,
            "search_query": search_query,
            "categories": categories,
            "sort_by": sort_by,
            "sort_direction": sort_direction,
            "limit": limit,
            "min_seeders": min_seeders,
            "max_age_days": max_age_days,
            "min_size_bytes": min_size,
            "max_size_bytes": max_size,
            "indexer_names": indexers,
            "result_ref": result_ref,
            "target_downloader": target_downloader,
        }

    async def _http_get(self, settings: Dict[str, Any], path: str, params: Dict[str, Any]) -> httpx.Response:
        url = f"{settings['base_url'].rstrip('/')}{path}"
        params = dict(params or {})
        params["apikey"] = settings["api_key"]
        timeout = httpx.Timeout(settings["timeout_seconds"])
        async with httpx.AsyncClient(timeout=timeout, verify=bool(settings["verify_ssl"]), follow_redirects=True) as client:
            response = await client.get(url, params=params)
            return response

    def _normalize_indexer(self, item: Dict[str, Any]) -> Dict[str, Any]:
        idx_id = self._safe_text(item.get("id") or item.get("indexer") or item.get("name"))
        name = self._safe_text(item.get("name") or idx_id or "Unknown")
        raw_type = self._safe_text(item.get("type") or item.get("privacy")).lower()
        privacy = "unknown"
        if "public" in raw_type:
            privacy = "public"
        elif "private" in raw_type or "semi" in raw_type:
            privacy = "private"
        configured = bool(item.get("configured", item.get("enabled", True)))
        enabled = bool(item.get("enabled", configured))
        status = self._safe_text(item.get("status") or item.get("health"), "")
        site = self._safe_text(item.get("site_link") or item.get("site"), "")
        language = self._safe_text(item.get("language") or item.get("language_code"), "")

        cat_map = item.get("caps") or {}
        categories: List[str] = []
        if isinstance(cat_map, dict):
            cats = cat_map.get("categories")
            if isinstance(cats, list):
                for c in cats:
                    if isinstance(c, dict):
                        label = self._safe_text(c.get("name") or c.get("id"))
                        if label:
                            categories.append(label)
                    elif self._safe_text(c):
                        categories.append(self._safe_text(c))
        return {
            "id": idx_id or name,
            "name": name,
            "privacy": privacy,
            "type": raw_type or privacy,
            "enabled": enabled,
            "configured": configured,
            "status": status,
            "site_link": site,
            "language": language,
            "categories": categories,
        }

    async def _fetch_indexers(self, settings: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        try:
            resp = await self._http_get(settings, "/api/v2.0/indexers", {})
        except Exception as exc:
            return [], f"Failed to reach Jackett indexer API: {exc}"
        if resp.status_code != 200:
            body = self._safe_text(resp.text)
            return [], f"Jackett indexer API returned HTTP {resp.status_code}: {body[:180]}"
        try:
            payload = resp.json()
        except Exception as exc:
            return [], f"Jackett indexer API returned invalid JSON: {exc}"

        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            items = (
                payload.get("indexers")
                or payload.get("results")
                or payload.get("data")
                or payload.get("items")
                or []
            )
        else:
            items = []

        normalized: List[Dict[str, Any]] = []
        for raw in items:
            if not isinstance(raw, dict):
                continue
            n = self._normalize_indexer(raw)
            if n.get("id"):
                normalized.append(n)
        normalized.sort(key=lambda x: self._safe_text(x.get("name") or x.get("id")).lower())
        return normalized, None

    def _resolve_indexers_for_scope(
        self,
        *,
        scope: str,
        requested_names: List[str],
        indexers: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        available = [i for i in indexers if bool(i.get("enabled", True)) and bool(i.get("configured", True))]
        missing: List[str] = []

        if scope == "public":
            available = [i for i in available if i.get("privacy") == "public"]
        elif scope == "private":
            available = [i for i in available if i.get("privacy") == "private"]

        if not requested_names:
            return available, missing

        by_slug = {self._slug(i.get("id")): i for i in available}
        for i in available:
            by_slug[self._slug(i.get("name"))] = i

        selected: List[Dict[str, Any]] = []
        seen = set()
        for name in requested_names:
            key = self._slug(name)
            match = by_slug.get(key)
            if not match:
                # partial match fallback
                for idx in available:
                    if key and key in self._slug(idx.get("name")):
                        match = idx
                        break
            if match:
                sid = self._safe_text(match.get("id"))
                if sid and sid not in seen:
                    selected.append(match)
                    seen.add(sid)
            else:
                missing.append(name)
        return selected, missing

    @staticmethod
    def _attr_map(item: ET.Element) -> Dict[str, str]:
        attrs: Dict[str, str] = {}
        for attr in item.findall("torznab:attr", JackettSearchTorrentsPlugin.TORZNAB_NS):
            name = (attr.attrib.get("name") or "").strip().lower()
            value = (attr.attrib.get("value") or "").strip()
            if name and value and name not in attrs:
                attrs[name] = value
        for attr in item.findall(".//*"):
            if not str(attr.tag).endswith("attr"):
                continue
            name = (attr.attrib.get("name") or "").strip().lower()
            value = (attr.attrib.get("value") or "").strip()
            if name and value and name not in attrs:
                attrs[name] = value
        return attrs

    @staticmethod
    def _parse_pub_date(value: str) -> Tuple[str, Optional[int], Optional[float]]:
        text = str(value or "").strip()
        if not text:
            return "", None, None
        dt: Optional[datetime] = None
        try:
            dt = parsedate_to_datetime(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
        except Exception:
            dt = None
        if dt is None:
            return "", None, None
        now = datetime.now(timezone.utc)
        age_h = max(0.0, (now - dt).total_seconds() / 3600.0)
        return dt.isoformat(), int(dt.timestamp()), age_h

    @staticmethod
    def _looks_like_search_api(url: str) -> bool:
        text = str(url or "").strip().lower()
        if not text:
            return False
        if "results/torznab" in text and ("t=search" in text or "q=" in text or "cat=" in text):
            return True
        if "/api/v2.0/indexers/" in text and "/results/torznab/api" in text and ("q=" in text or "t=search" in text):
            return True
        return False

    @staticmethod
    def _is_http_url(url: str) -> bool:
        text = str(url or "").strip().lower()
        return text.startswith("http://") or text.startswith("https://")

    @staticmethod
    def _is_magnet_uri(uri: str) -> bool:
        raw = str(uri or "").strip()
        if not raw:
            return False
        lower = raw.lower()
        if lower.startswith("magnet:%3f"):
            raw = unquote(raw)
            lower = raw.lower()
        if not lower.startswith("magnet:?"):
            return False
        return "xt=urn:btih:" in lower

    @staticmethod
    def _looks_like_download_url(url: str) -> bool:
        low = str(url or "").strip().lower()
        if not low:
            return False
        if low.startswith("magnet:?"):
            return True
        if not (low.startswith("http://") or low.startswith("https://")):
            return False
        if (
            ".torrent" in low
            or "/download" in low
            or "/dl/" in low
            or "download=" in low
            or "action=download" in low
            or "torrentid=" in low
            or "get.php" in low
        ):
            return True
        return False

    def _preferred_transfer_uri(self, item: Dict[str, Any]) -> Tuple[str, str]:
        magnet = self._safe_text(item.get("magnet_url"))
        if self._is_magnet_uri(magnet):
            return magnet, "magnet"

        torrent_url = self._safe_text(item.get("torrent_url"))
        if (
            self._is_http_url(torrent_url)
            and not self._looks_like_search_api(torrent_url)
            and self._looks_like_download_url(torrent_url)
        ):
            return torrent_url, "torrent"

        return "", ""

    def _top_result_preview(self, results: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for item in results[: max(0, int(limit))]:
            transfer_uri, transfer_type = self._preferred_transfer_uri(item)
            out.append(
                {
                    "rank": int(item.get("rank") or 0),
                    "result_id": self._safe_text(item.get("id")),
                    "title": self._safe_text(item.get("title")),
                    "indexer": self._safe_text(item.get("indexer")),
                    "seeders": int(item.get("seeders") or 0),
                    "size": self._safe_text(item.get("size")),
                    "publish_date": self._safe_text(item.get("publish_date")),
                    "transfer_uri": transfer_uri,
                    "transfer_type": transfer_type,
                    "magnet_url": self._safe_text(item.get("magnet_url")),
                    "torrent_url": self._safe_text(item.get("torrent_url")),
                }
            )
        return out

    @staticmethod
    def _extract_inline_urls(item: ET.Element) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        seen = set()
        # Capture common RSS/Atom-style URL carriers.
        for node in list(item):
            tag = str(node.tag or "").lower()
            url = (node.attrib.get("url") or node.attrib.get("href") or "").strip()
            if not url:
                continue
            key = (url, tag)
            if key in seen:
                continue
            seen.add(key)
            out.append(key)
        return out

    def _normalize_item(
        self,
        item: ET.Element,
        *,
        fallback_indexer_id: str,
        indexer_lookup: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        title = self._safe_text(item.findtext("title"), "")
        link = self._safe_text(item.findtext("link"), "")
        guid = self._safe_text(item.findtext("guid"), "")
        comments = self._safe_text(item.findtext("comments"), "")
        pub_raw = self._safe_text(item.findtext("pubDate"), "")
        size_raw = self._safe_text(item.findtext("size"), "")
        category_text = self._safe_text(item.findtext("category"), "")

        attrs = self._attr_map(item)
        indexer_hint = (
            self._safe_text(attrs.get("jackettindexer"))
            or self._safe_text(attrs.get("indexer"))
            or self._safe_text(attrs.get("tracker"))
            or fallback_indexer_id
        )
        indexer_key = self._slug(indexer_hint)
        lookup = indexer_lookup.get(indexer_key) or indexer_lookup.get(self._slug(fallback_indexer_id)) or {}
        indexer_name = self._safe_text(lookup.get("name") or indexer_hint or fallback_indexer_id, "Unknown")
        indexer_id = self._safe_text(lookup.get("id") or indexer_hint or fallback_indexer_id, "unknown")
        indexer_privacy = self._safe_text(lookup.get("privacy"), "unknown")

        def as_int(*keys: str) -> int:
            for key in keys:
                val = attrs.get(key)
                if val is None:
                    continue
                try:
                    return int(float(str(val)))
                except Exception:
                    continue
            return 0

        seeders = as_int("seeders", "seed", "seeds")
        leechers = as_int("peers", "leechers")
        size_bytes = 0
        try:
            size_bytes = int(float(size_raw)) if size_raw else as_int("size")
        except Exception:
            size_bytes = as_int("size")

        magnet_url = (
            self._safe_text(attrs.get("magneturl"))
            or self._safe_text(attrs.get("magnet"))
            or self._safe_text(attrs.get("magneturi"))
            or self._safe_text(attrs.get("magnetlink"))
        )
        torrent_url = (
            self._safe_text(attrs.get("downloadurl"))
            or self._safe_text(attrs.get("download"))
            or self._safe_text(attrs.get("torrent"))
            or self._safe_text(attrs.get("torrenturl"))
        )
        details_url = (
            comments
            or self._safe_text(attrs.get("detailsurl"))
            or self._safe_text(attrs.get("details"))
            or ""
        )

        if magnet_url and not self._is_magnet_uri(magnet_url):
            # Some trackers overload magnet-style attrs with HTTP links; keep those as torrent/details candidates.
            if not torrent_url and self._is_http_url(magnet_url):
                torrent_url = magnet_url
            magnet_url = ""

        # Inspect enclosure/atom URLs because many Torznab feeds provide magnet or torrent here.
        for url, tag in self._extract_inline_urls(item):
            low_url = url.lower()
            low_tag = str(tag or "").lower()
            if self._is_magnet_uri(url):
                if not magnet_url:
                    magnet_url = url
                continue
            if not self._is_http_url(low_url):
                continue
            looks_like_torrent = self._looks_like_download_url(low_url) or "bittorrent" in low_tag
            if looks_like_torrent and not torrent_url:
                torrent_url = url
                continue
            if not details_url and not self._looks_like_search_api(low_url):
                details_url = url

        if self._is_magnet_uri(link) and not magnet_url:
            magnet_url = link
        elif self._is_http_url(link):
            low_link = link.lower()
            if not torrent_url and not self._looks_like_search_api(low_link) and self._looks_like_download_url(low_link):
                torrent_url = link
            if not details_url and not self._looks_like_search_api(low_link):
                details_url = link
            if self._looks_like_search_api(low_link) and not details_url:
                # Keep query/search-like URLs only as details fallback, never as download URI.
                details_url = link

        if torrent_url:
            low_torrent = torrent_url.lower()
            if self._looks_like_search_api(low_torrent):
                if not details_url:
                    details_url = torrent_url
                torrent_url = ""
            elif not self._looks_like_download_url(low_torrent):
                if not details_url:
                    details_url = torrent_url
                torrent_url = ""

        if not details_url and guid.startswith("http"):
            details_url = guid

        publish_date, publish_ts, age_hours = self._parse_pub_date(pub_raw)
        infohash = self._safe_text(attrs.get("infohash"), "")
        cats = []
        if attrs.get("categorydesc"):
            cats.append(self._safe_text(attrs.get("categorydesc")))
        if attrs.get("category"):
            cats.append(self._safe_text(attrs.get("category")))
        if category_text:
            cats.append(category_text)
        categories: List[str] = []
        for c in cats:
            if c and c not in categories:
                categories.append(c)
        category = categories[0] if categories else ""

        uid_seed = "|".join(
            [
                indexer_id,
                infohash,
                guid,
                title,
                publish_date,
                magnet_url,
                torrent_url,
            ]
        )
        rid = f"jkt_{hashlib.sha1(uid_seed.encode('utf-8')).hexdigest()[:16]}"

        normalized = {
            "id": rid,
            "title": title,
            "indexer": indexer_name,
            "indexer_id": indexer_id,
            "indexer_privacy": indexer_privacy,
            "size_bytes": size_bytes,
            "size": self._human_size(size_bytes),
            "seeders": seeders,
            "leechers": leechers,
            "peers": seeders + leechers if (seeders + leechers) > 0 else leechers,
            "publish_date": publish_date,
            "publish_ts": publish_ts,
            "age_hours": age_hours,
            "details_url": details_url,
            "magnet_url": magnet_url,
            "torrent_url": torrent_url,
            "category": category,
            "categories": categories,
            "guid": guid,
            "infohash": infohash,
            "raw_attrs": attrs,
        }
        transfer_uri, transfer_type = self._preferred_transfer_uri(normalized)
        normalized["transfer_uri"] = transfer_uri
        normalized["transfer_type"] = transfer_type
        normalized["has_transfer_uri"] = bool(transfer_uri)
        normalized["handoff"] = {
            "title": normalized["title"],
            "indexer": normalized["indexer"],
            "indexer_id": normalized["indexer_id"],
            "magnet_url": normalized["magnet_url"],
            "torrent_url": normalized["torrent_url"],
            "details_url": normalized["details_url"],
            "transfer_uri": normalized["transfer_uri"],
            "transfer_type": normalized["transfer_type"],
            "size_bytes": normalized["size_bytes"],
            "seeders": normalized["seeders"],
            "leechers": normalized["leechers"],
            "category": normalized["category"],
        }
        return normalized

    def _parse_torznab_xml(
        self,
        *,
        xml_text: str,
        fallback_indexer_id: str,
        indexer_lookup: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        text = str(xml_text or "").strip()
        if not text:
            return []
        try:
            root = ET.fromstring(text)
        except Exception as exc:
            logger.warning("[jackett] Failed to parse Torznab XML (%s): %.180s", exc, text)
            return []
        out: List[Dict[str, Any]] = []
        for item in root.findall(".//item"):
            try:
                normalized = self._normalize_item(
                    item,
                    fallback_indexer_id=fallback_indexer_id,
                    indexer_lookup=indexer_lookup,
                )
                if normalized.get("title"):
                    out.append(normalized)
            except Exception as exc:
                logger.debug("[jackett] item normalize skip: %s", exc)
        return out

    async def _fetch_torznab(
        self,
        *,
        settings: Dict[str, Any],
        indexer_id: str,
        params: Dict[str, Any],
    ) -> Tuple[List[str], Optional[str]]:
        path = f"/api/v2.0/indexers/{indexer_id}/results/torznab/api"
        try:
            resp = await self._http_get(settings, path, params)
        except Exception as exc:
            return [], f"Failed querying indexer '{indexer_id}': {exc}"
        if resp.status_code != 200:
            body = self._safe_text(resp.text)
            return [], f"Indexer '{indexer_id}' returned HTTP {resp.status_code}: {body[:160]}"
        return [resp.text], None

    def _filter_results(self, results: List[Dict[str, Any]], plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        min_seeders = int(plan.get("min_seeders") or 0)
        max_age_days = plan.get("max_age_days")
        min_size = plan.get("min_size_bytes")
        max_size = plan.get("max_size_bytes")
        query_tokens = self._tokenize(plan.get("search_query") or "")
        is_recent_mode = plan.get("action") == "recent"

        dedup: Dict[str, Dict[str, Any]] = {}
        for item in results:
            key = (
                self._safe_text(item.get("infohash"))
                or self._safe_text(item.get("magnet_url"))
                or self._safe_text(item.get("guid"))
                or f"{self._slug(item.get('title'))}|{self._slug(item.get('indexer_id'))}"
            )
            if not key:
                continue
            existing = dedup.get(key)
            if existing is None:
                dedup[key] = item
            else:
                if int(item.get("seeders") or 0) > int(existing.get("seeders") or 0):
                    dedup[key] = item

        out: List[Dict[str, Any]] = []
        for item in dedup.values():
            seeders = int(item.get("seeders") or 0)
            if seeders < min_seeders:
                continue
            age_hours = item.get("age_hours")
            if max_age_days and isinstance(age_hours, (float, int)):
                if age_hours > float(max_age_days) * 24.0:
                    continue
            size = int(item.get("size_bytes") or 0)
            if min_size is not None and size > 0 and size < int(min_size):
                continue
            if max_size is not None and size > int(max_size):
                continue
            if is_recent_mode and query_tokens:
                title_tokens = set(self._tokenize(self._safe_text(item.get("title"))))
                matches = len([t for t in query_tokens if t in title_tokens])
                if matches == 0:
                    continue
            out.append(item)
        return out

    @staticmethod
    def _match_score(title: str, query_tokens: List[str]) -> float:
        if not query_tokens:
            return 0.0
        title_tokens = set(re.findall(r"[a-z0-9]+", str(title or "").lower()))
        if not title_tokens:
            return 0.0
        matches = len([t for t in query_tokens if t in title_tokens])
        return matches / float(len(query_tokens) or 1)

    def _composite_score(self, item: Dict[str, Any], query_tokens: List[str]) -> float:
        relevance = self._match_score(self._safe_text(item.get("title")), query_tokens)
        seeders = max(0, int(item.get("seeders") or 0))
        age_hours = item.get("age_hours")
        freshness = 0.0
        if isinstance(age_hours, (int, float)):
            freshness = max(0.0, min(1.0, (72.0 - float(age_hours)) / 72.0))
        uri_bonus = 0.2 if bool(item.get("has_transfer_uri")) else 0.0
        return (relevance * 5.0) + (math.log1p(seeders) * 1.4) + freshness + uri_bonus

    def _explain_strength(self, item: Dict[str, Any], sort_by: str, query_tokens: List[str]) -> str:
        reasons: List[str] = []
        seeders = int(item.get("seeders") or 0)
        age_hours = item.get("age_hours")
        size = int(item.get("size_bytes") or 0)
        match = self._match_score(self._safe_text(item.get("title")), query_tokens)
        if sort_by == "seeders":
            reasons.append(f"high seeder count ({seeders})")
        elif sort_by == "newest":
            if isinstance(age_hours, (int, float)):
                reasons.append(f"recent upload ({age_hours:.1f}h old)")
            else:
                reasons.append("very recent publish date")
        elif sort_by == "size":
            reasons.append(f"sorted by size ({self._human_size(size) or str(size) + ' B'})")
        else:
            if match >= 0.65:
                reasons.append("strong title match")
            if seeders > 0:
                reasons.append(f"{seeders} seeders")
            if isinstance(age_hours, (int, float)) and age_hours <= 24:
                reasons.append("uploaded recently")
        if not reasons:
            reasons.append("best overall rank from available data")
        return ", ".join(reasons[:3])

    def _rank_results(self, results: List[Dict[str, Any]], plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        sort_by = self._normalize_sort_by(plan.get("sort_by"))
        direction = self._normalize_sort_direction(plan.get("sort_direction"), sort_by)
        reverse = direction != "asc"
        query_tokens = self._tokenize(plan.get("search_query") or "")

        for item in results:
            item["_match"] = self._match_score(self._safe_text(item.get("title")), query_tokens)
            item["_composite"] = self._composite_score(item, query_tokens)

        if sort_by == "seeders":
            key_fn = lambda x: (int(x.get("seeders") or 0), int(x.get("publish_ts") or 0))
        elif sort_by == "newest":
            key_fn = lambda x: (int(x.get("publish_ts") or 0), int(x.get("seeders") or 0))
        elif sort_by == "size":
            key_fn = lambda x: (int(x.get("size_bytes") or 0), int(x.get("seeders") or 0))
        else:
            key_fn = lambda x: (float(x.get("_composite") or 0), int(x.get("seeders") or 0), int(x.get("publish_ts") or 0))

        ranked = sorted(results, key=key_fn, reverse=reverse)
        for idx, item in enumerate(ranked, start=1):
            item["rank"] = idx
            item["match_score"] = round(float(item.get("_match") or 0.0), 4)
            item["ranking_score"] = round(float(item.get("_composite") or 0.0), 4)
            item["strength_explanation"] = self._explain_strength(item, sort_by, query_tokens)
            item.pop("_match", None)
            item.pop("_composite", None)
            item.pop("raw_attrs", None)
        return ranked

    def _summary_for_results(self, plan: Dict[str, Any], results: List[Dict[str, Any]], selected_indexers: List[Dict[str, Any]]) -> str:
        action = plan.get("action")
        query = self._safe_text(plan.get("search_query"))
        if not results:
            if action == "recent":
                return "No recent uploads matched your Jackett filters."
            return "No Jackett results matched your request."
        top = results[0]
        scope_text = plan.get("scope", "all")
        idx_count = len(selected_indexers)
        if action == "recent":
            base = f"Found {len(results)} recent uploads"
        else:
            base = f"Found {len(results)} Jackett results"
        if query:
            base += f" for '{query}'"
        if scope_text == "all" and idx_count == 0:
            base += " across Jackett all scope (indexer metadata unavailable)."
        else:
            base += f" across {idx_count} indexer(s) ({scope_text} scope)."
        top_bits = [self._safe_text(top.get("title"), "Top result")]
        if int(top.get("seeders") or 0) > 0:
            top_bits.append(f"{int(top.get('seeders') or 0)} seeders")
        if self._safe_text(top.get("indexer")):
            top_bits.append(f"from {top.get('indexer')}")
        if self._safe_text(top.get("publish_date")):
            top_bits.append("newer-ranked")
        preview = self._top_result_preview(results, limit=10)
        if not preview:
            return f"{base} Top result: {', '.join(top_bits)}."
        lines = [f"{base} Top result: {', '.join(top_bits)}.", "Top 10 (use result_id to select):"]
        for row in preview:
            transfer = row.get("transfer_uri") or "no transfer URI"
            lines.append(
                f"{row.get('rank')}. [{row.get('result_id')}] {row.get('title')} | "
                f"{row.get('seeders')} seeders | {row.get('size') or '?'} | "
                f"{row.get('indexer') or 'unknown'} | {transfer}"
            )
        return "\n".join(lines)

    def _cache_results(self, payload: Dict[str, Any]) -> None:
        try:
            compact = {
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "action": payload.get("action"),
                "query": payload.get("query"),
                "scope": payload.get("scope"),
                "ranking": payload.get("ranking"),
                "results": (payload.get("results") or [])[:120],
            }
            redis_client.set(self.CACHE_KEY, json.dumps(compact, ensure_ascii=False), ex=self.CACHE_TTL_SECONDS)
        except Exception as exc:
            logger.debug("[jackett] cache save skipped: %s", exc)

    def _load_cached_results(self) -> List[Dict[str, Any]]:
        try:
            raw = redis_client.get(self.CACHE_KEY)
            if not raw:
                return []
            payload = json.loads(raw)
            results = payload.get("results") if isinstance(payload, dict) else []
            if isinstance(results, list):
                return [r for r in results if isinstance(r, dict)]
        except Exception:
            return []
        return []

    def _pick_result_from_reference(self, results: List[Dict[str, Any]], result_ref: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not results:
            return None
        rid = self._safe_text(result_ref.get("result_id"))
        if rid:
            for item in results:
                if self._safe_text(item.get("id")) == rid:
                    return item

        guid = self._safe_text(result_ref.get("result_guid"))
        if guid:
            for item in results:
                if self._safe_text(item.get("guid")) == guid:
                    return item

        title_ref = self._safe_text(result_ref.get("result_title")).lower()
        if title_ref:
            for item in results:
                if title_ref in self._safe_text(item.get("title")).lower():
                    return item

        try:
            idx = int(result_ref.get("index") or 0)
        except Exception:
            idx = 0
        if idx > 0 and idx <= len(results):
            return results[idx - 1]

        return results[0] if results else None

    async def _handle_list_indexers(self, plan: Dict[str, Any], indexers: List[Dict[str, Any]]) -> Dict[str, Any]:
        filtered, missing = self._resolve_indexers_for_scope(
            scope=plan.get("scope", "all"),
            requested_names=plan.get("indexer_names") or [],
            indexers=indexers,
        )
        public_count = len([i for i in filtered if i.get("privacy") == "public"])
        private_count = len([i for i in filtered if i.get("privacy") == "private"])
        summary = (
            f"Jackett has {len(filtered)} enabled indexer(s) in {plan.get('scope', 'all')} scope "
            f"({public_count} public, {private_count} private)."
        )
        if missing:
            summary += f" Not found: {', '.join(missing)}."
        return action_success(
            facts={
                "action": "list_indexers",
                "scope": plan.get("scope", "all"),
                "indexer_count": len(filtered),
                "public_count": public_count,
                "private_count": private_count,
            },
            data={
                "indexers": filtered,
                "missing_indexers": missing,
            },
            summary_for_user=summary,
            say_hint="Summarize enabled Jackett indexers and mention public/private counts.",
        )

    async def _handle_inspect_indexer(self, plan: Dict[str, Any], indexers: List[Dict[str, Any]]) -> Dict[str, Any]:
        requested = plan.get("indexer_names") or []
        if not requested:
            return action_failure(
                code="missing_indexer",
                message="No indexer name was provided to inspect.",
                needs=["Which Jackett indexer should I inspect?"],
                say_hint="Ask for a specific indexer name.",
            )
        selected, missing = self._resolve_indexers_for_scope(scope="all", requested_names=requested, indexers=indexers)
        if not selected:
            return action_failure(
                code="indexer_not_found",
                message=f"Could not find indexer: {', '.join(missing or requested)}",
                needs=["Provide a configured Jackett indexer name or id."],
                say_hint="Explain the indexer could not be found and ask for an exact configured name.",
            )
        idx = selected[0]
        summary = (
            f"Indexer '{idx.get('name')}' is configured as {idx.get('privacy', 'unknown')} "
            f"and currently {'enabled' if idx.get('enabled') else 'disabled'}."
        )
        return action_success(
            facts={
                "action": "inspect_indexer",
                "indexer": idx.get("name"),
                "indexer_id": idx.get("id"),
                "privacy": idx.get("privacy"),
                "enabled": bool(idx.get("enabled")),
            },
            data={
                "indexer": idx,
                "missing_indexers": missing,
            },
            summary_for_user=summary,
            say_hint="Report the requested indexer details from returned data.",
        )

    async def _handle_which_indexer(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        candidate_results = self._load_cached_results()

        if not candidate_results:
            return action_failure(
                code="no_result_context",
                message="No recent Jackett results are available to map back to an indexer.",
                needs=["Run a Jackett search first, then ask which indexer returned a result."],
                say_hint="Explain there are no cached results to inspect and ask to run a search first.",
            )

        selected = self._pick_result_from_reference(candidate_results, plan.get("result_ref") or {})
        if not selected:
            return action_failure(
                code="result_not_found",
                message="Could not find the requested result in available Jackett results.",
                needs=["Provide a result id or a unique title snippet from the prior search results."],
                say_hint="Ask for a clearer result reference.",
            )

        summary = (
            f"'{selected.get('title', 'Result')}' came from indexer "
            f"'{selected.get('indexer') or selected.get('indexer_id') or 'Unknown'}'."
        )
        return action_success(
            facts={
                "action": "which_indexer",
                "result_id": selected.get("id"),
                "title": selected.get("title"),
                "indexer": selected.get("indexer"),
                "indexer_id": selected.get("indexer_id"),
            },
            data={"matched_result": selected},
            summary_for_user=summary,
            say_hint="Answer with the matched result's indexer source.",
        )

    async def _handle_handoff(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        candidate_results = self._load_cached_results()

        if not candidate_results:
            return action_failure(
                code="no_handoff_result",
                message="No Jackett result is available for handoff.",
                needs=["Run a Jackett search first or pass a specific result payload."],
                say_hint="Explain a search result is required before handoff.",
            )

        selected = self._pick_result_from_reference(candidate_results, plan.get("result_ref") or {})
        if not selected:
            return action_failure(
                code="handoff_selection_failed",
                message="Could not select a Jackett result for handoff.",
                needs=["Provide result_id or result_title to choose the item to send."],
                say_hint="Ask for the specific result to hand off.",
            )

        target = self._safe_text(plan.get("target_downloader"), "downloader")
        magnet = self._safe_text(selected.get("magnet_url"))
        torrent_url = self._safe_text(selected.get("torrent_url"))
        preferred_uri, preferred_type = self._preferred_transfer_uri(selected)
        if not preferred_uri:
            return action_failure(
                code="no_transfer_uri",
                message="Selected result does not include a magnet or torrent URL for downloader handoff.",
                say_hint="Explain this result cannot be handed off because no transfer URI is available.",
            )

        handoff = {
            "target_downloader": target,
            "result_id": selected.get("id"),
            "preferred_uri": preferred_uri,
            "preferred_uri_type": preferred_type,
            "available_uris": {"magnet": magnet, "torrent": torrent_url},
            "payload": {
                "title": selected.get("title"),
                "indexer": selected.get("indexer"),
                "indexer_id": selected.get("indexer_id"),
                "details_url": selected.get("details_url"),
                "magnet_url": magnet,
                "torrent_url": torrent_url,
                "transfer_uri": preferred_uri,
                "transfer_type": preferred_type,
                "size_bytes": selected.get("size_bytes"),
                "seeders": selected.get("seeders"),
                "leechers": selected.get("leechers"),
                "category": selected.get("category"),
            },
        }
        summary = (
            f"Prepared handoff payload for '{selected.get('title')}' to {target}. "
            "A downloader plugin can now start the transfer using the included URI."
        )
        return action_success(
            facts={
                "action": "handoff",
                "target_downloader": target,
                "result_id": selected.get("id"),
                "title": selected.get("title"),
            },
            data={"handoff": handoff, "selected_result": selected},
            summary_for_user=summary,
            say_hint="Confirm handoff payload is ready and offer to pass it to the downloader plugin.",
            suggested_followups=["Want me to prepare a different result for handoff?"],
        )

    async def _run_search_or_recent(
        self,
        *,
        settings: Dict[str, Any],
        plan: Dict[str, Any],
        indexers: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        action = plan.get("action")
        search_mode = "recent" if action == "recent" else "search"
        scope = plan.get("scope", "all")
        requested = plan.get("indexer_names") or []
        selected_indexers, missing = self._resolve_indexers_for_scope(
            scope=scope,
            requested_names=requested,
            indexers=indexers,
        )

        if requested and not selected_indexers:
            return action_failure(
                code="indexers_not_found",
                message=f"Requested indexers were not found or not enabled: {', '.join(missing or requested)}",
                needs=["Use configured enabled indexers from Jackett."],
                say_hint="Explain the requested indexers are unavailable and suggest listing enabled indexers.",
            )

        use_all_endpoint = not requested and scope == "all"
        if not use_all_endpoint and not selected_indexers:
            return action_failure(
                code="no_indexers_available",
                message=f"No enabled indexers available for scope '{scope}'.",
                needs=["Enable at least one matching indexer in Jackett."],
                say_hint="Explain there are no eligible indexers for this scope.",
            )

        params: Dict[str, Any] = {"t": "recent" if search_mode == "recent" else "search"}
        if search_mode == "search" and self._safe_text(plan.get("search_query")):
            params["q"] = self._safe_text(plan.get("search_query"))
        if plan.get("categories"):
            params["cat"] = ",".join([str(c) for c in plan.get("categories")])
        if plan.get("max_age_days"):
            params["maxage"] = int(plan.get("max_age_days"))
        per_call_limit = self._clamp_int(plan.get("limit") or 25, 1, self.MAX_LIMIT, 25)
        if not use_all_endpoint and len(selected_indexers) > 1:
            per_call_limit = min(self.MAX_LIMIT, max(per_call_limit, 30))
        params["limit"] = per_call_limit

        if use_all_endpoint:
            target_ids = ["all"]
        else:
            target_ids = [self._safe_text(x.get("id")) for x in selected_indexers if self._safe_text(x.get("id"))]

        fetches = [
            self._fetch_torznab(
                settings=settings,
                indexer_id=idx_id,
                params=params,
            )
            for idx_id in target_ids
        ]
        gathered = await asyncio.gather(*fetches, return_exceptions=True)

        indexer_lookup = {}
        for idx in indexers:
            indexer_lookup[self._slug(idx.get("id"))] = idx
            indexer_lookup[self._slug(idx.get("name"))] = idx

        results: List[Dict[str, Any]] = []
        warnings: List[str] = []
        for idx_id, packet in zip(target_ids, gathered):
            if isinstance(packet, Exception):
                warnings.append(f"{idx_id}: {packet}")
                continue
            xml_list, err = packet
            if err:
                warnings.append(err)
                continue
            for xml_text in xml_list:
                parsed = self._parse_torznab_xml(
                    xml_text=xml_text,
                    fallback_indexer_id=idx_id,
                    indexer_lookup=indexer_lookup,
                )
                results.extend(parsed)

        filtered = self._filter_results(results, plan)
        ranked = self._rank_results(filtered, plan)
        ranked = ranked[: self._clamp_int(plan.get("limit") or 25, 1, self.MAX_LIMIT, 25)]

        payload = {
            "action": action,
            "query": self._safe_text(plan.get("search_query")),
            "scope": scope,
            "sort_by": self._normalize_sort_by(plan.get("sort_by")),
            "sort_direction": self._normalize_sort_direction(plan.get("sort_direction"), plan.get("sort_by")),
            "selected_indexers": selected_indexers if not use_all_endpoint else [{"id": "all", "name": "all"}],
            "result_count": len(ranked),
            "results": ranked,
            "missing_indexers": missing,
            "warnings": warnings[:10],
            "top_results": self._top_result_preview(ranked, limit=10),
            "ranking": {
                "sort_by": self._normalize_sort_by(plan.get("sort_by")),
                "sort_direction": self._normalize_sort_direction(plan.get("sort_direction"), plan.get("sort_by")),
            },
            "filters": {
                "categories": plan.get("categories") or [],
                "min_seeders": int(plan.get("min_seeders") or 0),
                "max_age_days": plan.get("max_age_days"),
                "min_size_bytes": plan.get("min_size_bytes"),
                "max_size_bytes": plan.get("max_size_bytes"),
            },
        }
        self._cache_results(payload)

        summary = self._summary_for_results(plan, ranked, selected_indexers if not use_all_endpoint else indexers)
        if warnings:
            summary += " Some indexers returned errors."

        return action_success(
            facts={
                "action": action,
                "query": self._safe_text(plan.get("search_query")),
                "scope": scope,
                "result_count": len(ranked),
                "sort_by": payload["ranking"]["sort_by"],
                "top_result_title": ranked[0]["title"] if ranked else "",
                "top_result_indexer": ranked[0]["indexer"] if ranked else "",
            },
            data=payload,
            summary_for_user=summary,
            say_hint=(
                "Summarize ranked Jackett results and list the top 10 with result_id, seeders, size, indexer, "
                "and transfer URI (magnet or torrent URL) so the user can select one for downloader handoff."
            ),
            suggested_followups=[
                "Want me to sort these by newest or seeders?",
                "Want me to prepare the best result for your downloader plugin?",
            ],
        )

    async def _handle(self, args: Dict[str, Any], llm_client, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        settings = self._settings()
        diagnosis = self._diagnosis()
        if not settings["api_key"] or not settings["base_url"]:
            needs = needs_from_diagnosis(
                diagnosis,
                {
                    "jackett_base_url": "Set JACKETT_BASE_URL in Jackett plugin settings.",
                    "jackett_api_key": "Set JACKETT_API_KEY in Jackett plugin settings.",
                },
            )
            return action_failure(
                code="jackett_not_configured",
                message="Jackett is not configured yet.",
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain Jackett settings are missing and ask for base URL/API key.",
            )

        query_text = self._query_from_args(args or {}, context=context)
        if not query_text:
            return action_failure(
                code="missing_query",
                message="No natural-language request was provided.",
                needs=["Tell me what to do in Jackett (search, recent, list indexers, inspect, source lookup, or handoff)."],
                say_hint="Ask for the Jackett request in natural language.",
            )

        indexers, indexer_error = await self._fetch_indexers(settings)
        plan = await self._resolve_plan(
            query_text=query_text,
            args=args or {},
            llm_client=llm_client,
            known_indexers=indexers,
            default_limit=settings["default_limit"],
        )

        if plan.get("action") in {"list_indexers", "inspect_indexer"} and indexer_error:
            return action_failure(
                code="jackett_indexers_unavailable",
                message=indexer_error,
                diagnosis=diagnosis,
                say_hint="Explain indexer listing failed and suggest checking Jackett connectivity.",
            )

        if plan.get("action") == "list_indexers":
            return await self._handle_list_indexers(plan, indexers)
        if plan.get("action") == "inspect_indexer":
            return await self._handle_inspect_indexer(plan, indexers)
        if plan.get("action") == "which_indexer":
            return await self._handle_which_indexer(plan)
        if plan.get("action") == "handoff":
            return await self._handle_handoff(plan)

        if plan.get("action") == "search" and not self._safe_text(plan.get("search_query")):
            return action_failure(
                code="missing_query",
                message="No search terms were provided for Jackett search.",
                needs=["What should I search for in Jackett?"],
                say_hint="Ask the user what torrent query to run.",
            )

        if indexer_error and (plan.get("scope") != "all" or plan.get("indexer_names")):
            return action_failure(
                code="jackett_scope_requires_indexers",
                message=(
                    "Scoped indexer search needs Jackett indexer metadata, but listing indexers failed. "
                    f"Error: {indexer_error}"
                ),
                say_hint="Explain scoped filtering needs indexer metadata and ask to verify Jackett connectivity.",
            )

        return await self._run_search_or_recent(
            settings=settings,
            plan=plan,
            indexers=indexers,
        )

    # Platform shims
    async def handle_discord(self, message, args, llm_client):
        ctx = {}
        content = getattr(message, "content", None)
        if isinstance(content, str) and content.strip():
            ctx["request_text"] = content.strip()
        try:
            return await self._handle(args or {}, llm_client, context=ctx)
        except Exception as exc:
            logger.exception("[jackett] handle_discord error: %s", exc)
            return action_failure(code="jackett_exception", message=f"Jackett request failed: {exc}")

    async def handle_webui(self, args, llm_client):
        try:
            return await self._handle(args or {}, llm_client, context={})
        except Exception as exc:
            logger.exception("[jackett] handle_webui error: %s", exc)
            return action_failure(code="jackett_exception", message=f"Jackett request failed: {exc}")

    async def handle_macos(self, args, llm_client, context=None):
        try:
            return await self._handle(args or {}, llm_client, context=context or {})
        except Exception as exc:
            logger.exception("[jackett] handle_macos error: %s", exc)
            return action_failure(code="jackett_exception", message=f"Jackett request failed: {exc}")

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        ctx = {}
        if isinstance(raw_message, str) and raw_message.strip():
            ctx["request_text"] = raw_message.strip()
        try:
            return await self._handle(args or {}, llm_client, context=ctx)
        except Exception as exc:
            logger.exception("[jackett] handle_irc error: %s", exc)
            return action_failure(code="jackett_exception", message=f"Jackett request failed: {exc}")

    async def handle_homeassistant(self, args, llm_client):
        try:
            return await self._handle(args or {}, llm_client, context={})
        except Exception as exc:
            logger.exception("[jackett] handle_homeassistant error: %s", exc)
            return action_failure(code="jackett_exception", message="Jackett request failed.")

    async def handle_homekit(self, args, llm_client):
        try:
            return await self._handle(args or {}, llm_client, context={})
        except Exception as exc:
            logger.exception("[jackett] handle_homekit error: %s", exc)
            return action_failure(code="jackett_exception", message="Jackett request failed.")

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        ctx = {}
        if isinstance(body, str) and body.strip():
            ctx["request_text"] = body.strip()
        try:
            return await self._handle(args or {}, llm_client, context=ctx)
        except Exception as exc:
            logger.exception("[jackett] handle_matrix error: %s", exc)
            return action_failure(code="jackett_exception", message=f"Jackett request failed: {exc}")

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
            return await self._handle(args or {}, llm_client, context=ctx)
        except Exception as exc:
            logger.exception("[jackett] handle_telegram error: %s", exc)
            return action_failure(code="jackett_exception", message=f"Jackett request failed: {exc}")

    async def handle_xbmc(self, args, llm_client):
        try:
            return await self._handle(args or {}, llm_client, context={})
        except Exception as exc:
            logger.exception("[jackett] handle_xbmc error: %s", exc)
            return action_failure(code="jackett_exception", message="Jackett request failed.")


verba = JackettSearchTorrentsPlugin()
