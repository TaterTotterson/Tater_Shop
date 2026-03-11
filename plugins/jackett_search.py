import asyncio
import hashlib
import json
import logging
import math
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

import httpx

from helpers import extract_json, redis_client
from plugin_base import ToolPlugin
from plugin_diagnostics import combine_diagnosis, diagnose_hash_fields, needs_from_diagnosis
from plugin_result import action_failure, action_success

logger = logging.getLogger("jackett_search")
logger.setLevel(logging.INFO)


class JackettSearchPlugin(ToolPlugin):
    name = "jackett_search"
    plugin_name = "Jackett Search"
    version = "1.0.0"
    min_tater_version = "59"
    pretty_name = "Jackett Discovery"
    settings_category = "Jackett"
    usage = (
        '{"function":"jackett_search","arguments":{"query":"find Ubuntu 24.04 torrents from public trackers"}}'
    )
    description = (
        "Natural-language Jackett torrent discovery bridge with search, recent uploads, "
        "indexer browsing, ranking, and downloader handoff payloads."
    )
    plugin_dec = "Search Jackett, browse recent uploads, inspect indexers, and return structured torrent results."
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
        "Use when the user asks to search torrents, browse recent uploads, inspect Jackett indexers, "
        "rank Jackett results, identify source indexers, or prepare downloader handoff payloads."
    )
    how_to_use = (
        "Pass a natural-language query in query. Optional structured arguments: action, search_query, scope, "
        "indexers, categories, sort_by, min_seeders, max_age_days, min_size, max_size, limit, result_id, "
        "result_title, and target_downloader."
    )
    common_needs = ["A natural-language Jackett request."]
    missing_info_prompts = ["What should I search for in Jackett?"]
    example_calls = [
        '{"function":"jackett_search","arguments":{"query":"search Jackett for Ubuntu 24.04 torrents"}}',
        '{"function":"jackett_search","arguments":{"query":"show me the newest anime uploads"}}',
        '{"function":"jackett_search","arguments":{"query":"list all enabled Jackett indexers"}}',
        '{"function":"jackett_search","arguments":{"query":"send the best result to qBittorrent"}}',
    ]

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
            "plugin_settings:Jackett",
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
        raw = redis_client.hgetall("plugin_settings:Jackett") or {}
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
        if text in {"search", "recent", "list_indexers", "inspect_indexer", "which_indexer", "handoff"}:
            return text
        if text in {"list", "indexers", "listindexers"}:
            return "list_indexers"
        if text in {"inspect", "indexer_details", "indexer_info"}:
            return "inspect_indexer"
        if text in {"source", "find_source"}:
            return "which_indexer"
        if text in {"send", "handoff_prepare", "dispatch"}:
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

    def _extract_categories(self, values: Any, query_text: str) -> List[int]:
        items: List[str] = []
        if isinstance(values, list):
            items.extend([self._safe_text(v) for v in values if self._safe_text(v)])
        elif isinstance(values, str):
            items.extend([x.strip() for x in re.split(r"[,;/]+", values) if x.strip()])
        elif values is not None:
            items.append(self._safe_text(values))

        for token in self._tokenize(query_text):
            if token in self.CATEGORY_KEYWORDS:
                items.append(token)

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

    def _extract_limit(self, args: Dict[str, Any], query_text: str, default_limit: int) -> int:
        for key in ("limit", "max_results", "top_n", "count"):
            if key in args:
                return self._clamp_int(args.get(key), 1, self.MAX_LIMIT, default_limit)
        m = re.search(r"\b(?:top|first|show|return)\s+(\d{1,3})\b", query_text.lower())
        if m:
            return self._clamp_int(m.group(1), 1, self.MAX_LIMIT, default_limit)
        return self._clamp_int(default_limit, 1, self.MAX_LIMIT, default_limit)

    @staticmethod
    def _extract_min_seeders(args: Dict[str, Any], query_text: str) -> int:
        for key in ("min_seeders", "seeders_min", "minimum_seeds"):
            value = args.get(key)
            if value is not None:
                try:
                    return max(0, int(value))
                except Exception:
                    pass
        m = re.search(r"\b(?:at least|min(?:imum)?|over|more than)\s+(\d+)\s*(?:seeds|seeders)\b", query_text.lower())
        if m:
            return max(0, int(m.group(1)))
        return 0

    @staticmethod
    def _extract_max_age_days(args: Dict[str, Any], query_text: str) -> Optional[int]:
        for key in ("max_age_days", "age_days", "days"):
            value = args.get(key)
            if value is not None:
                try:
                    days = int(value)
                    return days if days > 0 else None
                except Exception:
                    pass
        text = query_text.lower()
        if "today" in text or "last 24 hours" in text:
            return 1
        if "yesterday" in text:
            return 2
        if "this week" in text:
            return 7
        m = re.search(r"\blast\s+(\d+)\s+days?\b", text)
        if m:
            days = int(m.group(1))
            return days if days > 0 else None
        return None

    def _extract_size_filters(self, args: Dict[str, Any], query_text: str) -> Tuple[Optional[int], Optional[int]]:
        min_size: Optional[int] = None
        max_size: Optional[int] = None
        for key in ("min_size", "min_size_bytes"):
            if args.get(key) is not None:
                value = args.get(key)
                try:
                    min_size = int(value)
                except Exception:
                    min_size = self._parse_size_to_bytes(str(value))
        for key in ("max_size", "max_size_bytes"):
            if args.get(key) is not None:
                value = args.get(key)
                try:
                    max_size = int(value)
                except Exception:
                    max_size = self._parse_size_to_bytes(str(value))
        text = query_text.lower()
        if min_size is None:
            m = re.search(r"\b(?:over|bigger than|larger than|at least)\s+(\d+(?:\.\d+)?\s*(?:kb|mb|gb|tb|kib|mib|gib|tib))\b", text)
            if m:
                min_size = self._parse_size_to_bytes(m.group(1))
        if max_size is None:
            m = re.search(r"\b(?:under|below|smaller than|at most)\s+(\d+(?:\.\d+)?\s*(?:kb|mb|gb|tb|kib|mib|gib|tib))\b", text)
            if m:
                max_size = self._parse_size_to_bytes(m.group(1))
        return min_size, max_size

    def _infer_action_from_query(self, query_text: str) -> str:
        text = query_text.lower()
        if re.search(r"\b(send|hand off|handoff|dispatch)\b.*\b(qbittorrent|qb\b|transmission|deluge|rtorrent|download)", text):
            return "handoff"
        if re.search(r"\b(which indexer|what indexer|which tracker|what tracker)\b", text):
            return "which_indexer"
        if re.search(r"\b(show|list|what)\b.*\b(indexers|trackers)\b", text):
            return "list_indexers"
        if re.search(r"\b(indexer details|inspect indexer|details for indexer|about indexer)\b", text):
            return "inspect_indexer"
        if re.search(r"\b(recent|newest|latest|what('?s| is) new|new uploads|recent uploads)\b", text):
            return "recent"
        return "search"

    def _infer_scope_from_query(self, query_text: str) -> str:
        text = query_text.lower()
        if re.search(r"\b(public(?:\s+trackers?)?|public-only|only public)\b", text):
            return "public"
        if re.search(r"\b(private(?:\s+trackers?)?|private-only|only private)\b", text):
            return "private"
        return "all"

    def _infer_sort_from_query(self, query_text: str) -> Tuple[str, str]:
        text = query_text.lower()
        if re.search(r"\b(best seeded|most seeded|highest seeds?|top seeded)\b", text):
            return "seeders", "desc"
        if re.search(r"\b(sort by seeds?|seeders?)\b", text):
            return "seeders", "desc"
        if re.search(r"\b(newest|latest|most recent)\b", text):
            return "newest", "desc"
        if re.search(r"\b(sort by size|largest|biggest)\b", text):
            return "size", "desc"
        if re.search(r"\b(smallest|small files)\b", text):
            return "size", "asc"
        if re.search(r"\b(best match|best result)\b", text):
            return "best_match", "desc"
        return "best_match", "desc"

    def _extract_downloader_target(self, args: Dict[str, Any], query_text: str) -> str:
        for key in ("target_downloader", "downloader", "target_client", "target"):
            value = self._safe_text(args.get(key))
            if value:
                return value
        text = query_text.lower()
        for candidate in ("qbittorrent", "transmission", "deluge", "rtorrent", "aria2", "premiumize"):
            if candidate in text:
                return candidate
        if "qb" in text:
            return "qbittorrent"
        return "downloader"

    def _extract_result_reference(self, args: Dict[str, Any], query_text: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key in ("result_id", "result_title", "result_guid", "selection", "index"):
            if args.get(key) is not None:
                out[key] = args.get(key)
        quoted = re.findall(r"['\"]([^'\"]{2,240})['\"]", query_text or "")
        if quoted and "result_title" not in out:
            out["result_title"] = quoted[0].strip()

        for word, rank in self.ORDINAL_MAP.items():
            if re.search(rf"\b{word}\b", (query_text or "").lower()):
                out.setdefault("index", rank)
                break
        m = re.search(r"\b(\d{1,3})(?:st|nd|rd|th)\b", (query_text or "").lower())
        if m:
            out.setdefault("index", int(m.group(1)))
        if re.search(r"\b(best|top)\b", (query_text or "").lower()):
            out.setdefault("index", 1)
        return out

    def _extract_indexer_mentions(self, args: Dict[str, Any], query_text: str, indexers: List[Dict[str, Any]]) -> List[str]:
        mentioned: List[str] = []
        raw = args.get("indexers") or args.get("indexer") or args.get("trackers") or args.get("tracker")
        if isinstance(raw, list):
            mentioned.extend([self._safe_text(x) for x in raw if self._safe_text(x)])
        elif raw is not None:
            text = self._safe_text(raw)
            if text:
                mentioned.extend([part.strip() for part in re.split(r"[,;/]+", text) if part.strip()])

        if query_text:
            m = re.search(r"\b(?:indexer|tracker)\s+([a-z0-9][a-z0-9 _\-.]{1,60})", query_text.lower())
            if m:
                mentioned.append(m.group(1).strip())

        if not mentioned and indexers and query_text:
            qslug = self._slug(query_text)
            for idx in indexers:
                sid = self._slug(idx.get("id"))
                sname = self._slug(idx.get("name"))
                if sid and sid in qslug:
                    mentioned.append(idx.get("id"))
                    continue
                if sname and sname in qslug:
                    mentioned.append(idx.get("name"))
        dedup: List[str] = []
        seen = set()
        for item in mentioned:
            key = self._slug(item)
            if not key or key in seen:
                continue
            seen.add(key)
            dedup.append(item)
        return dedup

    def _extract_search_terms(self, query_text: str, action: str, explicit_search: str = "") -> str:
        if explicit_search:
            return explicit_search.strip()
        text = str(query_text or "").strip()
        if not text:
            return ""
        lower = text.lower()
        if action in {"list_indexers", "inspect_indexer", "which_indexer", "handoff"}:
            return ""
        if action == "recent":
            recent_patterns = [
                r"^(?:show|find|get)\s+(?:me\s+)?(?:the\s+)?(?:newest|latest|recent)\s+",
                r"^(?:what(?:'s| is)\s+new(?:\s+today)?)\s*",
            ]
            candidate = lower
            for pattern in recent_patterns:
                candidate = re.sub(pattern, "", candidate).strip()
            candidate = re.sub(r"\b(?:uploads?|torrents?)\b", "", candidate).strip()
            return re.sub(r"\s+", " ", candidate)

        match = re.search(r"\b(?:search|find|look up|lookup|show)\b(?:\s+jackett)?\s+(?:for\s+)?(.+)$", lower)
        candidate = match.group(1).strip() if match else lower
        candidate = re.sub(
            r"\b(?:from|on)\s+(?:all\s+)?(?:public|private)?\s*(?:indexers?|trackers?)\b.*$",
            "",
            candidate,
        ).strip()
        candidate = re.sub(r"\b(?:torrent|torrents|uploads?)\b", "", candidate).strip()
        candidate = re.sub(r"\s+", " ", candidate).strip(" .,!?:;")
        return candidate

    async def _plan_from_llm(
        self,
        *,
        query_text: str,
        llm_client: Any,
        known_indexers: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not llm_client or not query_text:
            return {}
        known = [self._safe_text(x.get("name") or x.get("id")) for x in known_indexers[:80]]
        prompt = (
            "You translate Jackett user requests into JSON only.\n"
            "Return one JSON object with keys:\n"
            "action(search|recent|list_indexers|inspect_indexer|which_indexer|handoff),\n"
            "search_query, scope(all|public|private), indexers(array), categories(array of words or IDs),\n"
            "sort_by(best_match|seeders|newest|size), sort_direction(desc|asc),\n"
            "min_seeders(number), max_age_days(number), min_size, max_size, limit(number),\n"
            "result_id, result_title, target_downloader.\n"
            "Only include indexers from known_indexers.\n"
            "If a field is unknown, use empty string, 0, null, or [].\n"
            f"known_indexers={json.dumps(known)}\n"
            f"user_request={json.dumps(query_text)}\n"
        )
        try:
            resp = await llm_client.chat(
                messages=[{"role": "system", "content": prompt}],
                max_tokens=260,
                temperature=0,
            )
            raw = ((resp or {}).get("message") or {}).get("content") or ""
            payload = json.loads(extract_json(raw) or raw)
            return payload if isinstance(payload, dict) else {}
        except Exception as exc:
            logger.debug("[jackett] LLM plan fallback: %s", exc)
            return {}

    async def _resolve_plan(
        self,
        *,
        args: Dict[str, Any],
        query_text: str,
        llm_client: Any,
        known_indexers: List[Dict[str, Any]],
        default_limit: int,
    ) -> Dict[str, Any]:
        args = args or {}
        llm_plan = await self._plan_from_llm(query_text=query_text, llm_client=llm_client, known_indexers=known_indexers)

        explicit_action = self._normalize_action(args.get("action") or args.get("mode") or args.get("operation"))
        has_explicit_action = any(
            args.get(key) is not None and self._safe_text(args.get(key)) != ""
            for key in ("action", "mode", "operation")
        )
        inferred_action = self._infer_action_from_query(query_text)
        llm_action = self._normalize_action(llm_plan.get("action"))
        action = explicit_action if has_explicit_action else (
            llm_action if llm_action != "search" or inferred_action == "search" else inferred_action
        )
        if not action:
            action = inferred_action

        explicit_scope = self._normalize_scope(args.get("scope") or args.get("indexer_scope"))
        has_explicit_scope = any(
            args.get(key) is not None and self._safe_text(args.get(key)) != ""
            for key in ("scope", "indexer_scope")
        )
        llm_scope = self._normalize_scope(llm_plan.get("scope"))
        inferred_scope = self._infer_scope_from_query(query_text)
        scope = explicit_scope if has_explicit_scope else (llm_scope if llm_plan.get("scope") else inferred_scope)

        explicit_sort = self._normalize_sort_by(args.get("sort_by") or args.get("sort"))
        explicit_direction = self._normalize_sort_direction(args.get("sort_direction") or args.get("order"), explicit_sort)
        has_explicit_sort = any(
            args.get(key) is not None and self._safe_text(args.get(key)) != ""
            for key in ("sort_by", "sort")
        )
        has_explicit_direction = any(
            args.get(key) is not None and self._safe_text(args.get(key)) != ""
            for key in ("sort_direction", "order")
        )
        llm_sort = self._normalize_sort_by(llm_plan.get("sort_by"))
        llm_direction = self._normalize_sort_direction(llm_plan.get("sort_direction"), llm_sort)
        inferred_sort, inferred_direction = self._infer_sort_from_query(query_text)
        sort_by = explicit_sort if has_explicit_sort else (
            llm_sort if llm_plan.get("sort_by") else inferred_sort
        )
        sort_direction = explicit_direction if has_explicit_direction else (
            llm_direction if llm_plan.get("sort_direction") else inferred_direction
        )

        explicit_search = self._safe_text(args.get("search_query") or args.get("search") or args.get("q"))
        llm_search = self._safe_text(llm_plan.get("search_query"))
        search_query = self._extract_search_terms(query_text, action, explicit_search=explicit_search or llm_search)

        category_values = args.get("categories", None)
        if category_values is None:
            category_values = llm_plan.get("categories")
        categories = self._extract_categories(category_values, query_text)

        limit = self._extract_limit(
            args={"limit": args.get("limit", llm_plan.get("limit"))},
            query_text=query_text,
            default_limit=default_limit,
        )
        min_seeders = self._extract_min_seeders(
            {"min_seeders": args.get("min_seeders", llm_plan.get("min_seeders"))},
            query_text,
        )
        max_age_days = self._extract_max_age_days(
            {"max_age_days": args.get("max_age_days", llm_plan.get("max_age_days"))},
            query_text,
        )
        min_size, max_size = self._extract_size_filters(
            {
                "min_size": args.get("min_size", llm_plan.get("min_size")),
                "max_size": args.get("max_size", llm_plan.get("max_size")),
            },
            query_text,
        )

        indexers = self._extract_indexer_mentions(
            {
                "indexers": args.get("indexers") or llm_plan.get("indexers"),
                "indexer": args.get("indexer"),
                "tracker": args.get("tracker"),
            },
            query_text,
            known_indexers,
        )

        result_ref = self._extract_result_reference(
            {
                "result_id": args.get("result_id", llm_plan.get("result_id")),
                "result_title": args.get("result_title", llm_plan.get("result_title")),
                "result_guid": args.get("result_guid"),
                "selection": args.get("selection"),
                "index": args.get("index"),
            },
            query_text,
        )

        target_downloader = self._extract_downloader_target(
            {"target_downloader": args.get("target_downloader", llm_plan.get("target_downloader"))},
            query_text,
        )

        # Interpret requests like "indexer details for nyaa" as inspect_indexer.
        if action == "list_indexers" and indexers and re.search(r"\b(details?|inspect|about)\b", query_text.lower()):
            action = "inspect_indexer"

        if action in {"which_indexer", "handoff"} and not result_ref and not args.get("results"):
            result_ref = self._extract_result_reference({}, query_text)

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
        for attr in item.findall("torznab:attr", JackettSearchPlugin.TORZNAB_NS):
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

        magnet_url = self._safe_text(attrs.get("magneturl"), "")
        torrent_url = self._safe_text(attrs.get("downloadurl"), "")
        details_url = comments or ""

        if link.startswith("magnet:?") and not magnet_url:
            magnet_url = link
        elif link.startswith("http") and not torrent_url:
            torrent_url = link

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
        normalized["handoff"] = {
            "title": normalized["title"],
            "indexer": normalized["indexer"],
            "indexer_id": normalized["indexer_id"],
            "magnet_url": normalized["magnet_url"],
            "torrent_url": normalized["torrent_url"],
            "details_url": normalized["details_url"],
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
        uri_bonus = 0.2 if self._safe_text(item.get("magnet_url") or item.get("torrent_url")) else 0.0
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
        base += f" across {idx_count} indexer(s) ({scope_text} scope)."
        top_bits = [self._safe_text(top.get("title"), "Top result")]
        if int(top.get("seeders") or 0) > 0:
            top_bits.append(f"{int(top.get('seeders') or 0)} seeders")
        if self._safe_text(top.get("indexer")):
            top_bits.append(f"from {top.get('indexer')}")
        if self._safe_text(top.get("publish_date")):
            top_bits.append("newer-ranked")
        return f"{base} Top result: {', '.join(top_bits)}."

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

    async def _handle_which_indexer(self, args: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        candidate_results = []
        if isinstance(args.get("results"), list):
            candidate_results = [x for x in args.get("results") if isinstance(x, dict)]
        elif isinstance(args.get("result"), dict):
            candidate_results = [args.get("result")]
        if not candidate_results:
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

    async def _handle_handoff(self, args: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        candidate_results = []
        if isinstance(args.get("results"), list):
            candidate_results = [x for x in args.get("results") if isinstance(x, dict)]
        elif isinstance(args.get("result"), dict):
            candidate_results = [args.get("result")]
        if not candidate_results:
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
        preferred_uri = magnet or torrent_url
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
            "available_uris": {"magnet": magnet, "torrent": torrent_url},
            "payload": {
                "title": selected.get("title"),
                "indexer": selected.get("indexer"),
                "indexer_id": selected.get("indexer_id"),
                "details_url": selected.get("details_url"),
                "magnet_url": magnet,
                "torrent_url": torrent_url,
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
                "Summarize ranked Jackett results, explain the strongest match, and mention "
                "that results include magnet/torrent URLs for downloader handoff."
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
        indexers, indexer_error = await self._fetch_indexers(settings)
        plan = await self._resolve_plan(
            args=args or {},
            query_text=query_text,
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
            return await self._handle_which_indexer(args or {}, plan)
        if plan.get("action") == "handoff":
            return await self._handle_handoff(args or {}, plan)

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


plugin = JackettSearchPlugin()
