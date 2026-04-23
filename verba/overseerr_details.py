# verba/overseerr_details.py
import json
import asyncio
import logging
import requests
import re
from urllib.parse import quote

from dotenv import load_dotenv
from verba_base import ToolVerba
from helpers import redis_client, extract_json
from verba_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("overseerr_details")
logger.setLevel(logging.INFO)


class OverseerrDetailsPlugin(ToolVerba):
    name = "overseerr_details"
    verba_name = "Overseerr Details"
    version = "1.2.3"
    min_tater_version = "59"
    pretty_name = "Overseerr: Title Details"
    settings_category = "Overseerr"

    usage = (
        '{"function":"overseerr_details","arguments":{"query":"ONE natural-language title details request '
        '(for example: tell me about severance, get details for dune part two, show me info on the movie alien)."}}'
    )

    description = (
        "Get details for one movie or TV show from Overseerr from one natural-language request."
    )
    verba_dec = "Fetch details for a specific movie or TV show from Overseerr."
    waiting_prompt_template = (
        "Give {mention} a short, cheerful note that you’re fetching details from Overseerr now. "
        "Only output that message."
    )
    platforms = ['discord', 'webui', 'macos', 'irc', 'meshtastic', 'voice_core', 'homeassistant', 'matrix', 'homekit', 'telegram']

    required_settings = {
        "OVERSEERR_BASE_URL": {
            "label": "Overseerr Base URL (e.g., http://overseerr.local:5055)",
            "type": "string",
            "default": "http://localhost:5055",
        },
        "OVERSEERR_API_KEY": {
            "label": "Overseerr API Key",
            "type": "password",
            "default": "",
        },
    }
    when_to_use = "Use when the user asks for details, info, synopsis, rating, or status for a specific movie or TV show."
    how_to_use = (
        "Pass one natural-language request in query. Include the title naturally. "
        "Mention movie or show only when that distinction matters."
    )
    common_needs = ["A natural-language title lookup request."]
    missing_info_prompts = []
    example_calls = [
        '{"function":"overseerr_details","arguments":{"query":"tell me about severance"}}',
        '{"function":"overseerr_details","arguments":{"query":"get details for dune part two"}}',
        '{"function":"overseerr_details","arguments":{"query":"show me info on the movie alien"}}',
        '{"function":"overseerr_details","arguments":{"query":"give me details for the tv show the bear"}}',
    ]


    # ---------- Internals ----------
    @staticmethod
    def _get_settings():
        s = redis_client.hgetall("verba_settings:Overseerr") or {}

        def _val(k, default=""):
            v = s.get(k, default)
            return v.decode() if isinstance(v, (bytes, bytearray)) else v

        base = (_val("OVERSEERR_BASE_URL", "http://localhost:5055") or "http://localhost:5055").rstrip("/")
        api = _val("OVERSEERR_API_KEY", "")
        return base, api

    def _search(self, title: str):
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        raw_title = re.sub(r"\s+", " ", str(title or "").strip())
        if not raw_title:
            return {"error": "No title provided for search."}

        # Overseerr can return HTTP 400 on unescaped search strings; always encode.
        encoded_query = quote(raw_title, safe="")
        url = f"{base}/api/v1/search?query={encoded_query}"
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        try:
            resp = requests.get(url, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error(f"[Overseerr search] HTTP {resp.status_code} :: {resp.text}")
                return {"error": f"Overseerr returned HTTP {resp.status_code} for search."}
            payload = resp.json() or {}
            if isinstance(payload, dict):
                return {"results": payload.get("results") or []}
            return {"results": []}
        except Exception as e:
            logger.exception("[Overseerr search error] %s", e)
            return {"error": f"Failed to search Overseerr: {e}"}

    def _fetch_details(self, media_type: str, tmdb_id: int):
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        media_type = (media_type or "").lower().strip()
        endpoint = "movie" if media_type == "movie" else "tv"

        url = f"{base}/api/v1/{endpoint}/{int(tmdb_id)}"
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        try:
            resp = requests.get(url, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error(f"[Overseerr detail {endpoint}] HTTP {resp.status_code} :: {resp.text}")
                return {"error": f"Overseerr returned HTTP {resp.status_code} for details."}
            return resp.json() or {}
        except Exception as e:
            logger.exception("[Overseerr details fetch error] %s", e)
            return {"error": f"Failed to fetch details: {e}"}

    @staticmethod
    def _pick_result(search_data: dict, prefer_type: str | None):
        results = (search_data or {}).get("results") or []
        if not results:
            return None

        prefer_type = (prefer_type or "").lower().strip()
        if prefer_type in {"movie", "tv"}:
            for r in results:
                mt = (r.get("mediaType") or r.get("media_type") or "").lower()
                if mt == prefer_type:
                    return r

        return results[0]

    @staticmethod
    def _query_from_args(args: dict | None) -> str:
        data = args or {}
        for key in ("query", "request", "text", "message", "content", "prompt"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        title = data.get("title")
        if isinstance(title, str) and title.strip():
            return title.strip()
        return ""

    @staticmethod
    def _normalize_media_type(value: str) -> str:
        raw = str(value or "").strip().lower()
        if raw in {"movie", "film"}:
            return "movie"
        if raw in {"tv", "show", "series"}:
            return "tv"
        return ""

    def _infer_media_type_from_query(self, query: str, explicit: str = "") -> str:
        normalized = self._normalize_media_type(explicit)
        if normalized:
            return normalized
        text = re.sub(r"\s+", " ", str(query or "").strip().lower())
        if not text:
            return ""
        if re.search(r"\b(tv|show|series|season|episode|miniseries|anime)\b", text):
            return "tv"
        if re.search(r"\b(movie|film|cinema)\b", text):
            return "movie"
        return ""

    @staticmethod
    def _clean_title_candidate(text: str) -> str:
        value = re.sub(r"\s+", " ", str(text or "").strip())
        if not value:
            return ""
        quoted = re.findall(r"['\"]([^'\"]{2,160})['\"]", value)
        if quoted:
            return quoted[0].strip()
        value = re.sub(
            r"(?i)^(?:please\s+)?(?:can you\s+|could you\s+|will you\s+)?"
            r"(?:tell me about|get details for|get details on|show me details for|show me info on|"
            r"show me info about|give me details for|give me info on|look up|search for|find info on|"
            r"find details for|what can you tell me about|what do you know about)\s+",
            "",
            value,
        )
        value = re.sub(r"(?i)\b(?:on|in)\s+overseerr\b", "", value).strip(" .,!?:;")
        value = re.sub(r"(?i)^(?:the\s+)?(?:movie|film|tv show|show|series)\s+", "", value).strip()
        return value[:160]

    async def _parse_request_with_llm(self, query: str, llm_client):
        text = re.sub(r"\s+", " ", str(query or "").strip())
        if not text or not llm_client:
            return "", ""
        prompt = (
            "Extract the ONE movie or TV title the user wants details about.\n"
            "Return only JSON with this shape: {\"title\":\"\",\"media_type\":\"movie|tv|\"}\n"
            "Rules:\n"
            "1) title must contain only the actual title, without filler words.\n"
            "2) media_type must be movie or tv only when the request clearly specifies it.\n"
            "3) If unclear, leave media_type empty.\n"
            "4) If no clear title is present, return an empty title.\n\n"
            f'User request: "{text}"\n'
        )
        try:
            resp = await llm_client.chat(
                messages=[{"role": "system", "content": prompt}],
                max_tokens=120,
                temperature=0,
            )
            raw = ((resp or {}).get("message") or {}).get("content") or ""
            data = json.loads(extract_json(raw) or raw)
            title = self._clean_title_candidate(str(data.get("title") or ""))
            media_type = self._normalize_media_type(str(data.get("media_type") or ""))
            return title, media_type
        except Exception as e:
            logger.warning("[overseerr_details] request parse fallback: %s", e)
            return "", ""

    async def _resolve_request(self, args: dict | None, llm_client):
        data = args or {}
        explicit_title = self._clean_title_candidate(str(data.get("title") or ""))
        explicit_media_type = self._normalize_media_type(str(data.get("media_type") or ""))
        if explicit_title:
            return explicit_title, self._infer_media_type_from_query(explicit_title, explicit_media_type)

        query = self._query_from_args(data)
        if not query:
            return "", ""

        parsed_title, parsed_media_type = await self._parse_request_with_llm(query, llm_client)
        title = parsed_title or self._clean_title_candidate(query)
        media_type = parsed_media_type or self._infer_media_type_from_query(query, explicit_media_type)
        return title, media_type

    @staticmethod
    def _tame_text(text: str, max_len: int = 700) -> str:
        text = re.sub(r"\s+", " ", (text or "").strip())
        return text[:max_len]

    @staticmethod
    def _fallback_details_text(detail: dict) -> str:
        title = (
            detail.get("title")
            or detail.get("name")
            or detail.get("originalTitle")
            or detail.get("originalName")
            or "Unknown title"
        )
        year = (detail.get("releaseDate") or detail.get("firstAirDate") or "")[:4]
        rating = detail.get("voteAverage")
        media_info = detail.get("mediaInfo")
        media_info = media_info if isinstance(media_info, dict) else {}
        media_status = detail.get("status") or media_info.get("status")
        runtime = detail.get("runtime")
        overview = re.sub(r"\s+", " ", str(detail.get("overview") or "").strip())

        heading = f"{title} ({year})" if year else str(title)
        facts = []
        if isinstance(rating, (int, float)):
            facts.append(f"Rating: {float(rating):.1f}/10")
        if media_status:
            facts.append(f"Status: {media_status}")
        if isinstance(runtime, int) and runtime > 0:
            facts.append(f"Runtime: {runtime} min")
        genres = detail.get("genres") or []
        if isinstance(genres, list) and genres:
            names = [g.get("name") for g in genres if isinstance(g, dict) and g.get("name")]
            if names:
                facts.append("Genres: " + ", ".join(names[:4]))

        lines = [heading]
        if overview:
            lines.append(overview[:420])
        lines.extend([f"- {item}" for item in facts[:6]])
        return "\n".join(lines).strip()

    async def _ask_llm_details(self, detail: dict, title: str, llm_client):
        return self._fallback_details_text(detail)

    async def _answer(self, args, llm_client):
        args = args or {}
        title, prefer_type = await self._resolve_request(args, llm_client)
        if not title:
            return action_failure(
                code="missing_title",
                message="Tell me the movie or show title you want details for.",
                needs=["Provide the movie or TV title."],
                say_hint="Ask for the specific title to look up.",
            )

        search_data = self._search(title)
        if "error" in search_data:
            return action_failure(
                code="overseerr_search_failed",
                message=str(search_data["error"]),
                say_hint="Explain the Overseerr search failed and suggest checking settings.",
            )

        picked = self._pick_result(search_data, prefer_type)
        if not picked:
            return action_failure(
                code="title_not_found",
                message=f"I couldn’t find '{title}' in Overseerr.",
                say_hint="Explain no matching title was found in Overseerr.",
            )

        tmdb_id = picked.get("id")
        media_type = (picked.get("mediaType") or picked.get("media_type") or prefer_type or "movie").lower()

        if not tmdb_id:
            return action_failure(
                code="title_id_missing",
                message=f"I couldn’t resolve an ID for '{title}'.",
                say_hint="Explain that Overseerr did not return a resolvable title ID.",
            )

        detail = self._fetch_details(media_type, int(tmdb_id))
        if "error" in detail:
            return action_failure(
                code="overseerr_details_failed",
                message=str(detail["error"]),
                say_hint="Explain fetching title details failed.",
            )

        summary = await self._ask_llm_details(detail, title, llm_client)
        return action_success(
            facts={
                "requested_title": title,
                "media_type": media_type,
                "tmdb_id": int(tmdb_id),
                "details": detail,
            },
            summary_for_user=summary,
            say_hint="Summarize title details using the returned facts.",
        )

    # ---------- Platform handlers ----------
    async def handle_discord(self, message, args, llm_client):
        return await self._answer(args, llm_client)

    async def handle_webui(self, args, llm_client):
        async def inner():
            return await self._answer(args, llm_client)

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

    async def handle_meshtastic(self, args=None, llm_client=None, context=None, **kwargs):
        args = args or {}
        ctx = context if isinstance(context, dict) else {}
        origin = ctx.get("origin") if isinstance(ctx.get("origin"), dict) else {}
        sender = ""
        source_from = origin.get("from")
        if isinstance(source_from, dict):
            sender = str(source_from.get("node_id") or source_from.get("long_name") or source_from.get("short_name") or "").strip()
        channel = str(ctx.get("channel") or origin.get("channel") or origin.get("target") or origin.get("channel_id") or "").strip()
        user = str(ctx.get("user") or origin.get("user") or origin.get("user_id") or sender or "").strip()
        raw_text = str(
            ctx.get("raw_message")
            or ctx.get("raw")
            or ctx.get("request_text")
            or origin.get("text")
            or origin.get("message")
            or origin.get("body")
            or ""
        ).strip()
        call_kwargs = {"args": args, "llm_client": llm_client}
        try:
            sig = __import__("inspect").signature(self.handle_irc)
        except Exception:
            sig = None
        if sig is not None:
            if "bot" in sig.parameters:
                call_kwargs["bot"] = None
            if "channel" in sig.parameters:
                call_kwargs["channel"] = channel
            if "user" in sig.parameters:
                call_kwargs["user"] = user
            if "raw_message" in sig.parameters:
                call_kwargs["raw_message"] = raw_text
            if "raw" in sig.parameters:
                call_kwargs["raw"] = raw_text
            if "context" in sig.parameters:
                call_kwargs["context"] = ctx
        return await self.handle_irc(**call_kwargs)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._answer(args, llm_client)

    async def handle_homeassistant(self, args, llm_client):
        return await self._answer(args, llm_client)
    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        try:
            return await self.handle_homeassistant(args=args, llm_client=llm_client, context=context)
        except TypeError:
            try:
                return await self.handle_homeassistant(args=args, llm_client=llm_client)
            except TypeError:
                return await self.handle_homeassistant(args, llm_client)


    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        return await self._answer(args or {}, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self._answer(args or {}, llm_client)

    async def handle_homekit(self, args, llm_client):
        return await self._answer(args or {}, llm_client)


verba = OverseerrDetailsPlugin()
