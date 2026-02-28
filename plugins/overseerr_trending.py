import json
import logging
import re
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv

from helpers import redis_client, extract_json
from plugin_base import ToolPlugin
from plugin_result import action_failure, action_success

load_dotenv()
logger = logging.getLogger("overseerr_trending")
logger.setLevel(logging.INFO)


class OverseerrTrendingPlugin(ToolPlugin):
    name = "overseerr_trending"
    plugin_name = "Overseerr Trending"
    version = "1.2.0"
    min_tater_version = "59"
    pretty_name = "Overseerr: Trending & Upcoming"
    settings_category = "Overseerr"

    usage = (
        '{"function":"overseerr_trending","arguments":{"query":"ONE natural-language trending request '
        '(for example: what movies are trending, show upcoming tv shows, what is popular on overseerr right now)."}}'
    )

    description = "List trending or upcoming movies or TV shows from Overseerr from one natural-language request."
    plugin_dec = "List trending or upcoming movies/TV from Overseerr."
    waiting_prompt_template = (
        "Give {mention} a short, cheerful note that you’re fetching the latest lists from Overseerr now. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "telegram"]

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
    when_to_use = "Use when the user asks what movies or TV shows are trending, popular, or upcoming in Overseerr."
    how_to_use = (
        "Pass one natural-language request in query. Mention movie or TV if needed, and say trending or upcoming naturally."
    )
    common_needs = ["A natural-language trending or upcoming request."]
    missing_info_prompts = []
    example_calls = [
        '{"function":"overseerr_trending","arguments":{"query":"what movies are trending"}}',
        '{"function":"overseerr_trending","arguments":{"query":"show upcoming tv shows"}}',
        '{"function":"overseerr_trending","arguments":{"query":"what is popular on overseerr right now"}}',
        '{"function":"overseerr_trending","arguments":{"query":"what upcoming movies should I watch"}}',
    ]


    @staticmethod
    def _coerce_kind(kind: str) -> str:
        text = str(kind or "").strip().lower()
        if text.startswith("mov") or text.startswith("film"):
            return "movie"
        if "tv" in text or "show" in text or "series" in text:
            return "tv"
        return "movie"

    @staticmethod
    def _coerce_when(when: str) -> str:
        text = str(when or "").strip().lower()
        return "upcoming" if text == "upcoming" else "trending"

    @staticmethod
    def _query_from_args(args: Dict[str, Any]) -> str:
        data = args or {}
        for key in ("query", "request", "text", "message", "content", "prompt"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _infer_kind_from_query(self, query: str, explicit: str = "") -> str:
        normalized = self._coerce_kind(explicit)
        if explicit:
            return normalized
        text = re.sub(r"\s+", " ", str(query or "").strip().lower())
        if re.search(r"\b(tv|show|shows|series|episodes|anime)\b", text):
            return "tv"
        if re.search(r"\b(movie|movies|film|films|cinema)\b", text):
            return "movie"
        return "movie"

    def _infer_when_from_query(self, query: str, explicit: str = "") -> str:
        normalized = self._coerce_when(explicit)
        if explicit:
            return normalized
        text = re.sub(r"\s+", " ", str(query or "").strip().lower())
        if re.search(r"\b(upcoming|coming soon|coming out|release soon|releasing soon|next up)\b", text):
            return "upcoming"
        return "trending"

    async def _parse_request_with_llm(self, query: str, llm_client):
        text = re.sub(r"\s+", " ", str(query or "").strip())
        if not text or not llm_client:
            return "", ""
        prompt = (
            "Extract the trending-list intent from the user request.\n"
            "Return only JSON with this shape: {\"kind\":\"movie|tv\",\"when\":\"trending|upcoming\"}\n"
            "Rules:\n"
            "1) kind must be movie or tv.\n"
            "2) when must be trending or upcoming.\n"
            "3) If the user does not specify kind, choose the most likely one.\n"
            "4) If the user does not specify when, default to trending.\n\n"
            f'User request: "{text}"\n'
        )
        try:
            resp = await llm_client.chat(
                messages=[{"role": "system", "content": prompt}],
                max_tokens=80,
                temperature=0,
            )
            raw = ((resp or {}).get("message") or {}).get("content") or ""
        except Exception as exc:
            logger.warning("[overseerr_trending] request parse fallback: %s", exc)
            return "", ""
        try:
            parsed = extract_json(raw) or raw
            payload = json.loads(parsed)
            kind = self._coerce_kind(str(payload.get("kind") or ""))
            when = self._coerce_when(str(payload.get("when") or ""))
            return kind, when
        except Exception as exc:
            logger.warning("[overseerr_trending] request parse decode fallback: %s", exc)
            return "", ""

    async def _resolve_request(self, args: Dict[str, Any], llm_client):
        data = args or {}
        explicit_kind = str(data.get("kind") or "").strip()
        explicit_when = str(data.get("when") or "").strip()
        if explicit_kind or explicit_when:
            return self._infer_kind_from_query(explicit_kind, explicit_kind), self._infer_when_from_query(explicit_when, explicit_when)

        query = self._query_from_args(data)
        if not query:
            return "movie", "trending"

        parsed_kind, parsed_when = await self._parse_request_with_llm(query, llm_client)
        kind = parsed_kind or self._infer_kind_from_query(query)
        when = parsed_when or self._infer_when_from_query(query)
        return kind, when

    @staticmethod
    def _get_settings() -> Tuple[str, str]:
        settings = redis_client.hgetall("plugin_settings:Overseerr") or {}

        def _val(key: str, default: str = "") -> str:
            value = settings.get(key, default)
            if isinstance(value, (bytes, bytearray)):
                return value.decode("utf-8", "ignore")
            return str(value or "")

        base = (_val("OVERSEERR_BASE_URL", "http://localhost:5055") or "http://localhost:5055").rstrip("/")
        api = _val("OVERSEERR_API_KEY", "")
        return base, api

    @staticmethod
    def _is_tts_platform(platform: str) -> bool:
        return (platform or "").lower() in {"homeassistant", "homekit"}

    def _fetch_list(self, kind: str, when: str) -> Dict[str, Any]:
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        norm_kind = self._coerce_kind(kind)
        norm_when = self._coerce_when(when)

        if norm_when == "trending":
            url = f"{base}/api/v1/discover/trending"
        else:
            bucket = "movies" if norm_kind == "movie" else "tv"
            url = f"{base}/api/v1/discover/{bucket}/upcoming"

        headers = {"X-Api-Key": api_key, "Accept": "application/json"}
        try:
            resp = requests.get(url, params={"page": 1}, headers=headers, timeout=12)
            if resp.status_code != 200:
                logger.error("[Overseerr %s] HTTP %s :: %s", norm_when, resp.status_code, resp.text)
                return {"error": f"Overseerr returned HTTP {resp.status_code} for {norm_when}."}
            return resp.json() or {}
        except Exception as exc:
            logger.exception("[Overseerr list fetch error] %s", exc)
            return {"error": f"Failed to reach Overseerr: {exc}"}

    def _extract_titles(self, data: Dict[str, Any], kind: str) -> List[str]:
        results = (data.get("results") or data.get("items") or [])[:25]
        if not results:
            return []

        want = self._coerce_kind(kind)
        filtered = [r for r in results if (r.get("mediaType") or r.get("media_type") or "").lower() == want]
        use = filtered[:10] if filtered else results[:10]

        out: List[str] = []
        seen = set()
        for item in use:
            title = item.get("title") or item.get("name") or ""
            clean = re.sub(r"\s+", " ", str(title)).strip()
            if not clean or clean.lower() in seen:
                continue
            seen.add(clean.lower())
            out.append(clean)
            if len(out) >= 10:
                break
        return out

    @staticmethod
    def _tts_title_list(titles: List[str], max_titles: int = 8) -> str:
        clean = [re.sub(r"\s+", " ", (t or "").strip()) for t in titles if (t or "").strip()]
        clean = clean[:max_titles]
        if not clean:
            return ""
        if len(clean) == 1:
            return f"{clean[0]}."
        head = clean[:-1]
        last = clean[-1]
        parts = [f"{title}." for title in head]
        parts.append(f"and {last}.")
        return " ".join(parts)

    async def _ask_llm_intro_prefix(self, kind: str, when: str, titles: List[str], llm_client) -> str:
        kind_label = "movies" if self._coerce_kind(kind) == "movie" else "TV shows"
        when_norm = self._coerce_when(when)
        if when_norm == "trending":
            return f"Here are this week’s hottest {kind_label}:"
        return f"Here are some upcoming {kind_label}:"

    async def _answer(self, args: Dict[str, Any], llm_client, *, platform: str) -> str:
        args = args or {}
        kind, when = await self._resolve_request(args, llm_client)

        data = self._fetch_list(kind, when)
        if "error" in data:
            return action_failure(
                code="overseerr_list_failed",
                message=str(data["error"]),
                say_hint="Explain fetching Overseerr lists failed and suggest checking settings.",
            )

        titles = self._extract_titles(data, kind)
        if not titles:
            return action_failure(
                code="no_results",
                message=f"No {self._coerce_when(when)} results found.",
                say_hint="Explain there were no matching trending/upcoming results.",
            )

        intro = await self._ask_llm_intro_prefix(kind, when, titles, llm_client)
        if self._is_tts_platform(platform):
            intro_sentence = intro.rstrip(":").strip() + "."
            spoken_titles = self._tts_title_list(titles, max_titles=8)
            summary = f"{intro_sentence} {spoken_titles}".strip()
        else:
            summary = f"{intro} {'; '.join(titles[:10])}".strip()
        return action_success(
            facts={
                "kind": self._coerce_kind(kind),
                "when": self._coerce_when(when),
                "titles": titles,
                "count": len(titles),
            },
            summary_for_user=summary,
            say_hint="Provide the requested trending/upcoming title list from the returned facts.",
        )

    async def handle_discord(self, message, args, llm_client):
        try:
            return await self._answer(args or {}, llm_client, platform="discord")
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_discord] %s", exc)
            return action_failure(code="overseerr_trending_exception", message=f"Error: {exc}")

    async def handle_webui(self, args, llm_client):
        try:
            return await self._answer(args or {}, llm_client, platform="webui")
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_webui] %s", exc)
            return action_failure(code="overseerr_trending_exception", message=f"Error: {exc}")

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        try:
            return await self._answer(args or {}, llm_client, platform="irc")
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_irc] %s", exc)
            return action_failure(code="overseerr_trending_exception", message=f"Error: {exc}")

    async def handle_homeassistant(self, args, llm_client):
        try:
            return await self._answer(args or {}, llm_client, platform="homeassistant")
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_homeassistant] %s", exc)
            return action_failure(code="overseerr_trending_exception", message="There was an error fetching results.")

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        try:
            return await self._answer(args or {}, llm_client, platform="matrix")
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_matrix] %s", exc)
            return action_failure(code="overseerr_trending_exception", message=f"Error: {exc}")

    async def handle_telegram(self, update, args, llm_client):
        try:
            return await self._answer(args or {}, llm_client, platform="telegram")
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_telegram] %s", exc)
            return action_failure(code="overseerr_trending_exception", message=f"Error: {exc}")

    async def handle_homekit(self, args, llm_client):
        try:
            return await self._answer(args or {}, llm_client, platform="homekit")
        except Exception as exc:
            logger.exception("[OverseerrTrending handle_homekit] %s", exc)
            return action_failure(code="overseerr_trending_exception", message="There was an error fetching results.")


plugin = OverseerrTrendingPlugin()
