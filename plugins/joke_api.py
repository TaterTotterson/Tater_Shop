import logging
import re
from typing import Any, Dict, List

import requests

from plugin_base import ToolPlugin
from helpers import redis_client
from plugin_result import action_failure, action_success

logger = logging.getLogger("joke_api")
logger.setLevel(logging.INFO)


class JokeAPIPlugin(ToolPlugin):
    name = "joke_api"
    plugin_name = "Joke API"
    version = "1.0.0"
    min_tater_version = "59"
    pretty_name = "Joke API"
    usage = (
        '{"function":"joke_api","arguments":{"query":"ONE consolidated joke request in natural language. '
        'Include theme/count/language naturally (for example: tell me two programming jokes in spanish)."}}'
    )
    description = (
        "Fetch jokes from JokeAPI from one natural-language request, including themed jokes and multi-joke asks."
    )
    plugin_dec = "Fetch jokes from JokeAPI."
    when_to_use = "Use when the user asks for a joke, themed joke, or multiple jokes."
    how_to_use = (
        "Pass one consolidated natural-language joke request. Include theme/count/language naturally "
        "(for example: tell me 2 programming jokes in english)."
    )
    common_needs = ["A natural-language joke request."]
    missing_info_prompts = []
    example_calls = [
        '{"function":"joke_api","arguments":{"query":"tell me a joke"}}',
        '{"function":"joke_api","arguments":{"query":"tell me two programming jokes"}}',
        '{"function":"joke_api","arguments":{"query":"give me a spooky joke about ghosts"}}',
        '{"function":"joke_api","arguments":{"query":"tell me a clean dad joke"}}',
    ]
    settings_category = "Joke API"
    platforms = ["webui", "homeassistant", "homekit", "discord", "telegram", "matrix", "irc", "xbmc"]
    routing_keywords = [
        "joke",
        "jokes",
        "funny",
        "laugh",
        "dad joke",
        "pun",
        "programming joke",
        "make me laugh",
        "tell me a joke",
    ]
    waiting_prompt_template = (
        "Let {mention} know you're grabbing a joke now. Keep it short and friendly. Only output that message."
    )

    required_settings = {
        "JOKEAPI_TIMEOUT_SECONDS": {
            "label": "HTTP Timeout Seconds",
            "type": "number",
            "default": 10,
            "description": "Timeout for JokeAPI HTTP calls.",
        },
        "JOKEAPI_MAX_AMOUNT": {
            "label": "Maximum Jokes Per Request",
            "type": "number",
            "default": 3,
            "description": "Upper bound for amount requested from natural-language asks.",
        },
        "JOKEAPI_DEFAULT_SAFE_MODE": {
            "label": "Default Safe Mode",
            "type": "checkbox",
            "default": True,
            "description": "If enabled, apply JokeAPI safe-mode and strict blacklist by default.",
        },
        "JOKEAPI_DEFAULT_LANGUAGE": {
            "label": "Default Language",
            "type": "string",
            "default": "en",
            "description": "JokeAPI language code (for example: en, es, de, fr, pt, cs).",
        },
    }

    _CATEGORY_HINTS = {
        "Programming": ("programming", "coding", "coder", "developer", "dev", "python", "javascript", "java", "software"),
        "Pun": ("pun", "puns", "dad joke", "dad jokes"),
        "Spooky": ("spooky", "halloween", "scary", "ghost", "creepy"),
        "Christmas": ("christmas", "xmas", "holiday"),
        "Dark": ("dark", "edgy", "morbid", "black humor", "black humour"),
    }
    _LANGUAGE_HINTS = {
        "english": "en",
        "spanish": "es",
        "espanol": "es",
        "español": "es",
        "german": "de",
        "french": "fr",
        "portuguese": "pt",
        "czech": "cs",
    }
    _STRICT_SAFE_FLAGS = ["nsfw", "religious", "political", "racist", "sexist", "explicit"]

    @staticmethod
    def _decode_text(value: Any) -> str:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", "ignore")
        if value is None:
            return ""
        return str(value)

    @classmethod
    def _to_int(cls, value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(float(cls._decode_text(value).strip()))
        except Exception:
            parsed = int(default)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    @classmethod
    def _to_bool(cls, value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        raw = cls._decode_text(value).strip().lower()
        if raw in {"1", "true", "yes", "on", "enabled"}:
            return True
        if raw in {"0", "false", "no", "off", "disabled"}:
            return False
        return default

    def _settings(self) -> Dict[str, Any]:
        s = (
            redis_client.hgetall(f"plugin_settings:{self.settings_category}")
            or redis_client.hgetall(f"plugin_settings: {self.settings_category}")
            or {}
        )
        timeout = self._to_int(s.get("JOKEAPI_TIMEOUT_SECONDS"), default=10, minimum=3, maximum=30)
        max_amount = self._to_int(s.get("JOKEAPI_MAX_AMOUNT"), default=3, minimum=1, maximum=10)
        safe_mode = self._to_bool(s.get("JOKEAPI_DEFAULT_SAFE_MODE"), default=True)
        default_lang = (self._decode_text(s.get("JOKEAPI_DEFAULT_LANGUAGE")) or "en").strip().lower()
        if len(default_lang) > 2:
            default_lang = default_lang[:2]
        if not re.fullmatch(r"[a-z]{2}", default_lang or ""):
            default_lang = "en"
        return {
            "timeout_seconds": timeout,
            "max_amount": max_amount,
            "default_safe_mode": safe_mode,
            "default_lang": default_lang,
        }

    @staticmethod
    def _query_from_args(args: Dict[str, Any]) -> str:
        args = args or {}
        for key in ("query", "request", "text", "content", "message"):
            value = args.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in args.values():
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def _infer_categories(self, query: str) -> List[str]:
        q = " ".join(str(query or "").strip().lower().split())
        if not q:
            return ["Any"]
        out: List[str] = []
        for category, hints in self._CATEGORY_HINTS.items():
            if any(h in q for h in hints):
                out.append(category)
        if not out:
            return ["Any"]
        # Keep deterministic order.
        ordered = ["Programming", "Pun", "Spooky", "Christmas", "Dark"]
        return [c for c in ordered if c in set(out)]

    @staticmethod
    def _clean_contains(value: str) -> str:
        text = " ".join(str(value or "").strip().split())
        if not text:
            return ""
        text = re.sub(r"^[\"'`]+|[\"'`]+$", "", text).strip()
        text = re.sub(r"(?i)\b(jokes?|theme|please|thanks?)\b", "", text).strip()
        text = re.sub(r"\s{2,}", " ", text).strip(" .,!?:;")
        if not text:
            return ""
        return text[:64]

    def _infer_contains(self, query: str, args: Dict[str, Any]) -> str:
        raw = (
            args.get("contains")
            or args.get("theme")
            or args.get("topic")
            or ""
        )
        explicit = self._clean_contains(str(raw))
        if explicit:
            return explicit

        q = " ".join(str(query or "").strip().split())
        if not q:
            return ""

        patterns = (
            r"(?i)\b(?:about|on)\s+(.+?)(?:\s+in\s+[a-z]+)?$",
            r"(?i)\b(?:with|using)\s+(?:a\s+)?(?:theme|topic)\s+(.+?)(?:\s+in\s+[a-z]+)?$",
            r"(?i)\b(?:theme|topic)\s*[:\-]\s*(.+)$",
        )
        for pat in patterns:
            m = re.search(pat, q)
            if not m:
                continue
            candidate = self._clean_contains(m.group(1))
            if candidate:
                return candidate
        return ""

    def _infer_amount(self, query: str, args: Dict[str, Any], max_amount: int) -> int:
        arg_amount = args.get("amount")
        if arg_amount is not None:
            return self._to_int(arg_amount, default=1, minimum=1, maximum=max_amount)

        q = " ".join(str(query or "").strip().lower().split())
        if not q:
            return 1
        match = re.search(r"\b(\d{1,2})\s+(?:jokes?|ones?)\b", q)
        if match:
            return self._to_int(match.group(1), default=1, minimum=1, maximum=max_amount)
        if any(token in q for token in ("a few jokes", "few jokes", "some jokes", "multiple jokes", "couple jokes", "couple of jokes")):
            return min(3, max_amount)
        return 1

    def _infer_joke_type(self, query: str, args: Dict[str, Any]) -> str:
        raw_type = str(args.get("type") or "").strip().lower()
        if raw_type in {"single", "twopart"}:
            return raw_type
        q = " ".join(str(query or "").strip().lower().split())
        if any(t in q for t in ("one-liner", "oneliner", "single line", "short joke")):
            return "single"
        if any(t in q for t in ("two part", "two-part", "setup and punchline", "setup punchline")):
            return "twopart"
        return ""

    def _infer_lang(self, query: str, args: Dict[str, Any], default_lang: str) -> str:
        raw = str(args.get("lang") or "").strip().lower()
        if re.fullmatch(r"[a-z]{2}", raw):
            return raw
        q = " ".join(str(query or "").strip().lower().split())
        for hint, code in self._LANGUAGE_HINTS.items():
            if re.search(rf"\bin\s+{re.escape(hint)}\b", q):
                return code
        return default_lang

    def _infer_safe_mode(self, query: str, args: Dict[str, Any], default_safe_mode: bool) -> bool:
        if "safe_mode" in (args or {}):
            return self._to_bool(args.get("safe_mode"), default_safe_mode)
        q = " ".join(str(query or "").strip().lower().split())
        if any(t in q for t in ("clean joke", "safe joke", "family friendly", "kid friendly", "for kids")):
            return True
        if any(t in q for t in ("nsfw", "adult joke", "dirty joke", "offensive joke", "uncensored", "unfiltered", "18+")):
            return False
        return default_safe_mode

    def _blacklist_flags(self, query: str, safe_mode: bool) -> List[str]:
        if safe_mode:
            return list(self._STRICT_SAFE_FLAGS)

        q = " ".join(str(query or "").strip().lower().split())
        flags = ["racist", "sexist"]
        if not any(t in q for t in ("nsfw", "adult", "dirty", "explicit", "18+")):
            flags.extend(["nsfw", "explicit"])
        if "political" not in q and "politics" not in q:
            flags.append("political")
        if "religious" not in q and "religion" not in q:
            flags.append("religious")
        return flags

    @staticmethod
    def _extract_jokes(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        source_items: List[Dict[str, Any]] = []
        if isinstance(payload.get("jokes"), list):
            source_items = [x for x in payload.get("jokes") if isinstance(x, dict)]
        elif isinstance(payload, dict):
            source_items = [payload]

        jokes: List[Dict[str, Any]] = []
        for item in source_items:
            if bool(item.get("error")):
                continue
            jtype = str(item.get("type") or "").strip().lower()
            if jtype == "single":
                text = str(item.get("joke") or "").strip()
                if text:
                    jokes.append(
                        {
                            "type": "single",
                            "text": text,
                            "category": str(item.get("category") or "").strip(),
                            "safe": bool(item.get("safe")),
                            "id": item.get("id"),
                            "lang": str(item.get("lang") or "").strip(),
                        }
                    )
                continue

            if jtype == "twopart":
                setup = str(item.get("setup") or "").strip()
                delivery = str(item.get("delivery") or "").strip()
                if setup or delivery:
                    text = f"{setup}\n{delivery}".strip()
                    jokes.append(
                        {
                            "type": "twopart",
                            "text": text,
                            "setup": setup,
                            "delivery": delivery,
                            "category": str(item.get("category") or "").strip(),
                            "safe": bool(item.get("safe")),
                            "id": item.get("id"),
                            "lang": str(item.get("lang") or "").strip(),
                        }
                    )

        return jokes

    @staticmethod
    def _render_joke_text(jokes: List[Dict[str, Any]]) -> str:
        if not jokes:
            return ""
        if len(jokes) == 1:
            return str(jokes[0].get("text") or "").strip()
        lines: List[str] = []
        for idx, item in enumerate(jokes, start=1):
            text = str(item.get("text") or "").strip()
            if text:
                lines.append(f"{idx}. {text}")
        return "\n\n".join(lines).strip()

    async def _handle(self, args: Dict[str, Any], _llm_client: Any) -> Dict[str, Any]:
        args = args or {}
        settings = self._settings()
        query = self._query_from_args(args) or "tell me a joke"

        categories = self._infer_categories(query)
        contains = self._infer_contains(query, args)
        amount = self._infer_amount(query, args, settings["max_amount"])
        joke_type = self._infer_joke_type(query, args)
        lang = self._infer_lang(query, args, settings["default_lang"])
        safe_mode = self._infer_safe_mode(query, args, settings["default_safe_mode"])
        blacklist = self._blacklist_flags(query, safe_mode)

        categories_path = ",".join(categories) if categories else "Any"
        url = f"https://v2.jokeapi.dev/joke/{categories_path}"
        params: Dict[str, Any] = {"lang": lang}
        if amount > 1:
            params["amount"] = amount
        if joke_type:
            params["type"] = joke_type
        if contains:
            params["contains"] = contains
        if blacklist:
            params["blacklistFlags"] = ",".join(blacklist)
        if safe_mode:
            params["safe-mode"] = ""

        try:
            response = requests.get(url, params=params, timeout=settings["timeout_seconds"])
            if response.status_code >= 400:
                return action_failure(
                    code="joke_api_http_error",
                    message=f"JokeAPI request failed with HTTP {response.status_code}.",
                    say_hint="Explain JokeAPI failed and suggest retrying.",
                )
            payload = response.json() if "application/json" in (response.headers.get("content-type") or "").lower() else {}
        except Exception as exc:
            logger.error("[joke_api] request failed: %s", exc)
            return action_failure(
                code="joke_api_unreachable",
                message="I could not reach JokeAPI right now.",
                say_hint="Explain joke service is temporarily unavailable and suggest retrying.",
            )

        if not isinstance(payload, dict):
            return action_failure(
                code="joke_api_bad_payload",
                message="JokeAPI returned an unexpected response.",
                say_hint="Explain joke service returned invalid data and suggest retrying.",
            )

        if bool(payload.get("error")):
            message = str(payload.get("message") or "JokeAPI returned an error.").strip()
            details = payload.get("additionalInfo")
            if isinstance(details, str) and details.strip():
                message = f"{message} {details.strip()}"
            return action_failure(
                code="joke_api_error",
                message=message,
                say_hint="Explain no joke could be returned with the current filters and suggest a different theme.",
            )

        jokes = self._extract_jokes(payload)
        if not jokes:
            return action_failure(
                code="no_jokes_found",
                message="No jokes matched that request.",
                needs=["Try a different theme or ask for a general joke."],
                say_hint="Ask for a different joke theme or fewer restrictions.",
            )

        rendered = self._render_joke_text(jokes)
        return action_success(
            facts={
                "count": len(jokes),
                "categories": categories,
                "contains": contains or "",
                "lang": lang,
                "safe_mode": safe_mode,
                "type": joke_type or "any",
            },
            data={"jokes": jokes, "query": query},
            summary_for_user=rendered,
            say_hint="Return the fetched joke text directly and do not invent additional jokes.",
        )

    async def handle_webui(self, args: Dict[str, Any], llm_client: Any):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args: Dict[str, Any], llm_client: Any):
        return await self._handle(args, llm_client)

    async def handle_homekit(self, args: Dict[str, Any], llm_client: Any):
        return await self._handle(args, llm_client)

    async def handle_discord(self, message: Any = None, args: Dict[str, Any] = None, llm_client: Any = None):
        return await self._handle(args, llm_client)

    async def handle_telegram(self, update: Any = None, args: Dict[str, Any] = None, llm_client: Any = None):
        return await self._handle(args, llm_client)

    async def handle_matrix(
        self,
        client: Any = None,
        room: Any = None,
        sender: Any = None,
        body: Any = None,
        args: Dict[str, Any] = None,
        llm_client: Any = None,
    ):
        return await self._handle(args, llm_client)

    async def handle_irc(
        self,
        bot: Any = None,
        channel: Any = None,
        user: Any = None,
        raw_message: Any = None,
        args: Dict[str, Any] = None,
        llm_client: Any = None,
    ):
        return await self._handle(args, llm_client)

    async def handle_xbmc(self, args: Dict[str, Any], llm_client: Any):
        return await self._handle(args, llm_client)


plugin = JokeAPIPlugin()
