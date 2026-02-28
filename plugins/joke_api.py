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
    version = "1.1.0"
    min_tater_version = "59"
    pretty_name = "Joke API"
    usage = '{"function":"joke_api","arguments":{}}'
    description = "Fetch one joke from JokeAPI."
    plugin_dec = "Fetch one joke from JokeAPI."
    when_to_use = "Use when the user asks for a joke."
    how_to_use = (
        "Call the tool with no arguments to fetch one joke. "
        "If the user wants a themed or rewritten joke, fetch the joke first and then transform it outside the plugin."
    )
    common_needs = []
    missing_info_prompts = []
    example_calls = [
        '{"function":"joke_api","arguments":{}}',
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
        safe_mode = self._to_bool(s.get("JOKEAPI_DEFAULT_SAFE_MODE"), default=True)
        default_lang = (self._decode_text(s.get("JOKEAPI_DEFAULT_LANGUAGE")) or "en").strip().lower()
        if len(default_lang) > 2:
            default_lang = default_lang[:2]
        if not re.fullmatch(r"[a-z]{2}", default_lang or ""):
            default_lang = "en"
        return {
            "timeout_seconds": timeout,
            "default_safe_mode": safe_mode,
            "default_lang": default_lang,
        }

    def _blacklist_flags(self, safe_mode: bool) -> List[str]:
        if safe_mode:
            return list(self._STRICT_SAFE_FLAGS)
        return ["racist", "sexist", "political", "religious"]

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
        settings = self._settings()
        lang = settings["default_lang"]
        safe_mode = settings["default_safe_mode"]
        blacklist = self._blacklist_flags(safe_mode)

        url = "https://v2.jokeapi.dev/joke/Any"
        params: Dict[str, Any] = {"lang": lang}
        if blacklist:
            params["blacklistFlags"] = ",".join(blacklist)

        try:
            response = requests.get(url, params=params, timeout=settings["timeout_seconds"])
            if response.status_code >= 400:
                return action_failure(
                    code="joke_api_http_error",
                    message=f"JokeAPI request failed with HTTP {response.status_code}.",
                    say_hint="Explain JokeAPI failed and suggest retrying later.",
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
                say_hint="Explain JokeAPI could not return a joke right now and suggest retrying later.",
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
                "lang": lang,
                "safe_mode": safe_mode,
            },
            data={"jokes": jokes},
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
