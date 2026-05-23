# verba/microwakeword.py
import asyncio
import difflib
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from helpers import redis_client
from verba_base import ToolVerba
from verba_result import action_failure, action_success
from voice_core_entities import VoiceCoreEntityClient

logger = logging.getLogger("microwakeword")
logger.setLevel(logging.INFO)

_VOICE_CORE = VoiceCoreEntityClient()


class MicroWakeWordPlugin(ToolVerba):
    """Change a Tater Voice Core satellite's local microWakeWord model at runtime."""

    name = "microwakeword"
    verba_name = "microWakeWord"
    version = "1.0.3"
    min_tater_version = "59"
    pretty_name = "microWakeWord Changer"
    settings_category = "microWakeWord"
    platforms = [
        "voice_core",
        "homekit",
        "xbmc",
        "webui",
        "macos",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    tags = ["voice_core", "microwakeword", "wake-word"]

    usage = (
        '{"function":"microwakeword","arguments":{"query":"change your wake word to hey computer"}}'
    )
    description = (
        "Change or suggest the local microWakeWord wake word on a Tater Voice Core satellite using natural language."
    )
    verba_dec = "Change or suggest the local microWakeWord wake word on a Tater Voice Core satellite."
    when_to_use = (
        "Use when the user wants to change Tater's local microWakeWord, asks for wake-word options, "
        "or asks to reset the microWakeWord model to the compiled default."
    )
    how_to_use = (
        "Pass one natural-language request in query. Optional explicit fields: action and wake_word. "
        "Examples: change your wake word to hey computer; suggest a few wake words; reset your wake word."
    )
    common_needs = ["A requested wake word, or a request for suggestions."]
    missing_info_prompts = ["Which wake word should I use?"]
    example_calls = [
        '{"function":"microwakeword","arguments":{"query":"change your wake word to hey computer"}}',
        '{"function":"microwakeword","arguments":{"query":"change your name to hey sam"}}',
        '{"function":"microwakeword","arguments":{"query":"suggest a few wake words"}}',
        '{"function":"microwakeword","arguments":{"query":"reset your wake word to default"}}',
    ]
    argument_schema = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Natural-language request, for example: change your wake word to hey computer.",
            },
            "wake_word": {
                "type": "string",
                "description": "Optional explicit requested wake word phrase, for example: hey computer.",
            },
            "action": {
                "type": "string",
                "description": "Optional action: set, suggest, reset, or status.",
            },
        },
        "required": [],
    }

    required_settings = {
        "WAKE_WORD_MANIFEST_URL": {
            "label": "Wake Word Manifest URL",
            "type": "string",
            "default": "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wake_word_manifest.json",
            "description": "Manifest catalog URL or local file path for available microWakeWord models.",
        },
        "MICROWAKEWORD_VOICE_CORE_SELECTOR": {
            "label": "Voice Core Satellite Selector (optional)",
            "type": "string",
            "default": "",
            "description": "Optional host: selector for web UI or chat requests that are not sent from a satellite.",
        },
        "MAX_SUGGESTIONS": {
            "label": "Max Suggestions",
            "type": "number",
            "default": 5,
            "description": "How many similar wake words to offer when no exact match exists.",
        },
    }

    waiting_prompt_template = (
        "Write a short friendly message telling {mention} you are checking the wake-word list now. "
        "Only output that message."
    )

    DEFAULT_MANIFEST_URL = (
        "https://raw.githubusercontent.com/TaterTotterson/microWakeWords/main/wake_word_manifest.json"
    )
    LOCAL_MANIFEST_CANDIDATES = [
        Path("/Users/ahphooey/Scripts/microWakeWords/wake_word_manifest.json"),
        Path(__file__).resolve().parents[2] / "microWakeWords" / "wake_word_manifest.json",
        Path(__file__).resolve().parents[1] / "wake_word_manifest.json",
    ]
    DEFAULT_SUGGESTION_SLUGS = [
        "hey_tater",
        "hey_computer",
        "computer",
        "jarvis",
        "cortana",
        "friday",
        "hey_atlas",
        "hey_athena",
    ]
    RESET_VALUES = {"", "compiled", "default"}

    _manifest_cache: Dict[str, Any] = {"loaded_at": 0.0, "source": "", "entries": []}

    # -------------------------------------------------------------
    # Small utilities
    # -------------------------------------------------------------

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def _decode_redis_map(raw: Optional[dict]) -> dict:
        out: dict = {}
        for key, value in (raw or {}).items():
            k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
            if isinstance(value, (bytes, bytearray)):
                out[k] = value.decode("utf-8", "ignore")
            elif value is None:
                out[k] = ""
            else:
                out[k] = str(value)
        return out

    def _get_settings(self) -> dict:
        merged: dict = {}
        for key in (f"verba_settings:{self.settings_category}", f"verba_settings: {self.settings_category}"):
            merged.update(self._decode_redis_map(redis_client.hgetall(key) or {}))
        return merged

    def _get_int_setting(self, key: str, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(float(self._get_settings().get(key) or default))
        except Exception:
            value = int(default)
        return max(minimum, min(maximum, value))

    @staticmethod
    def _normalize_words(value: Any) -> str:
        text = str(value or "").lower()
        text = text.replace("&", " and ")
        text = re.sub(r"[_\-]+", " ", text)
        text = re.sub(r"[^a-z0-9\s]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @classmethod
    def _compact_words(cls, value: Any) -> str:
        return cls._normalize_words(value).replace(" ", "")

    @classmethod
    def _slugify(cls, value: Any) -> str:
        return cls._normalize_words(value).replace(" ", "_")

    @classmethod
    def _speech_variant_keys(cls, value: Any) -> set[str]:
        text = cls._normalize_words(value)
        if not text:
            return set()

        keys = {text, text.replace(" ", "")}
        tokens = text.split()
        for index, token in enumerate(tokens):
            variants = {token}
            if len(token) > 3 and token.endswith("er"):
                variants.add(token[:-2] + "y")
            if len(token) > 3 and token.endswith("y"):
                variants.add(token[:-1] + "er")
            for variant in variants:
                if variant == token:
                    continue
                changed = list(tokens)
                changed[index] = variant
                phrase = " ".join(changed)
                keys.add(phrase)
                keys.add(phrase.replace(" ", ""))

        return keys

    @classmethod
    def _wake_core_words(cls, value: Any) -> str:
        text = cls._normalize_words(value)
        for prefix in ("hey ", "ok ", "okay "):
            if text.startswith(prefix):
                return text[len(prefix) :].strip()
        return text

    @staticmethod
    def _common_prefix_len(left: str, right: str) -> int:
        count = 0
        for a, b in zip(left, right):
            if a != b:
                break
            count += 1
        return count

    @staticmethod
    def _strip_polite_tail(value: str) -> str:
        text = str(value or "").strip()
        text = re.sub(r"\b(?:please|pls|thanks|thank you)\b", "", text, flags=re.I)
        text = re.sub(
            r"\b(?:from now on|for now|right now|instead|if you can|if possible)\b.*$",
            "",
            text,
            flags=re.I,
        )
        return re.sub(r"\s+", " ", text).strip(" .!?\"'")

    def _normalize_args(self, args: Any) -> Dict[str, Any]:
        if isinstance(args, dict):
            payload = dict(args)
            nested = payload.get("arguments")
            if isinstance(nested, dict):
                merged = dict(nested)
                for key, value in payload.items():
                    if key != "arguments":
                        merged[key] = value
                return merged
            return payload

        if isinstance(args, str):
            text = self._text(args)
            if not text:
                return {}
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    return self._normalize_args(parsed)
            except Exception:
                pass
            return {"query": text}

        return {}

    # -------------------------------------------------------------
    # Manifest loading and matching
    # -------------------------------------------------------------

    def _manifest_url(self) -> str:
        return self._text(self._get_settings().get("WAKE_WORD_MANIFEST_URL")) or self.DEFAULT_MANIFEST_URL

    def _load_manifest_payload(self) -> Tuple[dict, str]:
        configured = self._manifest_url()
        path_value = configured
        if path_value.startswith("file://"):
            path_value = path_value[7:]

        explicit_path = Path(path_value).expanduser() if path_value and not path_value.startswith("http") else None
        if explicit_path and explicit_path.exists():
            return json.loads(explicit_path.read_text(encoding="utf-8")), str(explicit_path)

        if configured == self.DEFAULT_MANIFEST_URL:
            for candidate in self.LOCAL_MANIFEST_CANDIDATES:
                if candidate.exists():
                    return json.loads(candidate.read_text(encoding="utf-8")), str(candidate)

        resp = requests.get(configured, timeout=15)
        if resp.status_code >= 400:
            raise RuntimeError(f"manifest fetch failed: HTTP {resp.status_code}: {resp.text[:200]}")
        return resp.json(), configured

    def _load_entries(self) -> List[dict]:
        source = self._manifest_url()
        now = time.time()
        cached_source = self._manifest_cache.get("source")
        cached_entries = self._manifest_cache.get("entries")
        if cached_source == source and isinstance(cached_entries, list) and cached_entries and (
            now - float(self._manifest_cache.get("loaded_at") or 0.0)
        ) < 3600:
            return list(cached_entries)

        payload, loaded_from = self._load_manifest_payload()
        raw_entries = payload.get("entries") if isinstance(payload, dict) else []
        entries: List[dict] = []
        seen_slugs: set[str] = set()
        for item in raw_entries if isinstance(raw_entries, list) else []:
            if not isinstance(item, dict):
                continue
            slug = self._slugify(item.get("slug") or item.get("name") or item.get("label"))
            url = self._text(item.get("download_url") or item.get("url"))
            if not slug or not url or slug in seen_slugs:
                continue
            label = self._text(item.get("label")) or slug.replace("_", " ").title()
            row = dict(item)
            row["slug"] = slug
            row["label"] = label
            row["url"] = url
            row["download_url"] = url
            row["_norm_forms"] = self._entry_forms(row)
            entries.append(row)
            seen_slugs.add(slug)

        if not entries:
            raise RuntimeError(f"wake-word manifest contained no usable entries: {loaded_from}")

        self._manifest_cache = {"loaded_at": now, "source": source, "loaded_from": loaded_from, "entries": entries}
        return entries

    def _entry_forms(self, entry: dict) -> List[str]:
        forms = {
            self._normalize_words(entry.get("label")),
            self._normalize_words(entry.get("name")),
            self._normalize_words(entry.get("slug")),
            self._normalize_words(self._text(entry.get("slug")).replace("_", " ")),
        }
        out = [x for x in forms if x]
        for form in list(out):
            if form.startswith("hey "):
                out.append(form[4:])
            elif form.startswith("ok "):
                out.append(form[3:])
            elif form.startswith("okay "):
                out.append(form[5:])
        return list(dict.fromkeys([x for x in out if x]))

    def _format_option(self, entry: dict) -> str:
        return self._text(entry.get("label")) or self._text(entry.get("slug")).replace("_", " ").title()

    def _suggestions_for(self, target: str, entries: List[dict], limit: int) -> List[dict]:
        norm_target = self._normalize_words(target)
        if not norm_target:
            preferred: List[dict] = []
            by_slug = {self._text(e.get("slug")): e for e in entries}
            for slug in self.DEFAULT_SUGGESTION_SLUGS:
                if slug in by_slug and by_slug[slug] not in preferred:
                    preferred.append(by_slug[slug])
                if len(preferred) >= limit:
                    return preferred
            return (preferred + entries)[:limit]

        scored: List[Tuple[float, str, dict]] = []
        target_slug = self._slugify(norm_target)
        target_core = self._wake_core_words(norm_target)
        target_compact = self._compact_words(norm_target)
        target_keys = self._speech_variant_keys(norm_target)
        target_core_keys = self._speech_variant_keys(target_core)
        for entry in entries:
            forms = entry.get("_norm_forms") if isinstance(entry.get("_norm_forms"), list) else self._entry_forms(entry)
            best = 0.0
            for form in forms:
                score = difflib.SequenceMatcher(None, norm_target, form).ratio()
                form_compact = self._compact_words(form)
                if target_compact and form_compact:
                    score = max(score, difflib.SequenceMatcher(None, target_compact, form_compact).ratio())
                    if target_compact == form_compact:
                        score += 1.0
                if norm_target in form or form in norm_target:
                    score += 0.18
                if self._slugify(form) == target_slug:
                    score += 1.0
                if target_keys & self._speech_variant_keys(form):
                    score = max(score, 1.16)
                target_parts = set(norm_target.split())
                form_parts = set(form.split())
                if target_parts and form_parts:
                    score += 0.04 * len(target_parts & form_parts)
                form_core = self._wake_core_words(form)
                if target_core and form_core and target_core != norm_target:
                    core_ratio = difflib.SequenceMatcher(None, target_core, form_core).ratio()
                    score = max(score, 0.55 + (0.55 * core_ratio))
                    compact_core_ratio = difflib.SequenceMatcher(
                        None,
                        self._compact_words(target_core),
                        self._compact_words(form_core),
                    ).ratio()
                    score = max(score, 0.55 + (0.55 * compact_core_ratio))
                    if target_core_keys & self._speech_variant_keys(form_core):
                        score = max(score, 1.28)
                    prefix_len = self._common_prefix_len(target_core, form_core)
                    if prefix_len:
                        score += min(0.24, 0.08 * prefix_len)
                    if target_core in form_core or form_core in target_core:
                        score += 0.14
                best = max(best, score)
            scored.append((best, self._format_option(entry), entry))

        scored.sort(key=lambda row: (row[0], row[1]), reverse=True)
        suggestions: List[dict] = []
        seen: set[str] = set()
        for _, _, entry in scored:
            slug = self._text(entry.get("slug"))
            if not slug or slug in seen:
                continue
            suggestions.append(entry)
            seen.add(slug)
            if len(suggestions) >= limit:
                break
        return suggestions

    def _exact_match(self, target: str, entries: List[dict]) -> Optional[dict]:
        norm_target = self._normalize_words(target)
        slug_target = self._slugify(target)
        compact_target = self._compact_words(target)
        if not norm_target and slug_target not in self.RESET_VALUES:
            return None

        for entry in entries:
            if self._text(entry.get("download_url") or entry.get("url")) == self._text(target):
                return entry
            if self._text(entry.get("slug")) == slug_target:
                return entry
            if compact_target and self._compact_words(entry.get("slug")) == compact_target:
                return entry
            forms = entry.get("_norm_forms") if isinstance(entry.get("_norm_forms"), list) else self._entry_forms(entry)
            if norm_target in forms:
                return entry
            if compact_target and any(self._compact_words(form) == compact_target for form in forms):
                return entry
        return None

    # -------------------------------------------------------------
    # Natural language interpretation
    # -------------------------------------------------------------

    def _merged_request_text(self, args: Dict[str, Any]) -> str:
        fields = (
            args.get("query"),
            args.get("request"),
            args.get("message"),
            args.get("prompt"),
            args.get("text"),
            args.get("content"),
        )
        return re.sub(r"\s+", " ", " ".join(self._text(v) for v in fields if self._text(v))).strip()

    def _infer_action(self, args: Dict[str, Any], request_text: str, target: str) -> str:
        explicit = self._normalize_words(args.get("action"))
        if explicit in {"set", "change", "switch", "update"}:
            return "set"
        if explicit in {"suggest", "suggestions", "list", "options", "help"}:
            return "suggest"
        if explicit in {"reset", "default", "compiled", "restore"}:
            return "reset"
        if explicit in {"status", "state", "current", "read"}:
            return "status"

        text = self._normalize_words(request_text)
        target_norm = self._normalize_words(target)
        if re.search(r"\b(reset|restore|default|compiled)\b", text):
            return "reset"
        if re.search(r"\b(what|which|current|status)\b.*\b(wake word|wakeword|model url|microwakeword)\b", text):
            return "status"
        if re.search(r"\b(suggest|suggestion|options|choices|list|pick a few|show me)\b", text):
            return "suggest"
        if not target_norm:
            return "suggest"
        return "set"

    def _extract_target(self, args: Dict[str, Any], request_text: str) -> str:
        for key in ("wake_word", "target_wake_word", "new_wake_word", "name", "target"):
            value = self._text(args.get(key))
            if value:
                return self._strip_polite_tail(value)

        text = self._text(request_text)
        if not text:
            return ""

        patterns = [
            r"\b(?:change|set|switch|update|make)\b.+?\b(?:wake\s*word|wakeword|name)\b.+?\b(?:to|as|into|be|become)\s+(.+)$",
            r"\b(?:change|set|switch|update|make)\b.+?\b(?:to|as|into)\s+(.+)$",
            r"\b(?:call\s+yourself|call\s+you|address\s+you)\s+(?:as\s+)?(.+)$",
            r"\b(?:your\s+name\s+is|wake\s*word\s+is|wakeword\s+is)\s+(.+)$",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.I)
            if not match:
                continue
            target = self._strip_polite_tail(match.group(1))
            if self._normalize_words(target) not in {
                "",
                "your wake word",
                "the wake word",
                "wake word",
                "wakeword",
                "your name",
                "name",
            }:
                return target

        return ""

    # -------------------------------------------------------------
    # Entity resolution: Voice Core
    # -------------------------------------------------------------

    def _voice_core_selector(self, context: dict | None) -> str:
        selector = VoiceCoreEntityClient.selector_from_context(context)
        if selector:
            return selector
        return self._text(self._get_settings().get("MICROWAKEWORD_VOICE_CORE_SELECTOR"))

    def _voice_core_entities_sync(self, selector: str) -> dict:
        return _VOICE_CORE.get_entities(selector)

    def _voice_core_set_text_sync(self, selector: str, entry: dict, value: str) -> dict:
        return _VOICE_CORE.set_text(selector, entry, value)

    def _pick_voice_core_text_entry(self, entries: Iterable[dict]) -> Optional[dict]:
        rows = [dict(item) for item in (entries or []) if isinstance(item, dict)]
        picked = (
            VoiceCoreEntityClient.find_best(rows, "text", ["microwakeword", "model", "url"])
            or VoiceCoreEntityClient.find_best(rows, "text", ["wakeword", "model", "url"])
            or VoiceCoreEntityClient.find_best(rows, "text", ["model", "url"], optional_parts=["micro", "wake"])
        )
        return picked

    async def _resolve_voice_core_text_entity(self, context: dict | None) -> Tuple[str, Optional[dict], List[dict]]:
        selector = self._voice_core_selector(context)
        if not selector:
            return "", None, []
        try:
            payload = await asyncio.to_thread(self._voice_core_entities_sync, selector)
        except Exception as exc:
            logger.warning("[microwakeword] Voice Core entity fetch failed for %s: %s", selector, exc)
            return selector, None, []
        entries = payload.get("entities") if isinstance(payload.get("entities"), list) else []
        return selector, self._pick_voice_core_text_entry(entries), entries

    # -------------------------------------------------------------
    # Actions
    # -------------------------------------------------------------

    def _suggestion_summary(self, target: str, suggestions: List[dict]) -> str:
        labels = [self._format_option(item) for item in suggestions]
        if target:
            return f"I do not have an exact microWakeWord match for {target}. Close options are: {', '.join(labels)}."
        return f"Here are a few wake words I can use: {', '.join(labels)}."

    def _suggestions_result(self, target: str, entries: List[dict]) -> dict:
        limit = self._get_int_setting("MAX_SUGGESTIONS", 5, 1, 10)
        suggestions = self._suggestions_for(target, entries, limit)
        return action_success(
            facts={
                "action": "suggest",
                "requested_wake_word": target,
                "suggestions": [self._format_option(item) for item in suggestions],
            },
            data={"suggestions": suggestions},
            summary_for_user=self._suggestion_summary(target, suggestions),
            say_hint=(
                "Tell the user these are available wake-word options. "
                "If they asked for a missing name, explain that these are close matches and ask which one to use."
            ),
        )

    async def _set_voice_core_model(self, context: dict | None, model_url: str) -> Tuple[bool, str, str]:
        selector, entry, _ = await self._resolve_voice_core_text_entity(context)
        if not selector:
            return False, "", "missing_selector"
        if not entry:
            return False, selector, "missing_text_entity"
        try:
            await asyncio.to_thread(self._voice_core_set_text_sync, selector, entry, model_url)
            return True, selector, ""
        except Exception as exc:
            logger.error("[microwakeword] Voice Core text_set failed for %s: %s", selector, exc)
            return False, selector, str(exc)

    async def _read_status(self, context: dict | None, entries: List[dict]) -> dict:
        selector, entry, _ = await self._resolve_voice_core_text_entity(context)
        if not entry:
            return self._missing_entity_result(selector)
        raw = VoiceCoreEntityClient.state_value(entry)
        value = self._text(raw)
        match = self._exact_match(value, entries)
        label = self._format_option(match) if match else (value or "compiled default")
        return action_success(
            facts={"action": "status", "wake_word": label, "model_url": value, "target": selector},
            summary_for_user=f"The current microWakeWord setting is {label}.",
            say_hint="Report the current microWakeWord setting.",
        )

    def _missing_entity_result(self, selector: str = "") -> dict:
        needs = []
        if not self._text(selector):
            needs.append(
                "Use this request from the target Voice Core satellite, or configure MICROWAKEWORD_VOICE_CORE_SELECTOR in the Verba settings."
            )
        else:
            needs.append(
                "Make sure the selected Voice Core satellite exposes a text entity whose name includes microWakeWord, model, and URL."
            )
        return action_failure(
            code="microwakeword_text_entity_not_found",
            message="Could not find the Voice Core microWakeWord Model URL text entity.",
            needs=needs,
            say_hint=(
                "Explain that the wake-word text entity could not be found. "
                "Ask the user to try from the target satellite or configure the Voice Core selector."
            ),
        )

    async def _apply_model_url(
        self,
        context: dict | None,
        model_url: str,
        label: str,
    ) -> dict:
        ok, target, error = await self._set_voice_core_model(context, model_url)
        if ok:
            return action_success(
                facts={"action": "set", "wake_word": label, "model_url": model_url, "target": target},
                summary_for_user=f"microWakeWord is now set to {label}.",
                say_hint=(
                    f"Confirm the new wake word is {label}. Mention it may take a few moments for the satellite "
                    "to download and activate the model."
                ),
            )
        if error in {"missing_selector", "missing_text_entity"}:
            return self._missing_entity_result(target)
        return action_failure(
            code="voice_core_text_set_failed",
            message=f"Voice Core could not set the microWakeWord text entity: {error}",
            say_hint="Explain the Voice Core entity update failed.",
        )

    async def _handle(self, args: Any, llm_client=None, context: dict | None = None):
        payload = self._normalize_args(args)
        request_text = self._merged_request_text(payload)
        target = self._extract_target(payload, request_text)
        action = self._infer_action(payload, request_text, target)

        try:
            entries = self._load_entries()
        except Exception as exc:
            return action_failure(
                code="wake_word_manifest_unavailable",
                message=f"Could not load the wake-word manifest: {exc}",
                say_hint="Explain the wake-word list is unavailable and suggest retrying later.",
            )

        if action == "status":
            return await self._read_status(context, entries)

        if action == "reset":
            return await self._apply_model_url(
                context,
                "compiled",
                "the compiled default wake word",
            )

        if action == "suggest":
            return self._suggestions_result(target, entries)

        match = self._exact_match(target, entries)
        if not match:
            return self._suggestions_result(target, entries)

        model_url = self._text(match.get("download_url") or match.get("url"))
        label = self._format_option(match)
        return await self._apply_model_url(
            context,
            model_url,
            label,
        )

    # -------------------------------------------------------------
    # Platform handlers
    # -------------------------------------------------------------

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args or {}, llm_client, context=context)

    async def handle_homekit(self, args=None, llm_client=None, context=None):
        return await self._handle(args or {}, llm_client, context=context)

    async def handle_xbmc(self, args=None, llm_client=None, context=None):
        return await self._handle(args or {}, llm_client, context=context)

    async def handle_webui(self, args=None, llm_client=None, context=None):
        return await self._handle(args or {}, llm_client, context=context)

    async def handle_macos(self, args=None, llm_client=None, context=None):
        return await self.handle_webui(args=args, llm_client=llm_client, context=context)

    async def handle_discord(self, message=None, args=None, llm_client=None):
        return await self.handle_webui(args=args, llm_client=llm_client, context=None)

    async def handle_telegram(self, update=None, args=None, llm_client=None):
        return await self.handle_webui(args=args, llm_client=llm_client, context=None)

    async def handle_matrix(self, client=None, room=None, sender=None, body=None, args=None, llm_client=None, **kwargs):
        return await self.handle_webui(args=args, llm_client=llm_client, context=None)

    async def handle_meshtastic(self, args=None, llm_client=None, context=None, **kwargs):
        args = args or {}
        ctx = context if isinstance(context, dict) else {}
        origin = ctx.get("origin") if isinstance(ctx.get("origin"), dict) else {}
        raw_text = str(
            ctx.get("raw_message")
            or ctx.get("raw")
            or ctx.get("request_text")
            or origin.get("text")
            or origin.get("message")
            or origin.get("body")
            or ""
        ).strip()
        payload = self._normalize_args(args)
        if raw_text and not any(self._text(payload.get(k)) for k in ("query", "request", "message", "text")):
            payload["query"] = raw_text
        return await self.handle_webui(payload, llm_client, context=ctx)

    async def handle_irc(self, bot=None, channel=None, user=None, raw_message=None, args=None, llm_client=None):
        payload = self._normalize_args(args or {})
        if raw_message and not any(self._text(payload.get(k)) for k in ("query", "request", "message", "text")):
            payload["query"] = raw_message
        return await self.handle_webui(payload, llm_client, context=None)


verba = MicroWakeWordPlugin()
