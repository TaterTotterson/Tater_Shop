# verba/events_query.py
import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from verba_base import ToolVerba
from helpers import redis_client, extract_json
from verba_result import action_failure, action_success

logger = logging.getLogger("events_query")
logger.setLevel(logging.INFO)


class EventsQueryPlugin(ToolVerba):
    name = "events_query"
    verba_name = "Events Query"
    pretty_name = "Events Query"
    version = "2.0.0"
    min_tater_version = "59"
    description = (
        "Natural-language semantic search over stored home events. "
        "Interprets request intent, area, timeframe, and semantic details with an LLM."
    )
    verba_dec = "Natural-language semantic event-history search."
    when_to_use = (
        "Use when the user asks what happened, who/what was seen, counts, presence, or semantic details "
        "from stored home event history."
    )
    how_to_use = "Pass one natural-language query. The plugin handles interpretation, search, and final answering."
    usage = '{"function":"events_query","arguments":{"query":"what happened in the front yard today?"}}'
    example_calls = [
        '{"function":"events_query","arguments":{"query":"what happened in the front yard today?"}}',
        '{"function":"events_query","arguments":{"query":"did a delivery person come by today?"}}',
        '{"function":"events_query","arguments":{"query":"find people wearing a blue shirt this morning"}}',
    ]

    platforms = ["webui", "macos", "homeassistant", "homekit", "discord", "telegram", "matrix", "irc"]
    settings_category = "Events Query"
    required_settings = {}

    waiting_prompt_template = (
        "Let {mention} know you are checking event history now. "
        "Keep it short and friendly. No emojis. Only output that message."
    )
    common_needs = ["A natural-language event-history question."]
    missing_info_prompts = ["What event-history question should I search for?"]

    MAX_EVENTS_PER_SOURCE = 1000
    MAX_CANDIDATE_EVENTS_FOR_LLM = 320
    MAX_RELEVANT_EVENTS_FOR_ANSWER = 180

    @staticmethod
    def _source_to_area(source: str) -> str:
        text = str(source or "").strip().lower().replace("_", " ")
        return " ".join(text.split())

    @staticmethod
    def _iso(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _parse_local_iso(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except Exception:
            return None
        # Automations event timestamps are stored as local naive ISO.
        if parsed.tzinfo is not None:
            parsed = parsed.replace(tzinfo=None)
        return parsed

    @staticmethod
    def _query_from_args(args: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
        args = args or {}
        for key in ("query", "request", "question", "user_query", "prompt", "text", "content", "message"):
            value = args.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        origin = args.get("origin")
        if isinstance(origin, dict):
            for key in ("request_text", "query", "question", "text", "content", "message"):
                value = origin.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

        if isinstance(context, dict):
            for key in ("request_text", "query", "question", "text", "content", "message", "raw_message", "body"):
                value = context.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ""

    @staticmethod
    def _discover_sources() -> List[str]:
        prefix = "tater:automations:events:"
        out: List[str] = []
        try:
            for key in redis_client.scan_iter(match=f"{prefix}*", count=500):
                source = str(key).split(":", maxsplit=3)[-1]
                source = str(source or "").strip()
                if source and source not in out:
                    out.append(source)
        except Exception as exc:
            logger.warning("[events_query] source discovery failed: %s", exc)
        return sorted(out)

    async def _fetch_one_source(
        self,
        *,
        source: str,
        since: datetime,
        until: datetime,
        limit: int,
        client: Any = None,
        base: str = "",
    ) -> List[Dict[str, Any]]:
        del client, base
        key = f"tater:automations:events:{source}"
        upper = max(1, min(1000, int(limit)))
        try:
            rows = redis_client.lrange(key, 0, upper - 1) or []
        except Exception as exc:
            logger.warning("[events_query] fetch failed for source=%s: %s", source, exc)
            return []

        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                item = json.loads(row)
            except Exception:
                continue
            if not isinstance(item, dict):
                continue
            event_dt = self._event_dt(item)
            if event_dt is None:
                continue
            if event_dt < since or event_dt > until:
                continue
            normalized = dict(item)
            normalized.setdefault("source", source)
            out.append(normalized)
        return out

    async def _fetch_sources_window(
        self,
        *,
        sources: List[str],
        since: datetime,
        until: datetime,
        per_source_limit: int,
    ) -> List[Dict[str, Any]]:
        if not sources:
            return []
        merged: List[Dict[str, Any]] = []
        for source in sources:
            out: List[Dict[str, Any]] = []
            try:
                out = await self._fetch_one_source(
                    source=source,
                    since=since,
                    until=until,
                    limit=per_source_limit,
                )
            except Exception as exc:
                logger.warning("[events_query] fetch task failed: %s", exc)
                continue
            merged.extend(out)
        return merged

    @staticmethod
    def _event_dt(event: Dict[str, Any]) -> Optional[datetime]:
        ts = str(event.get("ha_time") or "").strip()
        if not ts:
            return None
        try:
            parsed = datetime.fromisoformat(ts)
            if parsed.tzinfo is not None:
                parsed = parsed.replace(tzinfo=None)
            return parsed
        except Exception:
            return None

    @staticmethod
    def _event_id(event: Dict[str, Any]) -> str:
        src = str(event.get("source") or "").strip()
        ha_time = str(event.get("ha_time") or "").strip()
        title = str(event.get("title") or "").strip()
        message = str(event.get("message") or "").strip()
        entity = str(event.get("entity_id") or "").strip()
        seed = "|".join([src, ha_time, title, message, entity])
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
        return f"ev_{digest[:16]}"

    def _compact_event_for_llm(self, event: Dict[str, Any]) -> Dict[str, Any]:
        source = str(event.get("source") or "").strip()
        area = self._source_to_area(source) or str(((event.get("data") or {}).get("area") or "")).strip()
        event_id = self._event_id(event)
        data_payload = event.get("data") if isinstance(event.get("data"), dict) else {}
        return {
            "event_id": event_id,
            "source": source,
            "area": area,
            "ha_time": str(event.get("ha_time") or "").strip(),
            "title": str(event.get("title") or "").strip(),
            "message": str(event.get("message") or "").strip(),
            "type": str(event.get("type") or "").strip(),
            "entity_id": str(event.get("entity_id") or "").strip(),
            "level": str(event.get("level") or "").strip(),
            "data": data_payload,
        }

    async def _llm_json_object(
        self,
        *,
        llm_client: Any,
        system_prompt: str,
        user_payload: Dict[str, Any],
        max_tokens: int = 700,
        temperature: float = 0.0,
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        if llm_client is None:
            return None, "LLM client is unavailable."
        try:
            response = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                temperature=temperature,
                max_tokens=max(80, int(max_tokens or 700)),
                timeout_ms=45_000,
            )
        except Exception as exc:
            return None, f"LLM request failed: {exc}"

        raw = str(((response.get("message") or {}).get("content") or "")).strip()
        parsed_text = extract_json(raw) or raw
        try:
            obj = json.loads(parsed_text)
        except Exception as exc:
            return None, f"Could not parse LLM JSON: {exc}"
        if not isinstance(obj, dict):
            return None, "LLM did not return a JSON object."
        return obj, ""

    async def _interpret_query(
        self,
        *,
        llm_client: Any,
        user_query: str,
        sources: List[str],
        now_local: datetime,
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        source_rows = [
            {"source_id": source, "area_name": self._source_to_area(source)}
            for source in sources
        ]
        system_prompt = (
            "You interpret natural-language event-history requests.\n"
            "Return exactly one strict JSON object with this schema:\n"
            "{"
            "\"query_type\":\"summary|presence|count|semantic_search|timeline\","
            "\"search_scope\":\"selected_sources|all_sources\","
            "\"source_ids\":[\"<source_id>\"],"
            "\"time_window\":{\"start_local\":\"YYYY-MM-DDTHH:MM:SS\",\"end_local\":\"YYYY-MM-DDTHH:MM:SS\",\"label\":\"...\"},"
            "\"semantic_focus\":[\"...\"],"
            "\"response_mode\":\"summary|presence|count|matches\""
            "}\n"
            "Rules:\n"
            "- Use only source_ids from the provided source catalog.\n"
            "- If the user asks broadly (for example around the house/outside), use search_scope=all_sources.\n"
            "- time_window must always include both start_local and end_local in local naive ISO.\n"
            "- Preserve user intent including area, timeframe, and semantic details (people/clothing/vehicles/packages/animals/unusual activity).\n"
            "- Do not answer the user.\n"
            "- Do not invent sources that are not in the catalog.\n"
        )
        payload = {
            "user_query": user_query,
            "now_local": self._iso(now_local),
            "available_sources": source_rows,
        }
        return await self._llm_json_object(
            llm_client=llm_client,
            system_prompt=system_prompt,
            user_payload=payload,
            max_tokens=800,
            temperature=0.0,
        )

    def _normalize_interpretation(
        self,
        *,
        interpretation: Dict[str, Any],
        sources_catalog: List[str],
        now_local: datetime,
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        catalog = set(sources_catalog)
        query_type = str(interpretation.get("query_type") or "").strip().lower()
        if query_type not in {"summary", "presence", "count", "semantic_search", "timeline"}:
            query_type = "summary"

        response_mode = str(interpretation.get("response_mode") or "").strip().lower()
        if response_mode not in {"summary", "presence", "count", "matches"}:
            response_mode = "summary"

        search_scope = str(interpretation.get("search_scope") or "").strip().lower()
        source_ids_raw = interpretation.get("source_ids") if isinstance(interpretation.get("source_ids"), list) else []
        source_ids = [str(item).strip() for item in source_ids_raw if str(item).strip() in catalog]

        if search_scope == "all_sources":
            selected_sources = list(sources_catalog)
        else:
            selected_sources = sorted(set(source_ids))

        if not selected_sources:
            return None, "Could not resolve relevant event sources from request interpretation."

        time_window = interpretation.get("time_window") if isinstance(interpretation.get("time_window"), dict) else {}
        start_local = self._parse_local_iso(time_window.get("start_local"))
        end_local = self._parse_local_iso(time_window.get("end_local"))
        label = str(time_window.get("label") or "").strip() or "requested timeframe"

        if start_local is None or end_local is None:
            return None, "Could not resolve a valid timeframe from request interpretation."
        if end_local < start_local:
            return None, "Interpreted timeframe end is earlier than start."

        # Keep LLM-selected windows for summary/count/matches as-is (helps avoid
        # false-empty windows when host/user local clocks differ). Clamp only for
        # explicit presence-style "right now" intent.
        if end_local > now_local and response_mode == "presence":
            end_local = now_local

        focus_raw = interpretation.get("semantic_focus") if isinstance(interpretation.get("semantic_focus"), list) else []
        semantic_focus = [str(item).strip() for item in focus_raw if str(item).strip()][:24]

        broad_summary = bool(
            query_type in {"summary", "timeline"}
            and response_mode == "summary"
            and not semantic_focus
        )

        return (
            {
                "query_type": query_type,
                "response_mode": response_mode,
                "search_scope": search_scope,
                "selected_sources": selected_sources,
                "time_label": label,
                "time_start": start_local,
                "time_end": end_local,
                "semantic_focus": semantic_focus,
                "broad_summary": broad_summary,
            },
            "",
        )

    async def _select_relevant_event_ids(
        self,
        *,
        llm_client: Any,
        user_query: str,
        interpretation: Dict[str, Any],
        candidate_events: List[Dict[str, Any]],
    ) -> Tuple[Optional[List[str]], str]:
        system_prompt = (
            "You are selecting relevant home events for a user question.\n"
            "Return exactly one strict JSON object:\n"
            "{"
            "\"relevant_event_ids\":[\"ev_...\"],"
            "\"confidence\":\"high|medium|low\""
            "}\n"
            "Rules:\n"
            "- Select only event_ids that are directly relevant to the user's request.\n"
            "- Use only event_ids from the provided candidate list.\n"
            "- If none are relevant, return an empty list.\n"
            "- If interpreted_request.broad_summary is true and candidate_events is non-empty, do not return an empty list.\n"
            "- Do not invent events.\n"
        )
        payload = {
            "user_query": user_query,
            "interpreted_request": {
                "query_type": interpretation.get("query_type"),
                "response_mode": interpretation.get("response_mode"),
                "time_label": interpretation.get("time_label"),
                "semantic_focus": interpretation.get("semantic_focus"),
                "broad_summary": bool(interpretation.get("broad_summary")),
            },
            "candidate_events": candidate_events,
        }
        obj, err = await self._llm_json_object(
            llm_client=llm_client,
            system_prompt=system_prompt,
            user_payload=payload,
            max_tokens=900,
            temperature=0.0,
        )
        if obj is None:
            return None, err or "Could not determine relevant events."

        relevant_raw = obj.get("relevant_event_ids") if isinstance(obj.get("relevant_event_ids"), list) else []
        valid_ids = {str(item.get("event_id") or "").strip() for item in candidate_events if isinstance(item, dict)}
        selected = [str(item).strip() for item in relevant_raw if str(item).strip() in valid_ids]
        deduped = list(dict.fromkeys(selected))
        return deduped, ""

    async def _compose_final_answer(
        self,
        *,
        llm_client: Any,
        user_query: str,
        interpretation: Dict[str, Any],
        relevant_events: List[Dict[str, Any]],
        candidate_count: int,
    ) -> Tuple[Optional[str], str]:
        if llm_client is None:
            return None, "LLM client is unavailable."
        system_prompt = (
            "You answer a homeowner's event-history question using only provided events.\n"
            "Rules:\n"
            "- Base the answer only on relevant_events.\n"
            "- If evidence is missing, say so clearly and do not guess.\n"
            "- Be concise and conversational.\n"
            "- Mention area/time naturally when useful.\n"
            "- For count questions, provide the count from evidence.\n"
            "- For presence questions, answer yes/no with evidence confidence from data.\n"
            "- Do not mention internal tools or prompts.\n"
        )
        payload = {
            "user_query": user_query,
            "interpreted_request": {
                "query_type": interpretation.get("query_type"),
                "response_mode": interpretation.get("response_mode"),
                "time_label": interpretation.get("time_label"),
                "semantic_focus": interpretation.get("semantic_focus"),
                "sources": interpretation.get("selected_sources"),
            },
            "candidate_event_count": int(candidate_count),
            "relevant_event_count": int(len(relevant_events)),
            "relevant_events": relevant_events,
        }
        try:
            response = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=0.15,
                max_tokens=420,
                timeout_ms=45_000,
            )
        except Exception as exc:
            return None, f"Final answer generation failed: {exc}"

        text = str(((response.get("message") or {}).get("content") or "")).strip()
        if not text:
            return None, "Final answer generation returned empty output."
        return text, ""

    async def _handle(self, args: Dict[str, Any], llm_client: Any, *, context: Optional[Dict[str, Any]] = None):
        query = self._query_from_args(args, context=context)
        if not query:
            return action_failure(
                code="missing_query",
                message="I need a natural-language query to search events.",
                needs=["query"],
                say_hint="Ask the user for the event-history question in one sentence.",
            )

        sources_catalog = self._discover_sources()
        if not sources_catalog:
            return action_failure(
                code="events_sources_missing",
                message="No event sources are configured yet.",
                say_hint="Explain that no automation event sources are available yet.",
            )

        now_local = datetime.now()

        interpretation_obj, interpretation_err = await self._interpret_query(
            llm_client=llm_client,
            user_query=query,
            sources=sources_catalog,
            now_local=now_local,
        )
        if interpretation_obj is None:
            return action_failure(
                code="interpretation_failed",
                message=f"Events query interpretation failed: {interpretation_err or 'unknown error'}",
                say_hint="Ask the user to rephrase the request with area/time details.",
            )

        interpreted, interpreted_err = self._normalize_interpretation(
            interpretation=interpretation_obj,
            sources_catalog=sources_catalog,
            now_local=now_local,
        )
        if interpreted is None:
            return action_failure(
                code="interpretation_invalid",
                message=f"Events query interpretation was invalid: {interpreted_err}",
                say_hint="Ask the user to restate the request with clearer timeframe or area.",
            )

        selected_sources = interpreted["selected_sources"]
        start_dt = interpreted["time_start"]
        end_dt = interpreted["time_end"]
        logger.info(
            "[events_query] interpreted query_type=%s response_mode=%s sources=%s window=%s..%s label=%s broad_summary=%s",
            interpreted.get("query_type"),
            interpreted.get("response_mode"),
            ",".join(selected_sources),
            self._iso(start_dt),
            self._iso(end_dt),
            interpreted.get("time_label"),
            bool(interpreted.get("broad_summary")),
        )

        fetched = await self._fetch_sources_window(
            sources=selected_sources,
            since=start_dt,
            until=end_dt,
            per_source_limit=self.MAX_EVENTS_PER_SOURCE,
        )

        fetched_sorted = sorted(fetched, key=lambda item: self._event_dt(item) or datetime.min)
        compact_events = [self._compact_event_for_llm(item) for item in fetched_sorted]
        if len(compact_events) > self.MAX_CANDIDATE_EVENTS_FOR_LLM:
            compact_events = compact_events[-self.MAX_CANDIDATE_EVENTS_FOR_LLM :]
        logger.info(
            "[events_query] fetched_events=%s candidate_events=%s",
            len(fetched_sorted),
            len(compact_events),
        )

        relevant_ids, relevance_err = await self._select_relevant_event_ids(
            llm_client=llm_client,
            user_query=query,
            interpretation=interpreted,
            candidate_events=compact_events,
        )
        if relevant_ids is None:
            return action_failure(
                code="relevance_selection_failed",
                message=f"Events relevance selection failed: {relevance_err or 'unknown error'}",
                say_hint="Ask the user to retry the same request.",
            )

        # Guard against false-empty broad summaries from relevance selection.
        if (
            not relevant_ids
            and compact_events
            and bool(interpreted.get("broad_summary"))
        ):
            relevant_ids = [
                str(item.get("event_id") or "").strip()
                for item in compact_events
                if str(item.get("event_id") or "").strip()
            ]
            logger.info(
                "[events_query] relevance returned empty for broad summary; using all candidate events (%s).",
                len(relevant_ids),
            )

        event_by_id = {str(item.get("event_id") or ""): item for item in compact_events}
        relevant_events: List[Dict[str, Any]] = [
            event_by_id[event_id] for event_id in relevant_ids if event_id in event_by_id
        ]
        if len(relevant_events) > self.MAX_RELEVANT_EVENTS_FOR_ANSWER:
            relevant_events = relevant_events[-self.MAX_RELEVANT_EVENTS_FOR_ANSWER :]
        logger.info(
            "[events_query] relevant_event_ids=%s relevant_events=%s",
            len(relevant_ids),
            len(relevant_events),
        )

        final_text, final_err = await self._compose_final_answer(
            llm_client=llm_client,
            user_query=query,
            interpretation=interpreted,
            relevant_events=relevant_events,
            candidate_count=len(compact_events),
        )
        if final_text is None:
            return action_failure(
                code="final_answer_failed",
                message=f"Events final answer failed: {final_err or 'unknown error'}",
                say_hint="Ask the user to retry the request.",
            )

        return action_success(
            facts={
                "intent": interpreted.get("query_type"),
                "response_mode": interpreted.get("response_mode"),
                "timeframe": interpreted.get("time_label"),
                "sources": list(selected_sources),
                "candidate_event_count": int(len(compact_events)),
                "relevant_event_count": int(len(relevant_events)),
            },
            data={
                "time_window": {
                    "start_local": self._iso(start_dt),
                    "end_local": self._iso(end_dt),
                    "label": interpreted.get("time_label"),
                },
                "semantic_focus": list(interpreted.get("semantic_focus") or []),
                "sources": list(selected_sources),
                "candidate_event_count": int(len(compact_events)),
                "relevant_event_count": int(len(relevant_events)),
            },
            summary_for_user=final_text,
            say_hint="Answer only from matched stored events and avoid speculation.",
        )

    # ---------- Platform shims ----------
    async def handle_webui(self, args: Dict[str, Any], llm_client, context: Optional[Dict[str, Any]] = None):
        return await self._handle(args, llm_client, context=context)

    async def handle_macos(self, args, llm_client, context=None):
        return await self._handle(args, llm_client, context=context)

    async def handle_homeassistant(self, args: Dict[str, Any], llm_client, context: Optional[Dict[str, Any]] = None):
        return await self._handle(args, llm_client, context=context)

    async def handle_homekit(self, args: Dict[str, Any], llm_client, context: Optional[Dict[str, Any]] = None):
        return await self._handle(args, llm_client, context=context)

    async def handle_discord(self, message, args, llm_client):
        ctx: Dict[str, Any] = {}
        content = getattr(message, "content", None)
        if isinstance(content, str) and content.strip():
            ctx["request_text"] = content.strip()
        return await self._handle(args, llm_client, context=ctx)

    async def handle_telegram(self, update, args, llm_client):
        ctx: Dict[str, Any] = {}
        try:
            msg = update.get("message") if isinstance(update, dict) else None
            text = msg.get("text") if isinstance(msg, dict) else None
            if isinstance(text, str) and text.strip():
                ctx["request_text"] = text.strip()
        except Exception:
            pass
        return await self._handle(args, llm_client, context=ctx)

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        ctx: Dict[str, Any] = {}
        if isinstance(body, str) and body.strip():
            ctx["request_text"] = body.strip()
        return await self._handle(args, llm_client, context=ctx)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        ctx: Dict[str, Any] = {}
        if isinstance(raw_message, str) and raw_message.strip():
            ctx["request_text"] = raw_message.strip()
        return await self._handle(args, llm_client, context=ctx)


verba = EventsQueryPlugin()
