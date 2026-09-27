"""Small, deterministic Verba starter for Tater."""

from typing import Any, Dict

from verba_base import ToolVerba
from verba_result import action_failure, action_success


class ExampleLookupVerba(ToolVerba):
    name = "example_lookup"
    verba_name = "Example Lookup"
    pretty_name = "Example Lookup"
    version = "1.0.0"
    min_tater_version = "187"

    usage = '{"function":"example_lookup","arguments":{"query":"check the example status"}}'
    description = "Return a small example status or value."
    verba_dec = description
    when_to_use = "Use when the user asks for the example status or example value."
    how_to_use = "Pass the user's complete request in query."
    common_needs = ["query"]
    missing_info_prompts = ["What example information should I look up?"]
    example_calls = [
        '{"function":"example_lookup","arguments":{"query":"check the example status"}}',
        '{"function":"example_lookup","arguments":{"query":"show the example value"}}',
    ]
    argument_schema = {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The user's complete example lookup request.",
            }
        },
        "required": ["query"],
    }

    platforms = [
        "webui",
        "macos",
        "voice_core",
        "discord",
        "telegram",
        "matrix",
        "irc",
        "meshtastic",
    ]
    tags = ["example"]

    @staticmethod
    def _query(args: Dict[str, Any]) -> str:
        return str((args or {}).get("query") or (args or {}).get("request") or "").strip()

    async def _handle(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query = self._query(args)
        if not query:
            return action_failure(
                code="missing_query",
                message="No example lookup query was provided.",
                needs=["Provide what should be looked up."],
                say_hint="Ask what example information the user wants.",
            )

        lowered = query.lower()
        if "status" in lowered:
            facts = {"action": "status", "status": "ready"}
            summary = "The example service is ready."
        elif "value" in lowered or "look up" in lowered or "lookup" in lowered:
            facts = {"action": "value", "value": "example result"}
            summary = "The example value is example result."
        else:
            return action_failure(
                code="unknown_action",
                message="The request did not ask for example status or value.",
                needs=["Ask for status or value."],
                say_hint="Ask whether the user wants example status or value.",
            )

        return action_success(
            facts=facts,
            summary_for_user=summary,
            say_hint="Report the example result briefly.",
        )

    async def handle_webui(self, args, llm_client):
        return await self._handle(args or {})

    async def handle_macos(self, args, llm_client, context=None):
        return await self._handle(args or {})

    async def handle_voice_core(self, args=None, llm_client=None, context=None, **_kwargs):
        return await self._handle(args or {})

    async def handle_discord(self, message, args, llm_client):
        return await self._handle(args or {})

    async def handle_telegram(self, update, args, llm_client):
        return await self._handle(args or {})

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **_kwargs):
        return await self._handle(args or {})

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._handle(args or {})

    async def handle_meshtastic(self, args=None, llm_client=None, context=None, **_kwargs):
        return await self._handle(args or {})


verba = ExampleLookupVerba()
