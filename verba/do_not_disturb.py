import re
from typing import Any

from verba_base import ToolVerba


class DoNotDisturbPlugin(ToolVerba):
    name = "do_not_disturb"
    verba_name = "Do Not Disturb"
    pretty_name = "Satellite Do Not Disturb"
    version = "1.0.0"
    min_tater_version = "98.4"
    settings_category = "Satellites"
    platforms = ["voice_core", "webui", "little_spud", "macos"]
    usage = '{"function":"do_not_disturb","arguments":{"action":"on|off|status","target":"optional satellite or room"}}'
    description = (
        "Turn Do Not Disturb on or off for the satellite the user is speaking to, or for a named satellite/room. "
        "DND silences music, announcements, wake sounds, and non-critical playback while preserving timers and alarms. "
        "Use for requests such as 'enable DND', 'do not disturb', 'turn off do not disturb', or 'is DND on?'."
    )
    verba_dec = "Control per-satellite Do Not Disturb."
    when_to_use = "Use whenever the user asks to enable, disable, or check DND or Do Not Disturb on a satellite."
    how_to_use = "Pass action as on, off, or status. Omit target to use the satellite that heard the request."
    common_needs = ["Enable DND", "Disable DND", "Do not disturb", "Is DND on?"]
    missing_info_prompts = ["Which satellite or room should I use?"]
    argument_schema = {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["on", "off", "status"],
                "description": "Whether to enable, disable, or check DND.",
            },
            "target": {
                "type": "string",
                "description": "Optional satellite name, room, or 'all satellites'.",
            },
            "query": {
                "type": "string",
                "description": "The original natural-language DND request when action was not extracted.",
            },
        },
    }
    example_calls = [
        '{"function":"do_not_disturb","arguments":{"action":"on"}}',
        '{"function":"do_not_disturb","arguments":{"action":"off"}}',
        '{"function":"do_not_disturb","arguments":{"action":"status","target":"bedroom"}}',
    ]

    @staticmethod
    def _norm(value: Any) -> str:
        return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()

    @classmethod
    def _action(cls, args: dict) -> str:
        explicit = cls._norm((args or {}).get("action"))
        if explicit in {"on", "enable", "enabled", "start"}:
            return "on"
        if explicit in {"off", "disable", "disabled", "stop"}:
            return "off"
        if explicit in {"status", "check", "state"}:
            return "status"
        query = cls._norm((args or {}).get("query") or (args or {}).get("request"))
        if re.search(r"\b(?:turn|switch|set)?\s*(?:off|disable|stop)\b", query):
            return "off"
        if re.search(r"\b(?:status|enabled|active|on)\b", query) and re.search(r"\b(?:is|check|whether)\b", query):
            return "status"
        return "on"

    @staticmethod
    def _context_selector(context: dict | None) -> str:
        try:
            from voice_core_entities import VoiceCoreEntityClient

            selector = VoiceCoreEntityClient.selector_from_context(context)
            if selector:
                return str(selector).strip()
        except Exception:
            pass
        ctx = context if isinstance(context, dict) else {}
        origin = ctx.get("origin") if isinstance(ctx.get("origin"), dict) else {}
        for key in ("satellite_selector", "selector", "device_id"):
            selector = str(ctx.get(key) or origin.get(key) or "").strip()
            if selector:
                return selector
        return ""

    @classmethod
    def _matches(cls, query: str, selector: str, row: dict) -> bool:
        needle = cls._norm(query)
        if not needle:
            return False
        values = {
            cls._norm(selector),
            cls._norm(selector.removeprefix("native:")),
            cls._norm(row.get("device_name")),
            cls._norm(row.get("room")),
            cls._norm(row.get("board")),
        }
        return needle in values or any(needle in value or value in needle for value in values if value)

    async def _selectors(self, target: str, context: dict | None) -> tuple[list[str], dict]:
        from tater_voice import native_satellite

        snapshot = await native_satellite.status()
        clients = snapshot.get("clients") if isinstance(snapshot.get("clients"), dict) else {}
        connected = {
            str(selector): row
            for selector, row in clients.items()
            if isinstance(row, dict) and bool(row.get("connected"))
        }
        target_norm = self._norm(target)
        if target_norm in {"all", "all satellites", "every satellite", "everywhere", "whole house", "house"}:
            return sorted(connected), connected
        if target_norm:
            matches = [
                selector
                for selector, row in connected.items()
                if self._matches(target, selector, row)
            ]
            return sorted(set(matches)), connected
        current = self._context_selector(context)
        if current and current not in connected and not current.startswith("native:"):
            current = f"native:{current}"
        return ([current] if current in connected else []), connected

    async def _handle(self, args=None, context=None) -> str:
        from tater_voice import native_live_settings, native_satellite

        values = args if isinstance(args, dict) else {}
        action = self._action(values)
        target = str(values.get("target") or values.get("room") or values.get("satellite") or "").strip()
        selectors, clients = await self._selectors(target, context)
        if not selectors:
            if target:
                return f"I couldn't find a connected Tater satellite matching {target}."
            return "I couldn't determine which connected Tater satellite should use Do Not Disturb."

        if action == "status":
            active = []
            inactive = []
            for selector in selectors:
                row = clients.get(selector) or {}
                if native_live_settings.dnd_active(selector, board=row.get("board")):
                    active.append(row.get("device_name") or row.get("room") or selector)
                else:
                    inactive.append(row.get("device_name") or row.get("room") or selector)
            if active and not inactive:
                return "Do Not Disturb is on for " + ", ".join(str(item) for item in active) + "."
            if inactive and not active:
                return "Do Not Disturb is off for " + ", ".join(str(item) for item in inactive) + "."
            return "DND is on for " + ", ".join(str(item) for item in active) + "; it is off for " + ", ".join(str(item) for item in inactive) + "."

        enabled = action == "on"
        for selector in selectors:
            await native_satellite.save_live_settings(
                {"dnd_enabled": enabled},
                selector=selector,
                push=True,
            )

        names = [
            str((clients.get(selector) or {}).get("device_name") or (clients.get(selector) or {}).get("room") or selector)
            for selector in selectors
        ]
        if enabled:
            return "Do Not Disturb is now on for " + ", ".join(names) + ". Timers and alarms will still sound."

        still_scheduled = [
            name
            for selector, name in zip(selectors, names)
            if native_live_settings.dnd_active(selector, board=(clients.get(selector) or {}).get("board"))
        ]
        if still_scheduled:
            return (
                "Manual DND is off for " + ", ".join(names)
                + ", but the daily schedule is currently keeping DND active for " + ", ".join(still_scheduled) + "."
            )
        return "Do Not Disturb is now off for " + ", ".join(names) + "."

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, context=context)

    async def handle_webui(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, context=context)

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, context=context)

    async def handle_macos(self, args=None, llm_client=None, context=None, *unused_args, **unused_kwargs):
        return await self._handle(args, context=context)


verba = DoNotDisturbPlugin()
