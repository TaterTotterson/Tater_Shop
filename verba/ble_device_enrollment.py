"""Voice and UI entry point for Tater's one-shot BLE device enrollment."""

from __future__ import annotations

from typing import Any, Dict

from verba_base import ToolVerba
from verba_result import action_failure, action_success
from voice_core_entities import VoiceCoreEntityClient


class BleDeviceEnrollmentPlugin(ToolVerba):
    name = "ble_device_enrollment"
    verba_name = "Enroll My Device"
    pretty_name = "Enroll My Device"
    version = "1.0.0"
    min_tater_version = "99"
    settings_category = "BLE Presence"

    usage = (
        '{"function":"ble_device_enrollment","arguments":{"query":"enroll my watch",'
        '"name":"My watch","category":"watch"}}'
    )
    description = (
        "Start, cancel, or check a private one-shot Bluetooth enrollment window on the speaking "
        "Tater satellite. Use this to add a phone, watch, or wearable to room presence tracking."
    )
    verba_dec = "Enroll a phone or watch for private room-presence tracking."
    when_to_use = (
        "Use when the user says enroll, add, pair, or track my phone, watch, wearable, or Bluetooth device; "
        "also use for BLE enrollment status or cancellation."
    )
    how_to_use = (
        "Pass the user's natural request in query. The speaking satellite is selected automatically. "
        "After starting, tell the user to open Bluetooth settings and select the temporary Tater Enroll name."
    )
    common_needs = [
        "Enroll my watch",
        "Add my phone to room presence",
        "Cancel Bluetooth enrollment",
        "Is device enrollment still running?",
    ]
    missing_info_prompts = []
    example_calls = [
        '{"function":"ble_device_enrollment","arguments":{"query":"enroll my watch"}}',
        '{"function":"ble_device_enrollment","arguments":{"query":"add my phone to room presence"}}',
        '{"function":"ble_device_enrollment","arguments":{"query":"cancel device enrollment"}}',
    ]
    platforms = ["voice_core", "webui", "macos", "little_spud"]
    tags = ["bluetooth", "ble", "presence", "watch", "phone"]
    routing_keywords = [
        "enroll my device",
        "enroll my watch",
        "enroll my phone",
        "add my watch",
        "add my phone",
        "bluetooth enrollment",
    ]

    @staticmethod
    def _text(value: Any) -> str:
        return str(value or "").strip()

    @classmethod
    def _query(cls, args: Any) -> str:
        values = args if isinstance(args, dict) else {}
        return cls._text(values.get("query") or values.get("request") or values.get("text"))

    @classmethod
    def _action(cls, args: Any) -> str:
        values = args if isinstance(args, dict) else {}
        explicit = cls._text(values.get("action")).lower()
        if explicit in {"start", "status", "cancel"}:
            return explicit
        query = cls._query(values).lower()
        if any(word in query for word in ("cancel", "stop", "never mind", "nevermind")):
            return "cancel"
        if any(phrase in query for phrase in ("status", "still running", "still pairing", "how long")):
            return "status"
        return "start" if query else ""

    @classmethod
    def _category(cls, args: Any) -> str:
        values = args if isinstance(args, dict) else {}
        explicit = cls._text(values.get("category")).lower()
        if explicit:
            return explicit[:40]
        query = cls._query(values).lower()
        for category, words in (
            ("watch", ("watch", "wearable")),
            ("phone", ("phone", "iphone", "android")),
            ("tablet", ("tablet", "ipad")),
            ("tracker", ("tracker", "tag")),
        ):
            if any(word in query for word in words):
                return category
        return "device"

    @classmethod
    def _owner(cls, args: Any, context: Any) -> str:
        values = args if isinstance(args, dict) else {}
        explicit = cls._text(values.get("owner"))
        if explicit:
            return explicit[:80]
        ctx = context if isinstance(context, dict) else {}
        origin = ctx.get("origin") if isinstance(ctx.get("origin"), dict) else {}
        for key in ("user_name", "person_name", "speaker_name", "user"):
            value = cls._text(ctx.get(key) or origin.get(key))
            if value:
                return value[:80]
        return ""

    @classmethod
    def _name(cls, args: Any, category: str) -> str:
        values = args if isinstance(args, dict) else {}
        return cls._text(values.get("name") or values.get("device_name"))[:80] or f"My {category}"

    async def _handle(self, args: Any, context: Any = None) -> Dict[str, Any]:
        from tater_voice import ble_enrollment

        action = self._action(args)
        if not action:
            return action_failure(
                code="missing_request",
                message="No BLE enrollment request was provided.",
                needs=["Ask to enroll a phone, watch, wearable, or Bluetooth device."],
                say_hint="Ask which device the user wants to enroll.",
            )
        try:
            if action == "status":
                snapshot = await ble_enrollment.snapshot()
                active = snapshot.get("active") if isinstance(snapshot, dict) else None
                if not isinstance(active, dict):
                    return action_success(
                        facts={"action": "status", "active": False},
                        summary_for_user="No Bluetooth device enrollment is running right now.",
                        say_hint="Say that no enrollment is currently running.",
                    )
                return action_success(
                    facts={"action": "status", "active": True, "session": active},
                    summary_for_user=(
                        f"Bluetooth enrollment is {active.get('status', 'running')} on "
                        f"{(active.get('satellite') or {}).get('name', 'the satellite')}. "
                        f"Select {active.get('display_name', 'the Tater Enroll device')} in Bluetooth settings."
                    ),
                    say_hint="Report the active enrollment name and selected satellite.",
                )
            if action == "cancel":
                result = await ble_enrollment.cancel()
                return action_success(
                    facts={"action": "cancel", "session": result.get("session")},
                    summary_for_user="Bluetooth device enrollment has been cancelled.",
                    say_hint="Confirm that enrollment was cancelled.",
                )

            values = args if isinstance(args, dict) else {}
            category = self._category(values)
            selector = VoiceCoreEntityClient.selector_from_context(context)
            result = await ble_enrollment.start(
                selector=selector,
                name=self._name(values, category),
                owner=self._owner(values, context),
                category=category,
                timeout_s=values.get("timeout_s") or 90,
                source="verba",
            )
            session = result.get("session") if isinstance(result, dict) else {}
            satellite = session.get("satellite") if isinstance(session, dict) else {}
            return action_success(
                facts={"action": "start", "session": session},
                summary_for_user=(
                    f"Enrollment is ready on {satellite.get('name', 'the nearby satellite')}. "
                    f"Open Bluetooth settings on your {category} and select "
                    f"{session.get('display_name', 'Tater Enroll')}."
                ),
                say_hint="Give the temporary Bluetooth name and tell the user to select it in Bluetooth settings.",
            )
        except (RuntimeError, ValueError) as exc:
            return action_failure(
                code="ble_enrollment_failed",
                message=str(exc),
                say_hint="Briefly explain why Bluetooth enrollment could not start and suggest trying a compatible nearby satellite.",
            )

    async def handle_voice_core(self, args=None, llm_client=None, context=None, *unused, **kwargs):
        return await self._handle(args or {}, context)

    async def handle_webui(self, args=None, llm_client=None, context=None, *unused, **kwargs):
        return await self._handle(args or {}, context)

    async def handle_macos(self, args=None, llm_client=None, context=None, *unused, **kwargs):
        return await self._handle(args or {}, context)

    async def handle_little_spud(self, args=None, llm_client=None, context=None, *unused, **kwargs):
        return await self._handle(args or {}, context)


verba = BleDeviceEnrollmentPlugin()
