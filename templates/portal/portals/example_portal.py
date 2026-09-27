"""Async Portal lifecycle and Hydra-call starter for Tater."""

import asyncio
import logging
from typing import Any, Dict, List, Optional

import verba_registry
from helpers import get_llm_client_from_env, redis_client
from hydra import run_hydra_turn

__version__ = "1.0.0"
MIN_TATER_VERSION = "187"
PORTAL_DESCRIPTION = "Example transport Portal for Tater."
TAGS = ["example", "chat"]

logger = logging.getLogger("example_portal")
SETTINGS_KEY = "example_portal_settings"

PORTAL_SETTINGS = {
    "category": "Example Portal Settings",
    "tags": TAGS,
    "required": {
        "api_token": {
            "label": "API token",
            "type": "password",
            "default": "",
            "description": "Credential used to connect to the example platform.",
        },
        "poll_interval_seconds": {
            "label": "Poll interval",
            "type": "number",
            "default": 2,
            "description": "Seconds between checks for new messages.",
        },
    },
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _settings() -> Dict[str, Any]:
    return dict(redis_client.hgetall(SETTINGS_KEY) or {})


def _poll_interval(settings: Dict[str, Any]) -> float:
    try:
        return max(0.25, min(30.0, float(settings.get("poll_interval_seconds") or 2)))
    except Exception:
        return 2.0


def _verba_enabled(verba_id: str) -> bool:
    value = redis_client.hget("verba_enabled", verba_id)
    return _text(value).lower() == "true"


async def _receive_next_message(timeout_seconds: float) -> Optional[Dict[str, Any]]:
    """Replace with a bounded poll or receive call from the platform SDK."""
    await asyncio.sleep(timeout_seconds)
    return None


async def _deliver_response(message: Dict[str, Any], text: str, artifacts: List[Any]) -> None:
    """Replace with platform-specific text and artifact delivery."""
    logger.info(
        "[example_portal] response ready room=%s text_chars=%s artifacts=%s",
        _text(message.get("room_id")),
        len(text),
        len(artifacts),
    )


async def _handle_message(message: Dict[str, Any], llm_client: Any) -> None:
    text = _text(message.get("text"))
    user_id = _text(message.get("user_id"))
    room_id = _text(message.get("room_id"))
    if not text or not user_id or not room_id:
        return

    origin = {
        "platform": "example",
        "user_id": user_id,
        "user_handle": _text(message.get("user_handle")),
        "display_name": _text(message.get("display_name")) or user_id,
        "room_id": room_id,
        "thread_id": _text(message.get("thread_id")),
    }

    # Replace the empty list with a bounded history scoped to this room/thread.
    history: List[Dict[str, Any]] = []
    result = await run_hydra_turn(
        llm_client=llm_client,
        platform="example",
        history_messages=history,
        registry=verba_registry.get_verba_registry_snapshot(),
        enabled_predicate=_verba_enabled,
        context={"incoming": message},
        user_text=text,
        scope=f"room:{room_id}",
        origin=origin,
        redis_client=redis_client,
    )

    response_text = _text(result.get("text"))
    artifacts = list(result.get("artifacts") or [])
    await _deliver_response(message, response_text, artifacts)


async def _run_async(stop_event=None) -> None:
    settings = _settings()
    if not _text(settings.get("api_token")):
        logger.warning("[example_portal] API token is not configured")
        return

    llm_client = get_llm_client_from_env()
    logger.info("[example_portal] started v%s", __version__)
    try:
        while not (stop_event and stop_event.is_set()):
            settings = _settings()
            message = await _receive_next_message(_poll_interval(settings))
            if message is None:
                continue
            try:
                await _handle_message(message, llm_client)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[example_portal] message handling failed")
    finally:
        logger.info("[example_portal] stopped")


def run(stop_event=None) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run_async(stop_event))
    finally:
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
