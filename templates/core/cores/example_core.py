"""Working background Core and Vue settings-manager starter for Tater."""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from helpers import redis_client

__version__ = "1.0.0"
MIN_TATER_VERSION = "187"
CORE_DESCRIPTION = "Maintain a small set of example items and expose their status to Hydra."
TAGS = ["example"]

logger = logging.getLogger("example_core")

SETTINGS_KEY = "example_core_settings"
ITEMS_KEY = "example_core:items"
STATUS_KEY = "example_core:status"

CORE_SETTINGS = {
    "category": "Example Core Settings",
    "hydra_tools_require_running": False,
    "required": {
        "poll_interval_seconds": {
            "label": "Poll interval",
            "type": "number",
            "default": 30,
            "description": "Seconds between background status updates.",
        },
    },
    "tags": TAGS,
}

CORE_WEBUI_TAB = {
    "label": "Example",
    "order": 50,
    "requires_running": False,
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _settings(client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    return dict(store.hgetall(SETTINGS_KEY) or {})


def _poll_interval(client: Any = None) -> float:
    try:
        return max(5.0, float(_settings(client).get("poll_interval_seconds") or 30))
    except Exception:
        return 30.0


def _load_items(client: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item_id, raw in (client.hgetall(ITEMS_KEY) or {}).items():
        try:
            parsed = json.loads(str(raw or "{}"))
        except Exception:
            parsed = {}
        if not isinstance(parsed, dict):
            parsed = {}
        parsed["id"] = _text(item_id)
        rows.append(parsed)
    rows.sort(key=lambda row: _text(row.get("name") or row.get("id")).lower())
    return rows


def _save_item(client: Any, item_id: str, values: Dict[str, Any]) -> Dict[str, Any]:
    normalized_id = _text(item_id)
    if not normalized_id:
        raise ValueError("Item id is required.")
    name = _text(values.get("name"))
    if not name:
        raise ValueError("Name is required.")
    row = {
        "name": name,
        "enabled": bool(values.get("enabled", True)),
        "updated_at": time.time(),
    }
    client.hset(ITEMS_KEY, normalized_id, json.dumps(row, separators=(",", ":")))
    return {"id": normalized_id, **row}


def run(stop_event=None) -> None:
    logger.info("[example_core] started v%s", __version__)
    try:
        while not (stop_event and stop_event.is_set()):
            redis_client.hset(
                STATUS_KEY,
                mapping={"state": "ready", "last_seen": str(time.time())},
            )
            interval = _poll_interval()
            if stop_event:
                if stop_event.wait(interval):
                    break
            else:
                time.sleep(interval)
    finally:
        logger.info("[example_core] stopped")


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    items = _load_items(client)
    status = dict(client.hgetall(STATUS_KEY) or {})

    item_forms = []
    for item in items:
        item_id = _text(item.get("id"))
        item_forms.append(
            {
                "id": item_id,
                "title": _text(item.get("name")) or item_id,
                "subtitle": "Enabled" if bool(item.get("enabled", True)) else "Paused",
                "settings_label": "Manage",
                "settings_title": f"Manage {_text(item.get('name')) or item_id}",
                "save_action": "example_save",
                "save_label": "Save",
                "save_success_text": "Example item saved.",
                "remove_action": "example_delete",
                "remove_label": "Delete",
                "remove_confirm": "Delete this example item?",
                "fields": [
                    {
                        "key": "name",
                        "label": "Name",
                        "type": "text",
                        "value": _text(item.get("name")),
                        "required": True,
                    },
                    {
                        "key": "enabled",
                        "label": "Enabled",
                        "type": "checkbox",
                        "value": bool(item.get("enabled", True)),
                        "description": "Allow this example item to be used.",
                    },
                ],
            }
        )

    return {
        "summary": "Create and manage example items.",
        "stats": [
            {"label": "Items", "value": len(items)},
            {"label": "Runtime", "value": _text(status.get("state")) or "stopped"},
        ],
        "empty_message": "No example items have been created yet.",
        "ui": {
            "kind": "settings_manager",
            "title": "Example items",
            "item_fields_popup": True,
            "item_fields_popup_label": "Manage",
            "manager_tabs": [
                {"key": "items", "label": "Items", "source": "items"},
                {"key": "add", "label": "Add item", "source": "add_form"},
            ],
            "default_tab": "items",
            "add_form": {
                "action": "example_create",
                "submit_label": "Add item",
                "success_text": "Example item added.",
                "fields": [
                    {
                        "key": "name",
                        "label": "Name",
                        "type": "text",
                        "required": True,
                        "placeholder": "Kitchen display",
                    },
                    {
                        "key": "enabled",
                        "label": "Enabled",
                        "type": "checkbox",
                        "default": True,
                    },
                ],
            },
            "item_forms": item_forms,
        },
    }


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    client = redis_client or globals().get("redis_client")
    body = payload if isinstance(payload, dict) else {}
    values = body.get("values") if isinstance(body.get("values"), dict) else body

    if action == "example_create":
        item_id = f"item_{uuid.uuid4().hex[:10]}"
        item = _save_item(client, item_id, values)
        return {"ok": True, "message": f"Added {item['name']}."}

    if action == "example_save":
        item_id = _text(body.get("id"))
        if not client.hexists(ITEMS_KEY, item_id):
            raise KeyError("Example item not found.")
        item = _save_item(client, item_id, values)
        return {"ok": True, "message": f"Saved {item['name']}."}

    if action == "example_delete":
        item_id = _text(body.get("id"))
        if not item_id or not client.hexists(ITEMS_KEY, item_id):
            raise KeyError("Example item not found.")
        client.hdel(ITEMS_KEY, item_id)
        return {"ok": True, "message": "Example item deleted."}

    raise KeyError(f"Unsupported Example Core UI action: {action}")


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    return [
        {
            "id": "example_status",
            "description": "Read Example Core runtime status and item count.",
            "usage": '{"function":"example_status","arguments":{}}',
        }
    ]


async def run_hydra_kernel_tool(
    *,
    tool_id: str,
    args: Optional[Dict[str, Any]] = None,
    platform: str = "",
    scope: str = "",
    origin: Optional[Dict[str, Any]] = None,
    llm_client: Any = None,
    redis_client: Any = None,
    **_kwargs,
) -> Optional[Dict[str, Any]]:
    if tool_id != "example_status":
        return None
    client = redis_client or globals().get("redis_client")
    status = dict(client.hgetall(STATUS_KEY) or {})
    items = _load_items(client)
    return {
        "tool": "example_status",
        "ok": True,
        "status": _text(status.get("state")) or "stopped",
        "item_count": len(items),
        "summary_for_user": f"Example Core has {len(items)} configured item(s).",
    }
