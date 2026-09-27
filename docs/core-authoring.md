# Build a Core

A Core is a long-running service owned by Tater. It can maintain state, expose Core-owned Hydra tools and prompt context, and provide a custom management tab rendered by Tater's Vue UI.

Start with [`templates/core/cores/example_core.py`](../templates/core/cores/example_core.py).

## File and identity

A Core is installed as one file named `<id>_core.py`:

```text
cores/example_core.py
```

Use a distinctive lowercase `snake_case` ID. The filename produces the module key `example_core`, while the Store ID is `example`.

```python
__version__ = "1.0.0"
MIN_TATER_VERSION = "187"
CORE_DESCRIPTION = "A concise explanation of the Core."
TAGS = ["example"]
```

`__version__` is the semantic extension version used to detect updates. `MIN_TATER_VERSION` is currently advisory metadata and refers to a compatible Tater build identifier, not the public release label.

## Settings contract

`CORE_SETTINGS` controls the standard settings card and Core behavior:

```python
CORE_SETTINGS = {
    "category": "Example Core Settings",
    "hydra_tools_require_running": False,
    "required": {
        "poll_interval_seconds": {
            "label": "Poll interval",
            "type": "number",
            "default": 30,
            "description": "Seconds between refreshes.",
        },
    },
    "tags": TAGS,
}
```

The standard settings hash is `<module_key>_settings`, or `example_core_settings` here. Values loaded directly from Redis may be strings; normalize booleans, numbers, lists, and dictionaries before using them.

## Runtime lifecycle

Tater starts a Core in a managed background thread by calling synchronous `run(stop_event=...)`. The function should stay alive until stopped, check the event frequently, and release resources in `finally`:

```python
def run(stop_event=None) -> None:
    logger.info("[example_core] started")
    try:
        while not (stop_event and stop_event.is_set()):
            refresh_once()
            if stop_event and stop_event.wait(30):
                break
    finally:
        logger.info("[example_core] stopped")
```

Do not start work during module import. A Core that uses asynchronous libraries must create and close its own event loop inside `run()`:

```python
def run(stop_event=None) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_async(stop_event))
    finally:
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
```

Tater gives a Core only a short time to stop during a restart. Use bounded network timeouts and `stop_event.wait(timeout)` instead of a long uninterruptible `sleep()`.

## Redis ownership

Store Core-owned runtime data below a host-recognized namespace:

```text
example_core:status
example_core:item:123
```

The shorter `example:*` namespace is also recognized when it does not collide with a reserved Tater namespace. Prefer the full module-key prefix for clarity. Tater's **Remove and delete data** action deletes the standard settings and cooldown keys plus these Core-owned namespaces. Arbitrary keys outside them will not be removed.

Never scan or delete broad shared prefixes. Shared data should be accessed through a stable Tater API rather than claimed by an extension.

## Hydra kernel tools

A Core may add tools directly to Hydra:

```python
def get_hydra_kernel_tools(*, platform: str = "", **_kwargs):
    return [
        {
            "id": "example_status",
            "description": "Read the current Example Core status.",
            "usage": '{"function":"example_status","arguments":{}}',
        }
    ]


async def run_hydra_kernel_tool(
    *,
    tool_id,
    args=None,
    platform="",
    scope="",
    origin=None,
    llm_client=None,
    redis_client=None,
    **_kwargs,
):
    if tool_id != "example_status":
        return None
    return {
        "tool": tool_id,
        "ok": True,
        "status": {"state": "ready"},
        "summary_for_user": "Example Core is ready.",
    }
```

Use Core kernel tools for Core-owned data and workflows. Keep them narrow, safe to retry, and independent of a specific Portal transport. Set `hydra_tools_require_running` to `False` only when those tools are genuinely useful while the background loop is stopped.

Optional Core context hooks include `get_hydra_system_prompt_fragments(...)` and Core-specific compact context providers. Keep injected context small, current, and directly useful to the model.

## Add a Core tab

Declare the tab:

```python
CORE_WEBUI_TAB = {
    "label": "Example",
    "order": 50,
    "requires_running": False,
}
```

Tater calls `get_htmlui_tab_data(redis_client=..., core_key=..., core_tab=...)`. Return plain JSON-compatible data. The host renders it; extensions do not provide HTML, JavaScript, or Vue components.

A simple read-only tab may return:

```python
return {
    "summary": "Example Core status.",
    "stats": [{"label": "State", "value": "Ready"}],
    "items": [
        {"title": "Example item", "subtitle": "Online", "detail": "Updated now"}
    ],
    "empty_message": "No example data yet.",
}
```

For forms and management screens, return `ui.kind = "settings_manager"`.

### Manager structure

Common top-level UI keys are:

| Key | Purpose |
| --- | --- |
| `title` | Manager heading. |
| `manager_tabs` | Tabs using `items`, `add_form`, or `grouped_items` sources. |
| `default_tab` | Initially selected manager tab. |
| `add_form` | Create form definition. |
| `item_forms` | Existing item cards and forms. |
| `persistent_item_groups` | Item groups displayed above the manager tabs. |
| `item_fields_popup` | Put item fields in a modal opened by a settings button. |
| `item_fields_popup_label` | Default popup button label. |
| `item_fields_dropdown` | Put item fields in an inline disclosure. |
| `item_sections_in_dropdown` | Put item sections in the disclosure. |
| `stats_controls` | Fields displayed with the top summary metrics. |
| `stats_controls_action` | Action used to save top controls. |
| `stats_controls_auto_save` | Save controls on change unless explicitly `False`. |
| `stats_refresh_button` | Show a manual refresh button. |

A popup-based item looks like this:

```python
{
    "id": "example",
    "title": "Example item",
    "subtitle": "Stored by Example Core",
    "settings_label": "Manage",
    "settings_title": "Manage Example Item",
    "save_action": "example_save",
    "save_label": "Save",
    "save_success_text": "Example item saved.",
    "fields": [
        {"key": "name", "label": "Name", "type": "text", "value": "Example"},
        {
            "key": "enabled",
            "label": "Enabled",
            "type": "checkbox",
            "value": True,
            "description": "Allow this item to run.",
        },
    ],
}
```

Set `ui.item_fields_popup = True` to show those fields in a modal. An item may opt out with `fields_popup = False`, or supply additional `popup_fields`. Unsaved touched values are preserved when the tab refreshes as long as the item ID and field keys remain stable.

### Fields

The current Vue renderer supports:

- Inputs: `text`, `password`, `number`, `textarea` / `multiline`, `checkbox`, `range`, `select`, `multiselect`, and `hidden`.
- Rich selectors: `choice_cards`, `multi_choice_cards`, and `image_checklist`.
- Read-only display: `readonly`, `heading`, `section_heading`, `table`, `bar_chart` / `bars`, `image`, and `video`.
- Uploads: `file`, optionally with Base64 encoding and camera capture.

Useful field properties include `label`, `description`, `value`, `default`, `placeholder`, `options`, `min`, `max`, `step`, `suffix`, `disabled`, `read_only`, `full_width`, and `presentation`.

Conditional fields use `show_when` or `show_when_all`:

```python
{
    "key": "endpoint",
    "label": "Endpoint",
    "type": "text",
    "show_when": {"source_key": "mode", "equals": "remote"},
}
```

`disable_when` / `disable_when_all` use the same shape. A select can use `dependent_options` with `source_key`, `options_by_source`, and `default_options`.

### Actions

Tater calls:

```python
handle_htmlui_tab_action(
    action="example_save",
    payload={"id": "example", "values": {"name": "New name"}},
    redis_client=redis_client,
    core_key="example_core",
)
```

Add-form payloads include both top-level form values and `values`. Item save, run, and custom actions receive the item `id` and its current `values`. Bulk actions receive `ids` and `values.identity_ids`. Pagination actions receive `page` and `page_size`.

Return a useful success message:

```python
return {"ok": True, "message": "Example item saved."}
```

Tater displays successful messages and errors in the Vue panel. Raise `ValueError` for invalid input and `KeyError` for an unsupported action or missing item. Validate every action and ID server-side; the UI payload is not a security boundary.

Item cards may also define `actions`, `run_action`, `reset_action`, `remove_action`, confirmation text, danger tones, selection, bulk actions, client pagination, or server pagination. See the starter Core for a complete create/save/delete flow.

### Media

For Core-owned images, audio, or video that should not be embedded in the tab payload, implement:

```python
def get_htmlui_tab_media(*, media_id, redis_client=None, **_kwargs):
    return {
        "bytes": load_media_bytes(media_id),
        "content_type": "video/mp4",
        "filename": f"example-{media_id}.mp4",
    }
```

The media is available through `/api/cores/<core_key>/media/<media_id>` with byte-range support. The provider may also return Base64 `content` with `encoding = "base64"`. Raise `KeyError` when the media does not exist.

## Dependencies and testing

Tater installs one Core Python file and does not install Core-specific requirements or assets. Use the standard library and stable APIs already shipped by Tater, and guard optional capabilities.

Before publishing:

```bash
python3 -m py_compile cores/example_core.py
python3 tools/generate_core_manifest.py
python3 tools/validate_package.py --root . --kind core
```

Test start, stop, restart, settings persistence, every UI action, Core removal with data cleanup, and every advertised Hydra tool. See [Repository manifests and publishing](repository-manifests.md) for the publishing flow.
