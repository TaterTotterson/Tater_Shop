# Build a Portal

A Portal is a transport adapter. It receives messages or events from one platform, normalizes identity and conversation context, calls Hydra, and delivers text and artifacts back to that platform.

Use a Verba for domain actions and a Core for long-running business services. A Portal should concentrate on transport, authentication, platform formatting, message history, streaming, and delivery.

Start with [`templates/portal/portals/example_portal.py`](../templates/portal/portals/example_portal.py).

## File and identity

A Portal is installed as one file named `<id>_portal.py`:

```text
portals/example_portal.py
```

Use a distinctive lowercase `snake_case` ID. The filename produces module key `example_portal` and Store ID `example`.

```python
__version__ = "1.0.0"
MIN_TATER_VERSION = "187"
PORTAL_DESCRIPTION = "Connect Example Chat to Tater."
TAGS = ["chat", "example"]
```

`__version__` is the semantic extension version used for updates. `MIN_TATER_VERSION` is currently advisory metadata and uses a compatible Tater build identifier.

## Settings contract

Declare settings with `PORTAL_SETTINGS`:

```python
PORTAL_SETTINGS = {
    "category": "Example Portal Settings",
    "tags": TAGS,
    "required": {
        "api_token": {
            "label": "API token",
            "type": "password",
            "default": "",
            "description": "Token used to connect to Example Chat.",
        },
        "poll_interval_seconds": {
            "label": "Poll interval",
            "type": "number",
            "default": 5,
            "description": "Seconds between message checks.",
        },
    },
}
```

Tater stores values in `<module_key>_settings`, such as `example_portal_settings`. Values read from Redis may be strings; normalize them before use. Do not hardcode credentials or log secrets.

Optional module-level hooks can enrich settings fields or normalize submitted values:

```python
def webui_settings_fields(
    *,
    fields,
    current_settings=None,
    redis_client=None,
    notifier_destination_catalog=None,
    **_kwargs,
):
    return fields


def webui_prepare_settings_values(*, values, redis_client=None, **_kwargs):
    return dict(values or {})
```

Use these hooks for discovered channels or destinations, dependent options, and canonical storage. The settings-field hook has a short host timeout and should return quickly when its provider is offline.

## Runtime lifecycle

Tater starts a Portal in a managed background thread by calling synchronous `run(stop_event=...)`. The function must remain alive while the Portal is running and exit promptly when the event is set.

For a synchronous client:

```python
def run(stop_event=None) -> None:
    client = connect()
    try:
        while not (stop_event and stop_event.is_set()):
            poll_once(client)
            if stop_event and stop_event.wait(1):
                break
    finally:
        client.close()
```

For an asynchronous SDK, create an event loop inside `run()`. `await` cannot be used directly in the synchronous function:

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

Use bounded network timeouts, reconnect with backoff, and continue checking `stop_event`. Clean up clients, tasks, and file handles in `finally`.

## Calling Hydra

For every incoming message:

1. Validate that the sender and room are allowed.
2. Normalize the sender, room, and thread into an `origin` dictionary.
3. Load a bounded platform conversation history.
4. Take a current Verba registry snapshot.
5. Call `run_hydra_turn(...)` from the Portal's event loop.
6. Deliver returned text and artifacts using platform-native formatting.
7. Save only the bounded history needed for future context.

A minimal call looks like:

```python
result = await run_hydra_turn(
    llm_client=llm_client,
    platform="example",
    history_messages=history,
    registry=verba_registry.get_verba_registry_snapshot(),
    enabled_predicate=is_verba_enabled,
    context={"incoming": raw_message},
    user_text=text,
    scope=f"room:{room_id}",
    origin=origin,
    redis_client=redis_client,
)

response_text = str(result.get("text") or "").strip()
artifacts = list(result.get("artifacts") or [])
```

The starter template includes a correct event-loop boundary and a small Hydra helper. Replace its placeholder receive and delivery functions with the platform SDK.

## Origin and identity

Origin data lets Tater preserve identity, scope actions correctly, and enforce administrative restrictions. Include stable values provided by the platform:

```python
origin = {
    "platform": "example",
    "user_id": "stable-platform-user-id",
    "user_handle": "optional-handle",
    "display_name": "Visible Name",
    "room_id": "stable-room-or-channel-id",
    "thread_id": "optional-thread-id",
}
```

Do not treat a display name as a stable identity. Do not mark users as administrators based only on text they control. Use Tater's shared People/admin helpers when the Portal supports privileged tools.

Build `scope` from the conversation boundary that should share history—normally a room, direct-message chat, or thread. Never mix histories from unrelated users or rooms.

## History, streaming, and delivery

- Bound history by message count or size. Do not send an entire platform history to Hydra.
- Preserve user and final assistant text; omit transient typing and waiting messages.
- Use `wait_callback` for a platform-specific working indicator when helpful.
- Use `response_callback` only when the platform can safely edit or stream a response, and always send a final response if streaming fails.
- Respect each platform's message and attachment limits.
- Deliver every returned artifact using an appropriate platform representation. If a file is too large, compress or transcode it when safe, otherwise provide a clear failure or supported link.
- Escape or convert Markdown for the destination rather than assuming every platform renders it the same way.
- Apply rate limits and retry delays without blocking Portal shutdown.

## Notifications

If the Portal also consumes Tater's notification queue, use a Portal-specific queue and acknowledge an item only after successful delivery. Honor expiry metadata, avoid duplicate sends, and keep destination lookup data bounded.

Portals should not invent domain behavior for notification content. They translate the common notification payload into the platform's message and media APIs.

## Dependencies and packaging

Tater installs one Portal Python file. It does not install Portal-specific requirements, companion modules, or assets. Use the standard library and dependencies already shipped by supported Tater installations. Guard optional imports and log a clear startup error when a required SDK is unavailable.

Do not perform network discovery during import. Tater imports Portals to build the settings and runtime registry even when they are stopped.

## Test and publish

Before publishing:

```bash
python3 -m py_compile portals/example_portal.py
python3 tools/generate_portal_manifest.py
python3 tools/validate_package.py --root . --kind portal
```

Test at least:

- Missing and invalid credentials.
- Start, stop, restart, and reconnect behavior.
- Direct messages and shared rooms.
- Identity and admin restrictions.
- Bounded history and separate scopes.
- Text, errors, and each supported artifact type.
- Provider outages and rate limiting.
- Settings discovery while the provider is offline.

See [Repository manifests and publishing](repository-manifests.md) for the publishing flow.
