# Build a Verba

A Verba is one focused tool that Hydra may select to answer a request or perform an action. Use a Verba when the behavior is request-driven. Use a Core instead when work must continue in the background, maintain long-lived state, or own a management screen.

Start with [`templates/verba/verba/example_lookup.py`](../templates/verba/verba/example_lookup.py).

## File and identity

Place each Verba in one file:

```text
verba/example_lookup.py
```

The module must create one module-level `verba` object. Its `name` is the stable package and tool ID:

```python
class ExampleLookupVerba(ToolVerba):
    name = "example_lookup"
    verba_name = "Example Lookup"
    version = "1.0.0"


verba = ExampleLookupVerba()
```

Choose a distinctive lowercase `snake_case` ID and do not rename it after release. Tater installs the file as `<id>.py`, and the first configured repository containing an ID wins.

## Required and recommended metadata

| Attribute | Purpose |
| --- | --- |
| `name` | Stable tool and package ID. |
| `verba_name` or `pretty_name` | User-facing name. |
| `version` | Semantic extension version used for updates. |
| `min_tater_version` | Advisory minimum compatible Tater build identifier. |
| `description` / `verba_dec` | Concise Store and routing description. |
| `platforms` | Surfaces with implemented handlers. |
| `usage` | One JSON tool-call example. |
| `when_to_use` | Plain-language routing guidance. |
| `how_to_use` | Plain-language argument guidance. |
| `argument_schema` | Preferred JSON-schema-like argument description. |
| `example_calls` | A few valid JSON tool calls. |
| `common_needs` | Information normally needed to execute. |
| `missing_info_prompts` | Helpful questions for missing information. |
| `settings_category` | Shared settings bucket displayed in Tater. |
| `required_settings` | Settings field definitions. |
| `tags` | Search and catalog labels. |

Keep the tool payload small. For flexible requests, prefer a single natural-language field such as `query` or `request`, then interpret it inside the Verba:

```python
argument_schema = {
    "type": "object",
    "properties": {
        "query": {
            "type": "string",
            "description": "The user's complete lookup request.",
        }
    },
    "required": ["query"],
}
```

Use deterministic parsing first. If the request remains ambiguous, an internal `llm_client` call may select from a small, validated set of actions. Never execute an unvalidated action name returned by a model.

## Handlers

Advertise only platforms for which the Verba implements a handler. Keep one private `_handle()` method when possible and normalize platform-specific inputs into it.

Common handler shapes are:

```python
async def handle_webui(self, args, llm_client): ...
async def handle_macos(self, args, llm_client, context=None): ...
async def handle_voice_core(self, args=None, llm_client=None, context=None, **kwargs): ...
async def handle_discord(self, message, args, llm_client): ...
async def handle_telegram(self, update, args, llm_client): ...
async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs): ...
async def handle_irc(self, bot, channel, user, raw_message, args, llm_client): ...
async def handle_meshtastic(self, args=None, llm_client=None, context=None, **kwargs): ...
```

Platform objects are transport details. Business logic should work from normalized arguments and return the same structured result on every surface.

## Results and failures

Use Tater's result helpers:

```python
from verba_result import action_failure, action_success

return action_success(
    facts={"status": "online"},
    summary_for_user="The example service is online.",
    say_hint="Briefly report that the service is online.",
)
```

For missing information, invalid settings, unavailable services, and rejected actions, return `action_failure(...)` rather than guessing or returning an unstructured exception:

```python
return action_failure(
    code="missing_query",
    message="No lookup request was provided.",
    needs=["Provide what should be looked up."],
    say_hint="Ask what the user wants to look up.",
)
```

Make actions safe to retry where possible. Put useful machine-readable values in `facts` and keep `summary_for_user` concise.

## Settings

Declare settings in `required_settings` and read them from the category through Tater's existing settings helpers. Typical field types are `text`, `password`, `number`, `checkbox`, `select`, and `multiselect`.

```python
settings_category = "Example Lookup"
required_settings = {
    "EXAMPLE_API_KEY": {
        "label": "API key",
        "type": "password",
        "default": "",
        "description": "Credential used only for Example Lookup requests.",
    },
    "EXAMPLE_TIMEOUT_SECONDS": {
        "label": "Timeout",
        "type": "number",
        "default": 10,
        "description": "Maximum request time in seconds.",
    },
}
```

Optional instance hooks can enrich fields or normalize values without changing the host UI:

```python
def webui_settings_fields(
    self,
    fields,
    current_settings=None,
    redis_client=None,
    notifier_destination_catalog=None,
):
    return fields

def webui_prepare_settings_values(self, values=None, redis_client=None):
    return dict(values or {})
```

Do not perform slow remote discovery during module import. A settings-field hook should also remain quick and tolerate its provider being offline.

## Dependencies and integrations

Tater installs one `.py` file for each Verba. It does not install a Verba-specific dependency list or companion module. Use:

- Python's standard library.
- Stable modules supplied by Tater, such as `verba_base`, `verba_result`, and `integration_registry`.
- Libraries already included in supported Tater installations.

Guard optional imports and return a helpful failure if a capability is unavailable. Prefer Tater's shared integration registry for devices and provider connections instead of opening a second connection with separately stored credentials.

## Test and publish

From the repository root:

```bash
python3 -m py_compile verba/example_lookup.py
python3 tools/generate_manifest.py
python3 tools/validate_package.py --root . --kind verba
```

Then inspect the manifest diff. Confirm the expected `id`, `name`, `version`, `entry`, and `sha256`, commit both the source file and generated manifest, and test the Verba on every advertised platform.

See [Repository manifests and publishing](repository-manifests.md) for third-party repositories.
