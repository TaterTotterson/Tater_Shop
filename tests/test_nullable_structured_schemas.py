import ast
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]


def _dict_value(node: ast.AST, key: str) -> ast.AST:
    assert isinstance(node, ast.Dict), f"expected dict while looking for {key!r}"
    for key_node, value_node in zip(node.keys, node.values):
        if isinstance(key_node, ast.Constant) and key_node.value == key:
            return value_node
    raise AssertionError(f"schema key {key!r} was not found")


def _schema_call(path: str, function_name: str) -> ast.AST:
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        keywords = {keyword.arg: keyword.value for keyword in node.keywords if keyword.arg}
        name_node = keywords.get("function_name")
        if not isinstance(name_node, ast.Constant) or name_node.value != function_name:
            continue
        schema = keywords.get("schema")
        assert schema is not None, f"{path} {function_name} has no schema"
        return schema
    raise AssertionError(f"{path} has no structured call named {function_name}")


def _schema_path(node: ast.AST, *path: str) -> ast.AST:
    current = node
    for key in path:
        current = _dict_value(current, key)
    return current


def _ast_allows_null(node: ast.AST) -> bool:
    if not isinstance(node, ast.Dict):
        return False
    try:
        type_node = _dict_value(node, "type")
    except AssertionError:
        type_node = None
    if isinstance(type_node, ast.Constant):
        if type_node.value == "null":
            return True
    elif isinstance(type_node, (ast.List, ast.Tuple)):
        if any(isinstance(item, ast.Constant) and item.value == "null" for item in type_node.elts):
            return True
    try:
        any_of = _dict_value(node, "anyOf")
    except AssertionError:
        return False
    return isinstance(any_of, (ast.List, ast.Tuple)) and any(_ast_allows_null(item) for item in any_of.elts)


def _runtime_schema_function(path: str, function_names: List[str]) -> Dict[str, Any]:
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    selected = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in set(function_names)
    ]
    namespace: Dict[str, Any] = {
        "Any": Any,
        "Dict": Dict,
        "List": List,
        "MAX_ROLES": 10,
        "MAX_CATEGORIES": 8,
        "MAX_CHANNELS_PER_CATEGORY": 12,
    }
    module = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    exec(compile(module, path, "exec"), namespace)
    return namespace


def _dict_allows_null(schema: Dict[str, Any]) -> bool:
    schema_type = schema.get("type")
    if schema_type == "null":
        return True
    if isinstance(schema_type, list) and "null" in schema_type:
        return True
    return any(
        isinstance(option, dict) and _dict_allows_null(option)
        for option in schema.get("anyOf") or []
    )


def test_action_dependent_structured_fields_accept_null() -> None:
    cases = [
        ("cores/ai_task_core.py", "submit_execution_task_prompt", ("properties", "reminder_text")),
        ("cores/ai_task_core.py", "submit_destination_hint", ("properties", "target_hint")),
        ("cores/personal_core.py", "submit_email_query_plan", ("properties", "query_terms")),
        ("cores/personal_core.py", "submit_email_query_plan", ("properties", "days")),
        ("cores/personal_core.py", "submit_email_query_plan", ("properties", "date_from")),
        ("cores/music_core.py", "submit_curated_playlists", ("properties", "summary")),
        ("cores/music_core.py", "submit_curated_playlists", ("properties", "playlists", "items", "properties", "description")),
        ("cores/music_core.py", "submit_curated_playlists", ("properties", "playlists", "items", "properties", "items", "items", "properties", "reason")),
        ("cores/tater_tube_core.py", "submit_tater_picks", ("properties", "summary")),
        ("cores/tater_tube_core.py", "submit_tater_picks", ("properties", "picks_briefing")),
        ("cores/tater_tube_core.py", "submit_tater_picks", ("properties", "items", "items", "properties", "reason")),
        ("verba/roon_music.py", "submit_roon_music_plan", ("properties", "query")),
        ("verba/roon_music.py", "submit_roon_music_plan", ("properties", "media_kind")),
        ("verba/roon_music.py", "submit_roon_music_plan", ("properties", "random")),
        ("verba/microwakeword.py", "submit_wake_word_intent", ("properties", "wake_word")),
        ("verba/moltbook_account_summary.py", "submit_moltbook_route", ("properties", "target_name")),
        ("verba/moltbook_home_overview.py", "submit_moltbook_route", ("properties", "target_name")),
        ("verba/unifi_network_health.py", "submit_unifi_network_intent", ("properties", "target")),
        ("verba/unifi_network_health.py", "submit_unifi_network_intent", ("properties", "target_kind")),
        ("verba/unifi_network_lookup.py", "submit_unifi_network_intent", ("properties", "target")),
        ("verba/unifi_protect_area.py", "submit_unifi_protect_action", ("properties", "target")),
        ("verba/ha_camera_area.py", "extract_camera_area", ("properties", "area")),
        ("verba/overseerr_details.py", "extract_overseerr_title", ("properties", "title")),
        ("verba/overseerr_details.py", "extract_overseerr_title", ("properties", "media_type")),
        ("verba/overseerr_request.py", "extract_overseerr_request", ("properties", "title")),
        ("verba/overseerr_request.py", "extract_overseerr_request", ("properties", "kind")),
        ("tools/device_verba_runtime.py", "submit_device_intent", ("properties", "target")),
        ("tools/device_verba_runtime.py", "submit_device_intent", ("properties", "requested_property")),
        ("tools/device_verba_runtime.py", "select_device", ("properties", "device_id")),
    ]
    for path, function_name, field_path in cases:
        field_schema = _schema_path(_schema_call(path, function_name), *field_path)
        assert _ast_allows_null(field_schema), f"{path} {function_name} {field_path} rejects null"


def test_personal_email_insight_optional_values_accept_null() -> None:
    namespace = _runtime_schema_function("cores/personal_core.py", ["_email_insight_schema"])
    schema = namespace["_email_insight_schema"]()
    for category in (
        "spending_habits",
        "favorite_places",
        "important_notes",
        "upcoming_events",
        "subscriptions",
        "deliveries",
        "action_items",
    ):
        assert _dict_allows_null(schema["properties"][category])

    events = schema["properties"]["upcoming_events"]["anyOf"][0]["items"]["properties"]
    assert _dict_allows_null(events["ends_at"])
    assert _dict_allows_null(events["location"])
    assert not _dict_allows_null(events["title"])

    deliveries = schema["properties"]["deliveries"]["anyOf"][0]["items"]["properties"]
    for field in ("carrier", "tracking_id", "order_number", "item_description", "eta_at", "merchant", "summary"):
        assert _dict_allows_null(deliveries[field])
    assert not _dict_allows_null(deliveries["status"])


def test_discord_optional_plan_text_accepts_null() -> None:
    helpers = [
        "_discord_role_detail_schema",
        "_discord_rename_schema",
        "_discord_plan_schema",
        "_discord_outline_schema",
    ]
    for path in (
        "verba/discord_admin_response_mode.py",
        "verba/discord_admin_roles.py",
        "verba/discord_admin_setup.py",
    ):
        namespace = _runtime_schema_function(path, helpers)
        for factory in ("_discord_plan_schema", "_discord_outline_schema"):
            properties = namespace[factory]()["properties"]
            assert _dict_allows_null(properties["server_name"])
            assert _dict_allows_null(properties["notes"])
