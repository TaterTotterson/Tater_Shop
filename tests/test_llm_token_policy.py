import ast
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _parse(relative_path: str) -> ast.Module:
    return ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))


def _function(tree: ast.Module, name: str) -> ast.AST:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"Function {name!r} was not found")


def _call_name(call: ast.Call) -> str:
    target = call.func
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return ""


def _max_tokens_values(function: ast.AST, call_name: str):
    values = []
    for node in ast.walk(function):
        if not isinstance(node, ast.Call) or _call_name(node) != call_name:
            continue
        for keyword in node.keywords:
            if keyword.arg == "max_tokens":
                values.append(keyword.value)
    return values


def _assert_context_bounded_call(
    test: unittest.TestCase,
    tree: ast.Module,
    function_name: str,
    call_name: str,
) -> None:
    values = _max_tokens_values(_function(tree, function_name), call_name)
    test.assertTrue(values, f"{function_name} must explicitly set max_tokens")
    test.assertTrue(
        all(isinstance(value, ast.Constant) and value.value is None for value in values),
        f"{function_name} must use context-bounded max_tokens=None",
    )


class CoreTokenPolicyTests(unittest.TestCase):
    def test_large_structured_and_long_form_calls_are_context_bounded(self):
        guardian = _parse("cores/guardian_core.py")
        self.assertFalse(
            any(
                isinstance(node, (ast.Assign, ast.AnnAssign))
                and any(
                    isinstance(target, ast.Name) and target.id == "MAX_AI_OUTPUT_TOKENS"
                    for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
                )
                for node in guardian.body
            )
        )
        _assert_context_bounded_call(
            self,
            guardian,
            "_guardian_ai_repair_json",
            "_guardian_ai_chat_json",
        )
        _assert_context_bounded_call(
            self,
            guardian,
            "_guardian_ai_analyze_async",
            "_guardian_ai_chat_json",
        )

        memory = _parse("cores/memory_core.py")
        _assert_context_bounded_call(self, memory, "_llm_extract_observations", "chat")
        memory_settings = next(
            node.value
            for node in memory.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "CORE_SETTINGS" for target in node.targets)
        )
        self.assertNotIn("extraction_max_tokens", ast.literal_eval(memory_settings))

        awareness = _parse("cores/awareness_core.py")
        _assert_context_bounded_call(
            self,
            awareness,
            "_events_query_interpret_query",
            "_events_query_llm_json_object",
        )
        _assert_context_bounded_call(
            self,
            awareness,
            "_events_query_select_relevant_event_ids",
            "_events_query_llm_json_object",
        )
        _assert_context_bounded_call(
            self,
            awareness,
            "_events_query_compose_final_answer",
            "chat",
        )

        _assert_context_bounded_call(
            self,
            _parse("cores/tater_tube_core.py"),
            "_llm_json",
            "chat",
        )

        personal = _parse("cores/personal_core.py")
        _assert_context_bounded_call(self, personal, "_llm_extract_updates", "chat")
        _assert_context_bounded_call(
            self,
            personal,
            "_tool_personal_email_summarize_async",
            "chat",
        )

        _assert_context_bounded_call(
            self,
            _parse("cores/rss_core.py"),
            "process_entry",
            "chat",
        )

    def test_edited_core_versions_and_manifest_match(self):
        expected_versions = {
            "awareness": "3.4.11",
            "guardian": "1.3.11",
            "memory": "1.0.28",
            "personal": "1.0.54",
            "rss": "1.0.10",
            "tater_tube": "1.2.2",
        }
        manifest = json.loads((ROOT / "core_manifest.json").read_text(encoding="utf-8"))
        entries = {entry["id"]: entry for entry in manifest["cores"]}

        for core_id, expected_version in expected_versions.items():
            tree = _parse(f"cores/{core_id}_core.py")
            version = next(
                ast.literal_eval(node.value)
                for node in tree.body
                if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets)
            )
            self.assertEqual(version, expected_version)
            self.assertEqual(entries[core_id]["version"], expected_version)

        self.assertEqual(entries["memory"]["required_settings_count"], 9)


if __name__ == "__main__":
    unittest.main()
