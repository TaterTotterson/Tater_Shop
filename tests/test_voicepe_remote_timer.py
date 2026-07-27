from __future__ import annotations

import ast
import re
import types
import unittest
from pathlib import Path
from typing import Any, Dict


def _load_timer_parser():
    path = Path(__file__).resolve().parents[1] / "verba" / "voicepe_remote_timer.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    plugin = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "VoicePERemoteTimerPlugin"
    )
    names = {
        "_parse_amount_phrase",
        "_duration_components_from_text",
        "_merged_request_text",
        "_parse_duration_to_seconds",
        "_clean_timer_name",
        "_extract_timer_name",
    }
    selected = [
        node
        for node in plugin.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names
    ]
    if len(selected) != len(names):
        found = {node.name for node in selected}
        raise RuntimeError(f"Missing timer helpers: {sorted(names - found)}")

    parser_class = ast.ClassDef(
        name="TimerParser",
        bases=[],
        keywords=[],
        body=selected,
        decorator_list=[],
    )
    module_tree = ast.Module(
        body=[
            ast.ImportFrom(
                module="__future__",
                names=[ast.alias(name="annotations")],
                level=0,
            ),
            parser_class,
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(module_tree)
    module = types.ModuleType("test_timer_parser")
    module.__dict__.update({"re": re, "Any": Any, "Dict": Dict})
    exec(compile(module_tree, str(path), "exec"), module.__dict__)
    return module.TimerParser()


class TimerParserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.parser = _load_timer_parser()

    def test_extracts_named_timer(self) -> None:
        self.assertEqual(
            self.parser._extract_timer_name({"query": "set a pasta timer for 10 minutes"}, "start"),
            "pasta",
        )
        self.assertEqual(
            self.parser._extract_timer_name({"query": "cancel the pasta timer"}, "cancel"),
            "pasta",
        )

    def test_duration_only_timer_does_not_become_a_name(self) -> None:
        self.assertEqual(
            self.parser._extract_timer_name({"query": "cancel the 10 minute timer"}, "cancel"),
            "",
        )

    def test_decimal_duration_is_not_split(self) -> None:
        self.assertEqual(self.parser._parse_duration_to_seconds("1.5 hours", 100_000), 5_400)
        self.assertEqual(self.parser._parse_duration_to_seconds("2.5 minutes", 100_000), 150)


if __name__ == "__main__":
    unittest.main()
