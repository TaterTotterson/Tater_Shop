from __future__ import annotations

import ast
import unittest
from pathlib import Path
from typing import Any, List


ROOT = Path(__file__).resolve().parents[1]
PORTAL_PATH = ROOT / "portals" / "xbmc_portal.py"


def load_installed_game_helpers():
    source = PORTAL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(PORTAL_PATH))
    wanted_functions = {
        "_clean_installed_game_name",
        "_installed_game_names",
        "_installed_games_context",
    }
    selected = []
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            names = []
            if isinstance(node, ast.Assign):
                names = [target.id for target in node.targets if isinstance(target, ast.Name)]
            elif isinstance(node.target, ast.Name):
                names = [node.target.id]
            if "INSTALLED_GAME_CONTEXT_MAX_ITEMS" in names:
                selected.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted_functions:
            selected.append(node)

    namespace = {"Any": Any, "List": List}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(PORTAL_PATH), "exec"), namespace)
    return namespace


class XBMCInstalledGamesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.helpers = load_installed_game_helpers()

    def test_names_are_cleaned_deduplicated_and_ordered(self) -> None:
        names = self.helpers["_installed_game_names"](
            [
                {"name": " Halo 2\n"},
                {"name": "halo 2"},
                "Jet   Set Radio Future",
                {"name": ""},
                None,
            ]
        )

        self.assertEqual(names, ["Halo 2", "Jet Set Radio Future"])

    def test_context_guides_recommendations_to_exact_installed_titles(self) -> None:
        context = self.helpers["_installed_games_context"](
            [{"name": "Halo 2"}, {"name": "Fable"}]
        )

        self.assertIn("- Halo 2", context)
        self.assertIn("- Fable", context)
        self.assertIn("use the exact title", context)
        self.assertIn("ask whether to launch it", context)

    def test_empty_or_invalid_payload_adds_no_context(self) -> None:
        build_context = self.helpers["_installed_games_context"]

        self.assertEqual(build_context(None), "")
        self.assertEqual(build_context({"name": "Halo 2"}), "")
        self.assertEqual(build_context([]), "")


if __name__ == "__main__":
    unittest.main()
