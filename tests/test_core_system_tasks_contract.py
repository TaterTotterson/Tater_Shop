import ast
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class CoreSystemTaskContractTests(unittest.TestCase):
    def test_expected_cores_expose_self_contained_task_providers(self) -> None:
        expected = {
            "memory_core.py": ("memory_extraction",),
            "guardian_core.py": ("network_inventory", "ai_security_analysis"),
            "personal_core.py": ("personal_scan",),
            "rss_core.py": ("feed_check",),
            "tater_tube_core.py": ("viewing_context_sync", "recommendation_refresh"),
            "music_core.py": (
                "catalog_sync",
                "recommendation_refresh",
                "continuous_radio_refill",
            ),
        }
        for filename, task_ids in expected.items():
            source = (REPO_ROOT / "cores" / filename).read_text(encoding="utf-8")
            tree = ast.parse(source, filename=filename)
            function_names = {
                node.name
                for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            self.assertIn("get_core_system_tasks", function_names, filename)
            self.assertIn("run_core_system_task", function_names, filename)
            for task_id in task_ids:
                self.assertIn(f'"{task_id}"', source, filename)


if __name__ == "__main__":
    unittest.main()
