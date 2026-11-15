import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TrustedRepositoryListsTests(unittest.TestCase):
    def test_all_directories_are_valid_and_intentionally_empty(self):
        for kind in ("core", "verba", "portal"):
            with self.subTest(kind=kind):
                payload = json.loads((ROOT / f"{kind}_repositories.json").read_text())
                self.assertEqual(payload["schema"], 1)
                self.assertEqual(payload["kind"], kind)
                self.assertEqual(payload["repositories"], [])


if __name__ == "__main__":
    unittest.main()
