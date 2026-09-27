from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "tools" / "validate_package.py"


class ExtensionAuthoringToolsTests(unittest.TestCase):
    def _run(self, *args: object) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, *(str(arg) for arg in args)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_all_starter_templates_validate(self) -> None:
        for kind in ("verba", "core", "portal"):
            with self.subTest(kind=kind):
                result = self._run(VALIDATOR, "--root", ROOT / "templates" / kind)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_checksum_mismatch_is_actionable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir) / "verba-template"
            shutil.copytree(ROOT / "templates" / "verba", repo)
            source = repo / "verba" / "example_lookup.py"
            source.write_text(source.read_text(encoding="utf-8") + "\n# changed\n", encoding="utf-8")

            result = self._run(VALIDATOR, "--root", repo)

            self.assertEqual(result.returncode, 1)
            self.assertIn("sha256 does not match", result.stdout)
            self.assertIn("regenerate the manifest", result.stdout)

    def test_generators_support_an_external_repository_root(self) -> None:
        cases = (
            ("verba", "generate_manifest.py", "manifest.json"),
            ("core", "generate_core_manifest.py", "core_manifest.json"),
            ("portal", "generate_portal_manifest.py", "portal_manifest.json"),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            for kind, generator_name, manifest_name in cases:
                with self.subTest(kind=kind):
                    repo = Path(temp_dir) / kind
                    shutil.copytree(ROOT / "templates" / kind, repo)
                    (repo / manifest_name).unlink()

                    generated = self._run(ROOT / "tools" / generator_name, "--root", repo)
                    self.assertEqual(generated.returncode, 0, generated.stdout + generated.stderr)
                    self.assertTrue((repo / manifest_name).is_file())

                    validated = self._run(VALIDATOR, "--root", repo)
                    self.assertEqual(validated.returncode, 0, validated.stdout + validated.stderr)


if __name__ == "__main__":
    unittest.main()
