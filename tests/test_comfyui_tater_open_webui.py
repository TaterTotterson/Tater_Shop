from __future__ import annotations

import ast
import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGES = {
    "comfyui_audio_ace": "1.0.28",
    "comfyui_image_edit": "1.0.3",
    "comfyui_image_plugin": "1.0.8",
    "comfyui_image_video": "1.0.8",
    "comfyui_music_video": "1.0.8",
    "comfyui_video_plugin": "1.0.7",
    "lowfi_video": "1.1.8",
}


def _class_assignment(class_node: ast.ClassDef, name: str):
    for node in class_node.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{class_node.name} does not define {name}")


def _verba_class(tree: ast.Module, package_id: str) -> ast.ClassDef:
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        try:
            if _class_assignment(node, "name") == package_id:
                return node
        except AssertionError:
            continue
    raise AssertionError(f"No Verba class found for {package_id}")


class ComfyUITaterOpenWebUITests(unittest.TestCase):
    def test_comfyui_verbas_delegate_tater_open_webui_to_webui(self):
        for package_id, expected_version in PACKAGES.items():
            with self.subTest(package_id=package_id):
                path = ROOT / "verba" / f"{package_id}.py"
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                class_node = _verba_class(tree, package_id)

                self.assertEqual(_class_assignment(class_node, "version"), expected_version)
                self.assertIn("tater_open_webui", _class_assignment(class_node, "platforms"))

                handler = next(
                    (
                        node
                        for node in class_node.body
                        if isinstance(node, ast.AsyncFunctionDef)
                        and node.name == "handle_tater_open_webui"
                    ),
                    None,
                )
                self.assertIsNotNone(handler)
                delegates = any(
                    isinstance(node, ast.Attribute) and node.attr == "handle_webui"
                    for node in ast.walk(handler)
                )
                self.assertTrue(delegates, "Tater Open WebUI handler must reuse the WebUI implementation")

    def test_manifest_exposes_updated_packages(self):
        manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
        entries = {entry["id"]: entry for entry in manifest["verbas"]}

        for package_id, expected_version in PACKAGES.items():
            with self.subTest(package_id=package_id):
                entry = entries[package_id]
                source = ROOT / entry["entry"]
                self.assertEqual(entry["version"], expected_version)
                self.assertIn("tater_open_webui", entry["portals"])
                self.assertEqual(
                    entry["sha256"],
                    hashlib.sha256(source.read_bytes()).hexdigest(),
                )


if __name__ == "__main__":
    unittest.main()
