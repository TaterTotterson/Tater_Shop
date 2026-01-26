# tools/generate_manifest.py
from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGINS_DIR = ROOT / "plugins"
MANIFEST_PATH = ROOT / "manifest.json"

SCHEMA_VERSION = 1


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_plugin_from_file(py_file: Path):
    # Import by file path (no package required)
    mod_name = f"store_{py_file.stem}"
    spec = importlib.util.spec_from_file_location(mod_name, py_file)
    if not spec or not spec.loader:
        raise RuntimeError(f"Could not import {py_file.name}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)

    plugin = getattr(module, "plugin", None)
    if plugin is None:
        raise RuntimeError(f"{py_file.name} has no top-level `plugin = ...` instance")

    return plugin


def main():
    plugins = []
    errors = []

    if not PLUGINS_DIR.exists():
        raise SystemExit(f"Missing plugins dir: {PLUGINS_DIR}")

    for py_file in sorted(PLUGINS_DIR.glob("*.py")):
        if py_file.name.startswith("_"):
            continue

        rel_entry = str(py_file.relative_to(ROOT)).replace("\\", "/")
        try:
            p = load_plugin_from_file(py_file)

            pid = getattr(p, "name", "") or py_file.stem
            display_name = getattr(p, "plugin_name", "") or getattr(p, "pretty_name", "") or pid

            item = {
                "id": pid,
                "name": display_name,
                "version": getattr(p, "version", "0.0.0"),
                "min_tater_version": getattr(p, "min_tater_version", "0.0.0"),
                "description": getattr(p, "plugin_dec", "") or getattr(p, "description", "") or "",
                "platforms": list(getattr(p, "platforms", []) or []),
                "notifier": bool(getattr(p, "notifier", False)),
                "settings_category": getattr(p, "settings_category", None),
                "tags": list(getattr(p, "tags", []) or []),
                "entry": rel_entry,
                "sha256": sha256_file(py_file),
            }

            plugins.append(item)

        except Exception as e:
            errors.append({"file": py_file.name, "error": str(e)})

    manifest = {"schema": SCHEMA_VERSION, "plugins": plugins}
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    # If any plugin fails to load, exit non-zero so CI fails (you’ll notice)
    if errors:
        print("Plugin manifest build errors:")
        for err in errors:
            print(f" - {err['file']}: {err['error']}")
        raise SystemExit(1)

    print(f"Wrote {MANIFEST_PATH} with {len(plugins)} plugins")


if __name__ == "__main__":
    main()
