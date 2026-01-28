# tools/generate_manifest.py
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGINS_DIR = ROOT / "plugins"
MANIFEST_PATH = ROOT / "manifest.json"
SCHEMA_VERSION = 1

FIELDS = {
    "name",
    "plugin_name",
    "pretty_name",
    "version",
    "min_tater_version",
    "description",
    "plugin_dec",
    "platforms",
    "notifier",
    "settings_category",
    "tags",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def literal(node):
    try:
        return ast.literal_eval(node)
    except Exception:
        return None


def normalize_platforms(plats) -> list[str]:
    """
    Normalize platforms into a lowercased list[str] with stable unique ordering.
    Accepts:
      - None
      - "discord"
      - ["Discord", "webui", ...]
    """
    if not plats:
        return []
    if isinstance(plats, str):
        plats = [plats]
    if not isinstance(plats, list):
        return []
    out: list[str] = []
    for p in plats:
        if not p:
            continue
        out.append(str(p).strip().lower())
    # unique, stable order
    seen = set()
    uniq: list[str] = []
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def ensure_tag(plats: list[str], tag: str) -> list[str]:
    tag = str(tag).strip().lower()
    if not tag:
        return plats
    if tag not in plats:
        plats.append(tag)
    return plats


def extract_plugin_meta(py_file: Path) -> dict:
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))

    class_assigns = {}
    for n in tree.body:
        if isinstance(n, ast.ClassDef):
            # Take the first class that looks like a plugin (inherits ToolPlugin or has fields)
            base_names = []
            for b in n.bases:
                if isinstance(b, ast.Name):
                    base_names.append(b.id)
                elif isinstance(b, ast.Attribute):
                    base_names.append(b.attr)

            looks_like_plugin = ("ToolPlugin" in base_names)

            assigns = {}
            for stmt in n.body:
                if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                    key = stmt.targets[0].id
                    if key in FIELDS:
                        val = literal(stmt.value)
                        if val is not None:
                            assigns[key] = val

            # If it inherits ToolPlugin OR it has a name/platforms field, treat it as the plugin class
            if looks_like_plugin or ("name" in assigns and "platforms" in assigns):
                class_assigns = assigns
                break

    # Fallbacks
    pid = class_assigns.get("name") or py_file.stem
    display = class_assigns.get("plugin_name") or class_assigns.get("pretty_name") or pid
    desc = class_assigns.get("plugin_dec") or class_assigns.get("description") or ""

    plats = normalize_platforms(class_assigns.get("platforms") or [])
    is_notifier = bool(class_assigns.get("notifier", False))

    # Backward compatible:
    # - keep notifier boolean field
    # - ALSO add "notifier" into platforms so the Shop can filter by platform
    if is_notifier:
        plats = ensure_tag(plats, "notifier")

    return {
        "id": pid,
        "name": display,
        "version": class_assigns.get("version", "0.0.0"),
        "min_tater_version": class_assigns.get("min_tater_version", "0.0.0"),
        "description": desc,
        "platforms": plats,
        "notifier": is_notifier,
        "settings_category": class_assigns.get("settings_category", None),
        "tags": list(class_assigns.get("tags") or []),
    }


def main():
    if not PLUGINS_DIR.exists():
        raise SystemExit(f"Missing plugins dir: {PLUGINS_DIR}")

    plugins = []
    errors = []

    for py_file in sorted(PLUGINS_DIR.glob("*.py")):
        if py_file.name.startswith("_"):
            continue

        rel_entry = str(py_file.relative_to(ROOT)).replace("\\", "/")
        try:
            meta = extract_plugin_meta(py_file)
            meta["entry"] = rel_entry
            meta["sha256"] = sha256_file(py_file)
            plugins.append(meta)
        except Exception as e:
            errors.append({"file": py_file.name, "error": str(e)})

    manifest = {"schema": SCHEMA_VERSION, "plugins": plugins}
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )

    if errors:
        print("Manifest build errors:")
        for err in errors:
            print(f" - {err['file']}: {err['error']}")
        raise SystemExit(1)

    print(f"Wrote {MANIFEST_PATH} with {len(plugins)} plugins")


if __name__ == "__main__":
    main()
