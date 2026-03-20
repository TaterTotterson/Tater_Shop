# tools/generate_manifest.py
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERBA_DIR = ROOT / "verba"
MANIFEST_PATH = ROOT / "manifest.json"
SCHEMA_VERSION = 1

FIELDS = {
    "name",
    "verba_name",
    "pretty_name",
    "version",
    "min_tater_version",
    "description",
    "verba_dec",
    "platforms",
    "portals",
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


def normalize_surfaces(raw) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen = set()
    for item in raw:
        val = str(item or "").strip().lower()
        if not val or val in seen:
            continue
        seen.add(val)
        out.append(val)
    return out


def ensure_tag(values: list[str], tag: str) -> list[str]:
    tag = str(tag).strip().lower()
    if tag and tag not in values:
        values.append(tag)
    return values


def extract_verba_meta(py_file: Path) -> dict:
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))

    class_assigns = {}
    for n in tree.body:
        if not isinstance(n, ast.ClassDef):
            continue
        base_names = []
        for b in n.bases:
            if isinstance(b, ast.Name):
                base_names.append(b.id)
            elif isinstance(b, ast.Attribute):
                base_names.append(b.attr)

        looks_like_verba = ("ToolVerba" in base_names)

        assigns = {}
        for stmt in n.body:
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                key = stmt.targets[0].id
                if key in FIELDS:
                    val = literal(stmt.value)
                    if val is not None:
                        assigns[key] = val

        if looks_like_verba or ("name" in assigns and ("portals" in assigns or "platforms" in assigns)):
            class_assigns = assigns
            break

    pid = class_assigns.get("name") or py_file.stem
    display = class_assigns.get("verba_name") or class_assigns.get("pretty_name") or pid
    desc = class_assigns.get("verba_dec") or class_assigns.get("description") or ""

    surfaces = normalize_surfaces(class_assigns.get("portals") or class_assigns.get("platforms") or [])
    is_notifier = bool(class_assigns.get("notifier", False))
    if is_notifier:
        surfaces = ensure_tag(surfaces, "notifier")

    return {
        "id": pid,
        "name": display,
        "version": class_assigns.get("version", "0.0.0"),
        "min_tater_version": class_assigns.get("min_tater_version", "0.0.0"),
        "description": desc,
        "portals": surfaces,
        "notifier": is_notifier,
        "settings_category": class_assigns.get("settings_category", None),
        "tags": list(class_assigns.get("tags") or []),
    }


def main():
    if not VERBA_DIR.exists():
        raise SystemExit(f"Missing verba dir: {VERBA_DIR}")

    verbas = []
    errors = []

    for py_file in sorted(VERBA_DIR.glob("*.py")):
        if py_file.name.startswith("_"):
            continue

        rel_entry = str(py_file.relative_to(ROOT)).replace("\\", "/")
        try:
            meta = extract_verba_meta(py_file)
            meta["entry"] = rel_entry
            meta["sha256"] = sha256_file(py_file)
            verbas.append(meta)
        except Exception as e:
            errors.append({"file": py_file.name, "error": str(e)})

    manifest = {"schema": SCHEMA_VERSION, "verbas": verbas}
    MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )

    if errors:
        print("Manifest build errors:")
        for err in errors:
            print(f" - {err['file']}: {err['error']}")
        raise SystemExit(1)

    print(f"Wrote {MANIFEST_PATH} with {len(verbas)} verbas")


if __name__ == "__main__":
    main()
