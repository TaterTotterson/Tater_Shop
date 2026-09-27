# tools/generate_manifest.py
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path

DEFAULT_ROOT = Path(__file__).resolve().parents[1]
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

    verba_class_name = ""
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "verba" for target in node.targets):
            continue
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name):
            verba_class_name = node.value.func.id
            break

    class_defs = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
    }

    def assignments_for(class_name: str, visiting: set[str] | None = None) -> dict:
        node = class_defs.get(class_name)
        if node is None:
            return {}
        visiting = set(visiting or ())
        if class_name in visiting:
            return {}
        visiting.add(class_name)

        assigns = {}
        for base in node.bases:
            base_name = ""
            if isinstance(base, ast.Name):
                base_name = base.id
            elif isinstance(base, ast.Attribute):
                base_name = base.attr
            if base_name in class_defs:
                assigns.update(assignments_for(base_name, visiting))

        for stmt in node.body:
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                key = stmt.targets[0].id
                if key in FIELDS:
                    val = literal(stmt.value)
                    if val is not None:
                        assigns[key] = val
        return assigns

    class_assigns = assignments_for(verba_class_name) if verba_class_name else {}
    if class_assigns:
        candidate_names = ()
    else:
        candidate_names = tuple(class_defs)

    for class_name in candidate_names:
        n = class_defs[class_name]
        base_names = []
        for b in n.bases:
            if isinstance(b, ast.Name):
                base_names.append(b.id)
            elif isinstance(b, ast.Attribute):
                base_names.append(b.attr)

        looks_like_verba = bool(
            {"ToolVerba", "CategoryDeviceControlBase", "_DeviceVerbaRuntime"} & set(base_names)
        )
        assigns = assignments_for(class_name)

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


def build_manifest(verba_dir: Path, root: Path) -> tuple[dict, list[dict[str, str]]]:
    if not verba_dir.exists():
        raise FileNotFoundError(f"Missing verba dir: {verba_dir}")
    verbas = []
    errors = []
    for py_file in sorted(verba_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue

        rel_entry = str(py_file.relative_to(root)).replace("\\", "/")
        try:
            meta = extract_verba_meta(py_file)
            meta["entry"] = rel_entry
            meta["sha256"] = sha256_file(py_file)
            verbas.append(meta)
        except Exception as e:
            errors.append({"file": py_file.name, "error": str(e)})

    return {"schema": SCHEMA_VERSION, "verbas": verbas}, errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Verba manifest for Tater Shop")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="Extension repository root")
    parser.add_argument("--verba-dir", default="", help="Verba source directory (default: <root>/verba)")
    parser.add_argument("--output", default="", help="Manifest path (default: <root>/manifest.json)")
    parser.add_argument("--name", default="", help="Optional repository display name stored in the manifest")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    verba_dir = Path(args.verba_dir).expanduser().resolve() if args.verba_dir else root / "verba"
    manifest_path = Path(args.output).expanduser().resolve() if args.output else root / "manifest.json"

    manifest, errors = build_manifest(verba_dir=verba_dir, root=root)
    repository_name = str(args.name or "").strip()
    if repository_name:
        manifest = {"schema": manifest["schema"], "name": repository_name, "verbas": manifest["verbas"]}
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8"
    )

    if errors:
        print("Manifest build errors:")
        for err in errors:
            print(f" - {err['file']}: {err['error']}")
        return 1

    print(f"Wrote {manifest_path} with {len(manifest['verbas'])} verbas")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
