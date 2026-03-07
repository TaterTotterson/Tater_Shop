#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path

SCHEMA_VERSION = 1


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


def normalize_tags(raw) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, (list, tuple, set)):
        return []

    out: list[str] = []
    seen = set()
    for item in raw:
        tag = str(item or "").strip().lower()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
    return out


def collect_simple_constants(tree: ast.AST) -> dict[str, object]:
    constants: dict[str, object] = {}
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            key = node.targets[0].id
            value = literal(node.value)
            if value is not None:
                constants[key] = value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
            key = node.target.id
            value = literal(node.value)
            if value is not None:
                constants[key] = value
    return constants


def resolve_simple(node: ast.AST | None, constants: dict[str, object]):
    if node is None:
        return None
    direct = literal(node)
    if direct is not None:
        return direct
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        value = resolve_simple(node.operand, constants)
        if isinstance(value, (int, float)):
            return -value
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        items = []
        for elt in node.elts:
            val = resolve_simple(elt, constants)
            if val is not None:
                items.append(val)
        return items
    if isinstance(node, ast.Dict):
        out = {}
        for k_node, v_node in zip(node.keys, node.values):
            key = resolve_simple(k_node, constants)
            if key is None:
                continue
            value = resolve_simple(v_node, constants)
            if value is None:
                continue
            out[str(key)] = value
        return out
    return None


def extract_core_settings(tree: ast.AST, constants: dict[str, object]) -> dict[str, object]:
    for node in getattr(tree, "body", []):
        if not (isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)):
            continue
        if node.targets[0].id != "CORE_SETTINGS":
            continue
        if not isinstance(node.value, ast.Dict):
            return {}

        category = ""
        required_count = 0
        tags: list[str] = []

        for key_node, value_node in zip(node.value.keys, node.value.values):
            key = resolve_simple(key_node, constants)
            if key == "category":
                value = resolve_simple(value_node, constants)
                category = str(value or "").strip()
            elif key == "required" and isinstance(value_node, ast.Dict):
                required_count = len(value_node.keys)
            elif key == "tags":
                tags = normalize_tags(resolve_simple(value_node, constants))

        return {
            "category": category,
            "required_count": required_count,
            "tags": tags,
        }

    return {}


def extract_core_meta(py_file: Path, min_tater_version_default: str) -> dict[str, object]:
    source = py_file.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(py_file))
    constants = collect_simple_constants(tree)
    settings_meta = extract_core_settings(tree, constants)

    module_key = py_file.stem
    core_id = module_key[:-len("_core")] if module_key.endswith("_core") else module_key

    category = str(settings_meta.get("category") or "").strip()
    display_name = category.replace(" Settings", "").strip() if category else core_id.replace("_", " ").title()

    doc = ast.get_docstring(tree) or ""
    first_doc_line = next((line.strip() for line in doc.splitlines() if line.strip()), "")

    description = str(
        constants.get("CORE_DESCRIPTION")
        or constants.get("DESCRIPTION")
        or first_doc_line
        or (f"{display_name} integration core for Tater.")
    ).strip()

    version = str(constants.get("__version__") or constants.get("VERSION") or "0.0.0").strip() or "0.0.0"
    min_tater_version = str(constants.get("MIN_TATER_VERSION") or min_tater_version_default).strip()
    tags = normalize_tags(constants.get("TAGS") or settings_meta.get("tags") or [])

    return {
        "id": core_id,
        "name": display_name,
        "module_key": module_key,
        "version": version,
        "min_tater_version": min_tater_version,
        "description": description,
        "settings_category": category or None,
        "required_settings_count": int(settings_meta.get("required_count") or 0),
        "autostart_key": f"{module_key}_running",
        "tags": tags,
    }


def build_manifest(cores_dir: Path, root: Path, min_tater_version_default: str) -> tuple[dict, list[dict[str, str]]]:
    if not cores_dir.exists():
        raise FileNotFoundError(f"Missing cores dir: {cores_dir}")

    items = []
    errors = []

    for py_file in sorted(cores_dir.glob("*_core.py")):
        if py_file.name.startswith("_"):
            continue
        rel_entry = str(py_file.relative_to(root)).replace("\\", "/")
        try:
            meta = extract_core_meta(py_file, min_tater_version_default=min_tater_version_default)
            meta["entry"] = rel_entry
            meta["sha256"] = sha256_file(py_file)
            items.append(meta)
        except Exception as exc:
            errors.append({"file": py_file.name, "error": str(exc)})

    return {"schema": SCHEMA_VERSION, "cores": items}, errors


def main() -> int:
    root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(description="Generate core manifest for Tater Shop")
    parser.add_argument("--cores-dir", default=str(root / "cores"))
    parser.add_argument("--output", default=str(root / "core_manifest.json"))
    parser.add_argument("--min-tater-version", default="59")
    args = parser.parse_args()

    cores_dir = Path(args.cores_dir).resolve()
    output_path = Path(args.output).resolve()

    manifest, errors = build_manifest(
        cores_dir=cores_dir,
        root=root,
        min_tater_version_default=str(args.min_tater_version),
    )

    output_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    if errors:
        print("Core manifest build errors:")
        for err in errors:
            print(f" - {err['file']}: {err['error']}")
        return 1

    print(f"Wrote {output_path} with {len(manifest['cores'])} cores")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
