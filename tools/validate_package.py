#!/usr/bin/env python3
"""Statically validate Tater extension manifests and their single-file entries."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


DEFAULT_ROOT = Path(__file__).resolve().parents[1]
ID_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
SEMVER_RE = re.compile(r"^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
BUILD_RE = re.compile(r"^\d+(?:\.\d+){0,2}$")
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")

KIND_CONFIG = {
    "verba": {
        "manifest": "manifest.json",
        "collection": "verbas",
        "directory": "verba",
    },
    "portal": {
        "manifest": "portal_manifest.json",
        "collection": "portals",
        "directory": "portals",
    },
    "core": {
        "manifest": "core_manifest.json",
        "collection": "cores",
        "directory": "cores",
    },
}


@dataclass(frozen=True)
class Finding:
    severity: str
    location: str
    message: str


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assignment_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in getattr(tree, "body", []):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def _function_names(tree: ast.AST) -> set[str]:
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _top_level_function_names(tree: ast.AST) -> set[str]:
    return {
        node.name
        for node in getattr(tree, "body", [])
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _expected_entry(kind: str, item_id: str) -> str:
    if kind == "verba":
        return f"verba/{item_id}.py"
    return f"{KIND_CONFIG[kind]['directory']}/{item_id}_{kind}.py"


def _safe_entry_path(root: Path, entry: str) -> Path | None:
    raw = str(entry or "").strip()
    if not raw or raw.startswith(("/", "\\")):
        return None
    candidate = (root / raw).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return None
    return candidate


def _validate_source_contract(kind: str, path: Path, location: str) -> list[Finding]:
    findings: list[Finding] = []
    try:
        source = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return [Finding("error", location, "source is not valid UTF-8")]
    except OSError as exc:
        return [Finding("error", location, f"could not read source: {exc}")]

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        line = f" line {exc.lineno}" if exc.lineno else ""
        return [Finding("error", location, f"Python syntax error{line}: {exc.msg}")]

    assignments = _assignment_names(tree)
    functions = _function_names(tree)
    top_functions = _top_level_function_names(tree)

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and int(node.level or 0) > 0:
            findings.append(
                Finding(
                    "error",
                    location,
                    "relative imports cannot be installed with the single-file package format",
                )
            )
            break

    if kind == "verba":
        if "verba" not in assignments:
            findings.append(Finding("error", location, "missing module-level `verba = ...` assignment"))
        if not any(name.startswith("handle_") for name in functions):
            findings.append(Finding("error", location, "no platform `handle_*` method was found"))
    elif kind == "portal":
        if "run" not in top_functions:
            findings.append(Finding("error", location, "missing top-level `run(stop_event=...)`"))
        if "PORTAL_SETTINGS" not in assignments:
            findings.append(Finding("error", location, "missing module-level `PORTAL_SETTINGS`"))
        if not ({"__version__", "VERSION", "PORTAL_VERSION"} & assignments):
            findings.append(Finding("error", location, "missing Portal version constant"))
    elif kind == "core":
        if "run" not in top_functions:
            findings.append(Finding("error", location, "missing top-level `run(stop_event=...)`"))
        if "CORE_SETTINGS" not in assignments:
            findings.append(Finding("error", location, "missing module-level `CORE_SETTINGS`"))
        if not ({"__version__", "VERSION", "CORE_VERSION"} & assignments):
            findings.append(Finding("error", location, "missing Core version constant"))
        if "CORE_WEBUI_TAB" in assignments and "get_htmlui_tab_data" not in top_functions:
            findings.append(
                Finding("error", location, "`CORE_WEBUI_TAB` requires top-level `get_htmlui_tab_data()`")
            )
        if "get_hydra_kernel_tools" in top_functions and "run_hydra_kernel_tool" not in top_functions:
            findings.append(
                Finding("error", location, "`get_hydra_kernel_tools()` requires `run_hydra_kernel_tool()`")
            )
        if "handle_htmlui_tab_action" in top_functions and "get_htmlui_tab_data" not in top_functions:
            findings.append(
                Finding("warning", location, "UI action handler exists without a Core tab data provider")
            )

    return findings


def _validate_item(kind: str, root: Path, item: Any, index: int) -> tuple[list[Finding], str, str]:
    config = KIND_CONFIG[kind]
    location = f"{config['manifest']}:{config['collection']}[{index}]"
    findings: list[Finding] = []
    if not isinstance(item, dict):
        return [Finding("error", location, "item must be a JSON object")], "", ""

    item_id = str(item.get("id") or "").strip()
    entry = str(item.get("entry") or "").strip().replace("\\", "/")
    if not item_id:
        findings.append(Finding("error", location, "missing `id`"))
    elif not ID_RE.fullmatch(item_id):
        findings.append(Finding("error", location, "`id` must be lowercase snake_case"))

    name = str(item.get("name") or "").strip()
    if not name:
        findings.append(Finding("error", location, "missing user-facing `name`"))

    version = str(item.get("version") or "").strip()
    if not version:
        findings.append(Finding("error", location, "missing `version`"))
    elif not SEMVER_RE.fullmatch(version):
        findings.append(Finding("error", location, "`version` must be semantic, for example 1.2.0"))

    min_version = str(item.get("min_tater_version") or "").strip()
    if not min_version:
        findings.append(Finding("warning", location, "missing advisory `min_tater_version`"))
    elif not BUILD_RE.fullmatch(min_version):
        findings.append(Finding("warning", location, "`min_tater_version` should be a numeric Tater build identifier"))

    description = str(item.get("description") or "").strip()
    if not description:
        findings.append(Finding("warning", location, "missing Store `description`"))

    if item_id and entry:
        expected = _expected_entry(kind, item_id)
        if entry != expected:
            findings.append(Finding("error", location, f"entry must be `{expected}` for id `{item_id}`"))
    elif not entry:
        findings.append(Finding("error", location, "missing `entry`"))

    source_path = _safe_entry_path(root, entry)
    if entry and source_path is None:
        findings.append(Finding("error", location, "entry must be a safe path inside the repository"))
        return findings, item_id, entry
    if source_path is None:
        return findings, item_id, entry
    if not source_path.is_file():
        findings.append(Finding("error", location, f"entry file does not exist: {entry}"))
        return findings, item_id, entry

    expected_sha = str(item.get("sha256") or "").strip()
    if not expected_sha:
        findings.append(Finding("error", location, "missing `sha256`"))
    elif not SHA256_RE.fullmatch(expected_sha):
        findings.append(Finding("error", location, "`sha256` must contain 64 hexadecimal characters"))
    else:
        actual_sha = _sha256(source_path)
        if actual_sha.lower() != expected_sha.lower():
            findings.append(
                Finding(
                    "error",
                    location,
                    f"sha256 does not match {entry}; regenerate the manifest",
                )
            )

    findings.extend(_validate_source_contract(kind, source_path, entry))

    if kind in {"portal", "core"} and item_id:
        expected_module = f"{item_id}_{kind}"
        module_key = str(item.get("module_key") or "").strip()
        if module_key != expected_module:
            findings.append(Finding("error", location, f"module_key must be `{expected_module}`"))
        expected_autostart = f"{expected_module}_running"
        autostart = str(item.get("autostart_key") or "").strip()
        if autostart != expected_autostart:
            findings.append(Finding("error", location, f"autostart_key must be `{expected_autostart}`"))

    return findings, item_id, entry


def validate_kind(root: Path, kind: str) -> list[Finding]:
    config = KIND_CONFIG[kind]
    manifest_path = root / config["manifest"]
    if not manifest_path.is_file():
        return [Finding("error", config["manifest"], "manifest file does not exist")]

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [Finding("error", config["manifest"], f"invalid JSON at line {exc.lineno}: {exc.msg}")]
    except OSError as exc:
        return [Finding("error", config["manifest"], f"could not read manifest: {exc}")]

    findings: list[Finding] = []
    if not isinstance(manifest, dict):
        return [Finding("error", config["manifest"], "manifest root must be a JSON object")]
    if manifest.get("schema") != 1:
        findings.append(Finding("error", config["manifest"], "`schema` must be 1"))

    items = manifest.get(config["collection"])
    if not isinstance(items, list):
        return [
            *findings,
            Finding("error", config["manifest"], f"`{config['collection']}` must be a list"),
        ]

    seen_ids: set[str] = set()
    manifest_entries: set[str] = set()
    for index, item in enumerate(items):
        item_findings, item_id, entry = _validate_item(kind, root, item, index)
        findings.extend(item_findings)
        if item_id:
            if item_id in seen_ids:
                findings.append(Finding("error", config["manifest"], f"duplicate id `{item_id}`"))
            seen_ids.add(item_id)
        if entry:
            manifest_entries.add(entry)

    source_dir = root / config["directory"]
    if source_dir.is_dir():
        for source_path in sorted(source_dir.glob("*.py")):
            if source_path.name.startswith("_"):
                continue
            rel = source_path.relative_to(root).as_posix()
            if rel not in manifest_entries:
                findings.append(Finding("warning", rel, "source file is not listed in the manifest"))

    return findings


def _selected_kinds(root: Path, requested: str) -> Iterable[str]:
    if requested != "auto":
        return [requested]
    found = [
        kind
        for kind, config in KIND_CONFIG.items()
        if (root / config["manifest"]).is_file()
    ]
    return found


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Statically validate Tater Verba, Portal, and Core packages without importing them."
    )
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="Extension repository root")
    parser.add_argument("--kind", choices=["auto", *KIND_CONFIG], default="auto")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        print(f"ERROR {root}: repository root does not exist", file=sys.stderr)
        return 2

    kinds = list(_selected_kinds(root, args.kind))
    if not kinds:
        print(f"ERROR {root}: no Tater manifests found", file=sys.stderr)
        return 2

    findings: list[Finding] = []
    for kind in kinds:
        findings.extend(validate_kind(root, kind))

    errors = [finding for finding in findings if finding.severity == "error"]
    warnings = [finding for finding in findings if finding.severity == "warning"]
    for finding in findings:
        print(f"{finding.severity.upper()} {finding.location}: {finding.message}")

    if errors:
        print(
            f"Validation failed: {len(errors)} error(s), {len(warnings)} warning(s) across {', '.join(kinds)}.",
            file=sys.stderr,
        )
        return 1

    print(f"Validation passed: {len(warnings)} warning(s) across {', '.join(kinds)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
