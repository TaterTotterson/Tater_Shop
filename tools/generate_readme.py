from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "manifest.json"
CORE_MANIFEST = ROOT / "core_manifest.json"
PORTAL_MANIFEST = ROOT / "portal_manifest.json"
README = ROOT / "README.md"

BEGIN = "<!-- AUTO:VERBA_TABLES:BEGIN -->"
END = "<!-- AUTO:VERBA_TABLES:END -->"


def md_escape(s: str) -> str:
    return (s or "").replace("\n", " ").replace("|", "\\|").strip()


def verba_surfaces(p: dict) -> list[str]:
    return list(p.get("portals") or [])


def verba_row(p: dict) -> str:
    pid = md_escape(p.get("id", ""))
    desc = md_escape(p.get("description", ""))
    portals = md_escape(", ".join(verba_surfaces(p)))
    return f"| `{pid}` | {desc} | {portals} |"


def core_row(c: dict) -> str:
    cid = md_escape(c.get("id", ""))
    module_key = md_escape(c.get("module_key", ""))
    desc = md_escape(c.get("description", ""))
    return f"| `{cid}` | `{module_key}` | {desc} |"


def portal_row(p: dict) -> str:
    pid = md_escape(p.get("id", ""))
    module_key = md_escape(p.get("module_key", ""))
    desc = md_escape(p.get("description", ""))
    return f"| `{pid}` | `{module_key}` | {desc} |"


def build_verba_tables(verbas: list[dict]) -> str:
    verbas = sorted(verbas, key=lambda x: (x.get("id") or "").lower())

    def portals(p):
        return set(verba_surfaces(p))

    def is_notifier(p):
        return ("notifier" in portals(p)) or bool(p.get("notifier", False))

    def is_automation(p):
        return "automation" in portals(p)

    interactive = [p for p in verbas if (not is_notifier(p)) and (not is_automation(p))]
    automation = [p for p in verbas if is_automation(p)]
    notifiers = [p for p in verbas if is_notifier(p)]

    def table_block(title: str, items: list[dict]) -> str:
        if not items:
            return f"### {title}\n\n*(none)*\n"
        lines = [
            f"### {title}",
            "",
            "| Verba ID | Description | Portals |",
            "|------------|-------------|----------|",
        ]
        lines += [verba_row(p) for p in items]
        lines.append("")
        return "\n".join(lines)

    out = []
    out.append("## 🧩 Verba Store\n")
    out.append(table_block("💬 Interactive / Conversational Verbas", interactive))
    out.append(table_block("⚙️ Automation Verbas (Home Assistant)", automation))
    out.append(table_block("📡 RSS Notifier Verbas", notifiers))
    return "\n".join(out).strip() + "\n"


def build_core_table(cores: list[dict]) -> str:
    cores = sorted(cores, key=lambda x: (x.get("id") or "").lower())

    out = []
    out.append("## 🧠 Core Store\n")

    if not cores:
        out.append("*(none)*\n")
        return "\n".join(out).strip() + "\n"

    out.append("| Core ID | Module | Description |")
    out.append("|-------------|--------|-------------|")
    out.extend(core_row(c) for c in cores)
    out.append("")
    return "\n".join(out).strip() + "\n"


def build_portal_table(portals: list[dict]) -> str:
    portals = sorted(portals, key=lambda x: (x.get("id") or "").lower())

    out = []
    out.append("## 🌐 Portal Store\n")

    if not portals:
        out.append("*(none)*\n")
        return "\n".join(out).strip() + "\n"

    out.append("| Portal ID | Module | Description |")
    out.append("|-------------|--------|-------------|")
    out.extend(portal_row(p) for p in portals)
    out.append("")
    return "\n".join(out).strip() + "\n"


def load_manifest_items(path: Path, key: str) -> list[dict]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    items = payload.get(key)
    if isinstance(items, list):
        return [item for item in items if isinstance(item, dict)]
    return []


def build_generated_block() -> str:
    verbas = load_manifest_items(MANIFEST, "verbas")
    cores = load_manifest_items(CORE_MANIFEST, "cores")
    portals = load_manifest_items(PORTAL_MANIFEST, "portals")
    verba_text = build_verba_tables(verbas)
    core_text = build_core_table(cores)
    portal_text = build_portal_table(portals)
    return (core_text + "\n" + portal_text + "\n" + verba_text).strip() + "\n"


def main() -> None:
    generated = build_generated_block()

    if not README.exists():
        README.write_text(f"{BEGIN}\n{generated}\n{END}\n", encoding="utf-8")
        print("Created README.md")
        return

    text = README.read_text(encoding="utf-8")
    if BEGIN in text and END in text:
        before = text.split(BEGIN, 1)[0]
        after = text.split(END, 1)[1]
        new_text = before + BEGIN + "\n" + generated + "\n" + END + after
    else:
        new_text = text.rstrip() + "\n\n" + BEGIN + "\n" + generated + "\n" + END + "\n"

    README.write_text(new_text, encoding="utf-8")
    print("Updated README.md")


if __name__ == "__main__":
    main()
