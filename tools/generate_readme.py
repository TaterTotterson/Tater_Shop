from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "manifest.json"
PLATFORM_MANIFEST = ROOT / "platform_manifest.json"
README = ROOT / "README.md"

BEGIN = "<!-- AUTO:PLUGIN_TABLES:BEGIN -->"
END = "<!-- AUTO:PLUGIN_TABLES:END -->"


def md_escape(s: str) -> str:
    return (s or "").replace("\n", " ").replace("|", "\\|").strip()


def plugin_row(p: dict) -> str:
    pid = md_escape(p.get("id", ""))
    desc = md_escape(p.get("description", ""))
    plats = ", ".join((p.get("platforms") or []))
    plats = md_escape(plats)
    return f"| `{pid}` | {desc} | {plats} |"


def platform_row(p: dict) -> str:
    pid = md_escape(p.get("id", ""))
    module_key = md_escape(p.get("module_key", ""))
    desc = md_escape(p.get("description", ""))
    return f"| `{pid}` | `{module_key}` | {desc} |"


def build_plugin_tables(plugins: list[dict]) -> str:
    # Normalize sorting
    plugins = sorted(plugins, key=lambda x: (x.get("id") or "").lower())

    def plats(p):
        return set((p.get("platforms") or []))

    def is_notifier(p):
        return ("notifier" in plats(p)) or bool(p.get("notifier", False))

    def is_automation(p):
        return "automation" in plats(p)

    interactive = [p for p in plugins if (not is_notifier(p)) and (not is_automation(p))]
    automation = [p for p in plugins if is_automation(p)]
    notifiers = [p for p in plugins if is_notifier(p)]

    def table_block(title: str, items: list[dict]) -> str:
        if not items:
            return f"### {title}\n\n*(none)*\n"
        lines = [
            f"### {title}",
            "",
            "| Plugin Name | Description | Platform |",
            "|------------|-------------|----------|",
        ]
        lines += [plugin_row(p) for p in items]
        lines.append("")
        return "\n".join(lines)

    out = []
    out.append("## 🧩 Verba Plugin Store (Tater Shop)\n")
    out.append(table_block("💬 Interactive / Conversational Plugins", interactive))
    out.append(table_block("⚙️ Automation Plugins (Home Assistant)", automation))
    out.append(table_block("📡 RSS Notifier Plugins", notifiers))
    return "\n".join(out).strip() + "\n"


def build_platform_table(platforms: list[dict]) -> str:
    platforms = sorted(platforms, key=lambda x: (x.get("id") or "").lower())

    out = []
    out.append("## 🌐 Platform Store\n")

    if not platforms:
        out.append("*(none)*\n")
        return "\n".join(out).strip() + "\n"

    out.append("| Platform ID | Module | Description |")
    out.append("|-------------|--------|-------------|")
    out.extend(platform_row(p) for p in platforms)
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
    plugins = load_manifest_items(MANIFEST, "plugins")
    platforms = load_manifest_items(PLATFORM_MANIFEST, "platforms")
    plugin_text = build_plugin_tables(plugins)
    platform_text = build_platform_table(platforms)
    return (platform_text + "\n" + plugin_text).strip() + "\n"


def main() -> None:
    generated = build_generated_block()

    # If README doesn’t exist yet, create a basic one
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
        # Append if markers missing
        new_text = text.rstrip() + "\n\n" + BEGIN + "\n" + generated + "\n" + END + "\n"

    README.write_text(new_text, encoding="utf-8")
    print("Updated README.md")


if __name__ == "__main__":
    main()
