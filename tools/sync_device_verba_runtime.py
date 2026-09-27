#!/usr/bin/env python3
"""Embed the category-device runtime into every downloadable Verba.

The files in ``verba/`` are the artifacts downloaded by Tater. They must not
import another Verba or rely on an unlisted companion file. This script keeps
the repeated runtime block synchronized while preserving each plugin class
below it.
"""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "tools" / "device_verba_runtime.py"
VERBA_DIR = ROOT / "verba"
START_MARKER = "# BEGIN EMBEDDED DEVICE VERBA RUNTIME"
END_MARKER = "# END EMBEDDED DEVICE VERBA RUNTIME"
TARGETS = (
    "battery_status.py",
    "device_control.py",
    "energy_status.py",
    "entry_sensor_status.py",
    "humidity_status.py",
    "illuminance_status.py",
    "leak_status.py",
    "motion_status.py",
    "network_device_status.py",
    "presence_status.py",
    "sensor_status.py",
    "temperature_status.py",
)


def _runtime_source() -> str:
    source = RUNTIME_PATH.read_text(encoding="utf-8").rstrip()
    ast.parse(source, filename=str(RUNTIME_PATH))
    if "verba =" in source:
        raise RuntimeError("The embedded runtime must not instantiate a Verba.")
    if "class _DeviceVerbaRuntime(ToolVerba):" not in source:
        raise RuntimeError("The embedded runtime class is missing.")
    return source


def _plugin_source(path: Path, current: str) -> str:
    if START_MARKER in current or END_MARKER in current:
        if current.count(START_MARKER) != 1 or current.count(END_MARKER) != 1:
            raise RuntimeError(f"Invalid generated markers in {path}")
        return current.split(END_MARKER, 1)[1].lstrip()

    import_line = "from category_device_control import CategoryDeviceControlBase"
    if import_line not in current:
        raise RuntimeError(f"Expected legacy runtime import in {path}")
    plugin = current.replace(import_line, "", 1).lstrip()
    return plugin.replace("CategoryDeviceControlBase", "_DeviceVerbaRuntime")


def main() -> None:
    runtime = _runtime_source()
    for filename in TARGETS:
        path = VERBA_DIR / filename
        current = path.read_text(encoding="utf-8")
        plugin = _plugin_source(path, current)
        rendered = (
            "# Generated as a standalone Shop artifact. Run "
            "tools/sync_device_verba_runtime.py after editing the shared runtime.\n"
            f"{START_MARKER}\n"
            f"{runtime}\n"
            f"{END_MARKER}\n\n\n"
            f"{plugin.rstrip()}\n"
        )
        ast.parse(rendered, filename=str(path))
        path.write_text(rendered, encoding="utf-8")
        print(f"Updated {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
