# Core starter

Copy this folder's `cores/` directory into a new repository, then rename `example_core.py` and change its IDs, metadata, settings, and behavior.

The starter includes:

- A cooperative background loop.
- Core-owned Redis namespaces.
- A working Vue settings-manager tab.
- Create, save, and delete actions.
- Popup item settings.
- A small Hydra kernel tool.

From this template directory:

```bash
python3 ../../tools/generate_core_manifest.py --root . --name "Example Core Repository"
python3 ../../tools/validate_package.py --root . --kind core
```

When using the template outside this repository, replace `../../tools/` with the path to a Tater Shop checkout or copy the two standard-library-only tools into your repository.

Read [Build a Core](https://github.com/TaterTotterson/Tater_Shop/blob/main/docs/core-authoring.md) and [Repository manifests and publishing](https://github.com/TaterTotterson/Tater_Shop/blob/main/docs/repository-manifests.md) before publishing.
