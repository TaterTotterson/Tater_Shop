# Portal starter

Copy this folder's `portals/` directory into a new repository, then rename `example_portal.py` and change its IDs, metadata, settings, transport, and delivery functions.

The starter includes:

- A correct asyncio event-loop boundary inside synchronous `run()`.
- Cooperative shutdown.
- Settings loading.
- Normalized origin and conversation scope.
- A complete Hydra call shape.
- Separate receive and delivery functions to replace with a platform SDK.

From this template directory:

```bash
python3 ../../tools/generate_portal_manifest.py --root . --name "Example Portal Repository"
python3 ../../tools/validate_package.py --root . --kind portal
```

When using the template outside this repository, replace `../../tools/` with the path to a Tater Shop checkout or copy the two standard-library-only tools into your repository.

Read [Build a Portal](https://github.com/TaterTotterson/Tater_Shop/blob/main/docs/portal-authoring.md) and [Repository manifests and publishing](https://github.com/TaterTotterson/Tater_Shop/blob/main/docs/repository-manifests.md) before publishing.
