# Tater Shop

<div align="center">
  <a href="https://taterassistant.com">
    <img src="images/tater-repo-logo.png" alt="Tater Shop" width="460"/>
  </a>
</div>
<p align="center">
  <a href="https://taterassistant.com">
    <img alt="Visit Tater Assistant" src="https://img.shields.io/badge/Tater%20Assistant-Visit%20Website-F28C28?style=for-the-badge&logo=googlechrome&logoColor=white" />
  </a>
  <a href="https://discord.gg/w52namKyXT">
    <img alt="Join the Tater Assistant Discord" src="https://img.shields.io/badge/Discord-Join%20the%20Community-5865F2?style=for-the-badge&logo=discord&logoColor=white" />
  </a>
</p>

Tater Shop is the modular source repository for Tater Verbas, Portals, and Cores. Tater reads its generated manifests and installs each selected extension as a single Python module.

## Choose an extension type

| Type | Use it for | Authoring guide | Starter template |
| --- | --- | --- | --- |
| **Verba** | A focused tool Hydra may call to answer or act on a request | [Build a Verba](docs/verba-authoring.md) | [Verba template](templates/verba) |
| **Portal** | A transport that receives messages from a platform and delivers Tater's responses | [Build a Portal](docs/portal-authoring.md) | [Portal template](templates/portal) |
| **Core** | A background service, Core-owned Hydra tools, context, or a custom Vue-rendered management tab | [Build a Core](docs/core-authoring.md) | [Core template](templates/core) |

If you are publishing extensions from your own GitHub repository, also read [Repository manifests and publishing](docs/repository-manifests.md).

## Quick start

1. Copy the closest starter template into your repository.
2. Give the module and its stable ID a unique lowercase `snake_case` name.
3. Replace the example behavior and metadata.
4. Generate the corresponding manifest.
5. Validate the repository before publishing.
6. Add the raw manifest URL under the matching **Repositories** tab in Tater.

For work inside this repository:

```bash
python3 tools/generate_manifest.py
python3 tools/generate_core_manifest.py
python3 tools/generate_portal_manifest.py
python3 tools/validate_package.py --root .
```

The GitHub workflow regenerates the official manifests on push and validates the result.

## Repository layout

- `verba/`: official Hydra-callable tools.
- `portals/`: official platform runtimes.
- `cores/`: official background services and Core UI providers.
- `tools/`: manifest generators, validators, and maintenance utilities.
- `templates/`: small, working starting points for external authors.
- `tests/`: contract and behavior tests for official extensions.
- `manifest.json`: official Verba catalog.
- `portal_manifest.json`: official Portal catalog.
- `core_manifest.json`: official Core catalog.
- `*_repositories.json`: curated third-party repositories shown in Tater's **Trusted repositories** picker.

## Important packaging rules

- Every installed extension is one UTF-8 Python file. Tater does not install a package-specific `requirements.txt`, companion Python modules, or asset folders.
- Imports may use Python's standard library and stable APIs or dependencies already shipped by Tater. Handle optional imports gracefully.
- An extension runs inside the Tater process. It is not sandboxed and can access Tater's Redis connection, filesystem permissions, and network access. Only install code you trust.
- Manifest `entry` paths are resolved relative to the manifest URL. Keep the manifest at the repository root when using paths such as `cores/example_core.py`.
- Manifest IDs are global within their extension type. The first configured repository containing an ID wins, so third-party authors should choose distinctive IDs.
- Use semantic versions for extension `version` values and bump the version whenever users should receive an update.
- `MIN_TATER_VERSION` / `min_tater_version` is currently advisory catalog metadata. It uses Tater's compatible build identifier rather than the public release label; it does not replace testing against the oldest supported Tater build.

## Validation

Compile-check a changed module, regenerate its manifest, then validate the repository:

```bash
python3 -m py_compile verba/example_lookup.py
python3 tools/generate_manifest.py
python3 tools/validate_package.py --root . --kind verba
```

Use `core` or `portal` for the other package types. The validator checks IDs, versions, filenames, entries, checksums, Python syntax, and the required static contract without importing or executing extension code.

## Publishing your own repository

A third-party repository needs only its extension file or files and the matching generated manifest. Users add the raw manifest URL in Tater's Verba, Portal, or Core **Repositories** tab. See the [publishing guide](docs/repository-manifests.md) for complete examples and the trusted-repository submission format.

## Design principles

- Keep tools and actions narrow, predictable, and safe to retry.
- Do not perform network calls or long discovery during module import.
- Keep Core and Portal loops cooperative and quick to stop.
- Store credentials in settings, never in source code.
- Use shared Tater integrations instead of creating duplicate provider connections when one is available.
- Return structured results and concise user-facing messages.
- Keep UI payloads bounded; paginate or summarize long histories.
- Keep IDs stable after release. Renaming an ID creates a different package.

For questions or extension reviews, join the [Tater Assistant Discord](https://discord.gg/w52namKyXT).
