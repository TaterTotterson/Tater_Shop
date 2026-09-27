# Repository manifests and publishing

Tater can install Verbas, Portals, and Cores from any HTTPS-accessible JSON manifest. A repository may publish one extension or many, but each manifest contains only one extension type.

## Recommended layout

Keep the manifest at the repository root so relative entries are obvious:

```text
my-tater-extension/
├── README.md
├── manifest.json
├── verba/
│   └── my_lookup.py
├── portal_manifest.json
├── portals/
│   └── my_chat_portal.py
├── core_manifest.json
└── cores/
    └── my_service_core.py
```

You only need the directory and manifest for extension types you actually publish.

Tater resolves each `entry` relative to the manifest URL. For example:

```text
Manifest URL:
https://raw.githubusercontent.com/example/my-tater-extension/main/core_manifest.json

Entry:
cores/my_service_core.py

Downloaded URL:
https://raw.githubusercontent.com/example/my-tater-extension/main/cores/my_service_core.py
```

Do not use a GitHub HTML page URL. Use the raw JSON URL.

## Manifest shapes

### Verba

```json
{
  "schema": 1,
  "name": "Example Verba Repository",
  "verbas": [
    {
      "id": "example_lookup",
      "name": "Example Lookup",
      "version": "1.0.0",
      "min_tater_version": "187",
      "description": "Look up an example value.",
      "portals": ["webui", "macos", "voice_core"],
      "notifier": false,
      "settings_category": "Example Lookup",
      "tags": ["example"],
      "entry": "verba/example_lookup.py",
      "sha256": "generated-file-sha256"
    }
  ]
}
```

### Portal

```json
{
  "schema": 1,
  "name": "Example Portal Repository",
  "portals": [
    {
      "id": "example",
      "name": "Example",
      "module_key": "example_portal",
      "version": "1.0.0",
      "min_tater_version": "187",
      "description": "Connect Example Chat to Tater.",
      "settings_category": "Example Portal Settings",
      "required_settings_count": 2,
      "autostart_key": "example_portal_running",
      "tags": ["chat"],
      "entry": "portals/example_portal.py",
      "sha256": "generated-file-sha256"
    }
  ]
}
```

### Core

```json
{
  "schema": 1,
  "name": "Example Core Repository",
  "cores": [
    {
      "id": "example",
      "name": "Example Core",
      "module_key": "example_core",
      "version": "1.0.0",
      "min_tater_version": "187",
      "description": "Provide Example Core services to Tater.",
      "settings_category": "Example Core Settings",
      "required_settings_count": 1,
      "autostart_key": "example_core_running",
      "tags": ["example"],
      "entry": "cores/example_core.py",
      "sha256": "generated-file-sha256"
    }
  ]
}
```

The optional top-level `name`, `title`, `shop_name`, or `repo_name` gives Tater a source label when the user did not enter one.

## Generate manifests

The Tater Shop generators use only Python's standard library and support another repository root:

```bash
python3 /path/to/Tater_Shop/tools/generate_manifest.py \
  --root /path/to/my-tater-extension \
  --name "My Verba Repository"

python3 /path/to/Tater_Shop/tools/generate_portal_manifest.py \
  --root /path/to/my-tater-extension \
  --name "My Portal Repository"

python3 /path/to/Tater_Shop/tools/generate_core_manifest.py \
  --root /path/to/my-tater-extension \
  --name "My Core Repository"
```

Run only the generators needed by the repository. The generator reads literal module metadata without importing or executing the extension and writes the relevant manifest at the supplied root.

The generator owns fields derived from source, including the package ID, name, version, descriptions, settings summary, `entry`, and `sha256`. Regenerate after every source change.

## Validate before publishing

```bash
python3 /path/to/Tater_Shop/tools/validate_package.py \
  --root /path/to/my-tater-extension
```

Use `--kind verba`, `--kind portal`, or `--kind core` to validate only one manifest. Validation is static: it checks structure, source paths, checksums, syntax, filenames, and required contract declarations without executing downloaded code.

The validator cannot prove that provider calls, UI actions, or every platform handler works. Run the extension in a test Tater installation as well.

## IDs and precedence

- Verba IDs use lowercase `snake_case` and normally match `verba/<id>.py`.
- Portal ID `example` maps to `portals/example_portal.py` and module key `example_portal`.
- Core ID `example` maps to `cores/example_core.py` and module key `example_core`.
- IDs are global within each extension type and should remain stable forever.
- Tater merges configured repositories in order and keeps the first item for each ID. The built-in Tater Shop is configured first, so a third-party package should not reuse an official ID.

## Single-file and dependency policy

The installer downloads exactly the `entry` file. It does not install:

- `requirements.txt` or Python packages.
- Neighboring Python modules.
- Static images, model files, or configuration directories.
- Git submodules or release archives.

Embed small defaults in the extension file and use stable Tater APIs for shared services. If an optional library may not exist, guard its import and report a clear unavailable state. Do not download and execute Python dependencies at runtime.

## Versions and compatibility

Use semantic versions such as `1.2.0`. Tater offers an update only when the catalog version compares higher than the installed extension version. Changing code or a checksum without increasing the version will not produce the expected update prompt.

`min_tater_version` is currently advisory catalog metadata. It uses the oldest compatible Tater build identifier, historically values such as `164` or `187`, rather than a public version such as `1.2.3`. Set it to the oldest build you actually tested. Current clients do not block installation using this value, so extensions must still fail gracefully when an API is unavailable.

## Checksums and trust

Always publish `sha256`. Tater verifies the downloaded bytes before replacing an installed extension. A checksum prevents accidental mismatch; it does not make untrusted code safe.

Extensions execute inside Tater and are not sandboxed. They may inherit access to Redis, local files available to Tater, configured integrations, and the network. Repository owners should review dependencies, avoid runtime code downloads, never collect unrelated secrets, and explain any external data transmission in their README.

## Test a repository in Tater

1. Push the extension source and regenerated manifest.
2. Open Tater's matching **Verbas**, **Portals**, or **Cores** section.
3. Open **Repositories**.
4. Add the raw manifest URL and an optional source name.
5. Return to the Store and install the extension.
6. Configure it, start it when applicable, and exercise every supported action.
7. Bump the extension version, regenerate, push again, and verify Tater reports the update.
8. Remove the extension and verify its runtime stops and its owned data can be removed safely.

## Request trusted-repository listing

Trusted repositories are directory entries in this repository:

- `verba_repositories.json`
- `portal_repositories.json`
- `core_repositories.json`

A proposed entry should look like:

```json
{
  "id": "example-my-tater-core",
  "name": "My Tater Core",
  "repository": "My-Tater-Core",
  "description": "A concise description of what the repository adds.",
  "author": {
    "name": "example",
    "url": "https://github.com/example"
  },
  "manifest_url": "https://raw.githubusercontent.com/example/My-Tater-Core/main/core_manifest.json",
  "homepage": "https://github.com/example/My-Tater-Core",
  "tags": ["example"]
}
```

Before requesting inclusion, make the repository public, document configuration and external services, publish valid checksums, test installation and update behavior, and keep a stable default branch and raw manifest URL.
