# Verba starter

Copy this folder's `verba/` directory into a new repository, then rename `example_lookup.py` and change the class metadata and behavior.

From this template directory, generate and validate its manifest with the Tater Shop tools:

```bash
python3 ../../tools/generate_manifest.py --root . --name "Example Verba Repository"
python3 ../../tools/validate_package.py --root . --kind verba
```

When using the template outside this repository, replace `../../tools/` with the path to a Tater Shop checkout or copy the two standard-library-only tools into your repository.

Read [Build a Verba](https://github.com/TaterTotterson/Tater_Shop/blob/main/docs/verba-authoring.md) and [Repository manifests and publishing](https://github.com/TaterTotterson/Tater_Shop/blob/main/docs/repository-manifests.md) before publishing.
