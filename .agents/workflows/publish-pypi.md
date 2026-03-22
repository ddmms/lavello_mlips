---
description: Build and publish the process_omol25 distribution package to PyPI manually
---

# Publish to PyPI

This workflow dictates the native process to manually formulate the distribution tarballs and binary wheels and push them up to the Python Package Index, verifying the steps autonomous CI undertakes.

1. Ensure the `uv` toolchain is globally installed.
// turbo
2. `curl -LsSf https://astral.sh/uv/install.sh | sh`

3. Compile the Python package from source using `pyproject.toml`.
// turbo
4. `uv build`

5. Once built, the `/dist` directory will contain the `.tar.gz` and `.whl` files. Publish them to PyPI natively utilizing `uv publish`. (Note: This will prompt for your PyPI API token).
6. `uv publish dist/*`

**Warning**: The `.github/workflows/publish-pypi.yml` handles this 100% autonomously whenever a new GitHub Release is fully published via UI. Manual uv publish interaction is purely for fallback or TestPyPI staging.
