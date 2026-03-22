---
description: Automatically publish the repository codebase to Zenodo when a new tag is cut
---

# Publish to Zenodo

This workflow maps out how to construct and verify the `Zenodo` Data Object Identifier (DOI) publishing mechanism using the Janus Core architecture.

1. Ensure your repository root has a `.zenodo.json` file (optional, but highly recommended) storing your specific JSON schema metadata.
2. Navigate directly to the Zenodo Web UI, link your GitHub account natively, and flick the repository toggle to 'On'.
3. Trigger the deployment natively via standard git tagging:
// turbo
4. `git tag v1.0.0 && git push origin v1.0.0`

5. The `.github/workflows/publish-zenodo.yml` pipeline will automatically spin up, instantiate `uv`, ensure the `pyproject.toml` version explicitly matches your tag using `uvx toml`, and finally invoke `ncipollo/release-action`.
6. This officially mints a GitHub Release. Because you flicked the Zenodo toggle in Step 2, Zenodo's native Webhooks will securely intercept the GitHub Release event and automatically mint your DOI! This approach guarantees that GitHub Releases and Zenodo DOIs are perfectly structurally paired.
