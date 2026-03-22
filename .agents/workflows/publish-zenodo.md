---
description: Automatically publish the repository codebase to Zenodo when a new tag is cut
---

# Publish to Zenodo

This workflow maps out how to construct and verify the `Zenodo` Data Object Identifier (DOI) publishing mechanism.

1. Ensure your repository root has a `.zenodo.json` file (optional, but highly recommended) storing your specific JSON schema metadata (title, creators, descriptions).
2. Obtain a Personal Access Token from Zenodo's Developer Settings.
3. Inject the token into your GitHub Repository Secrets specifically under the exact key `ZENODO_ACCESS_TOKEN`.
4. Trigger the deployment natively via standard git tagging:
// turbo
5. `git tag v1.0.0 && git push origin v1.0.0`

6. The `.github/workflows/publish-zenodo.yml` pipeline will automatically spin up, zip the current source code representing the tag, and push it directly up to Zenodo to formally mint the DOI.

**Alternative**: Although GitHub Actions provide immense logging metrics and fail-safes, you can alternatively navigate directly to the Zenodo Web UI, link your GitHub account natively, and flick the repository toggle to 'On'. The native Zenodo Webhooks mimic the same response mechanism whenever you generate a GitHub Release.
