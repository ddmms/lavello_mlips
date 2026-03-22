---
description: Automatically build and verify Sphinx documentation for GitHub Pages
---

# Publish Documentation to GitHub Pages

This workflow allows the AI/User to natively test the Sphinx documentation compilation pipeline locally on the machine before pushing to the `main` branch, ensuring the GitHub Action executes flawlessly.

1. Install the main package and all necessary documentation rendering engines.
// turbo
2. `micromamba run -n janus pip install -e . sphinx sphinx-material`

3. Verify the Sphinx HTML documentation compiles cleanly.
// turbo
4. `cd docs && make html`

5. **Deployment Warning**: The remote deployment to GitHub Pages is handled 100% autonomously by the `.github/workflows/gh-pages.yml` Action attached to this repository. You do not need to manually push the `gh-pages` branch unless the CI token is broken.
