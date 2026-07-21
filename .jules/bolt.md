## 2025-02-18 - Avoid Repository Pollution During Profiling
**Learning:** Running local performance benchmarks and the pytest suite in this specific codebase generates a massive amount of tracked file modifications (`.aselmdb-lock` files) and untracked binary data directories (`mock_s3_data_*`). Committing these pollutes the PR and will block code reviews.
**Action:** Always explicitly verify `git status` and use `git clean -fd` alongside `git restore` on specific test artifact directories to thoroughly clean the workspace prior to requesting review or committing, ensuring only the target source file modifications are included in the patch.

## 2025-02-18 - Pandas Dataframe Iteration Anti-Pattern
**Learning:** Using `df.iterrows()` to iterate over large Pandas DataFrames creates significant bottlenecks by constructing a new Pandas Series object for every single row. In profiling, this took ~21 seconds for 100k rows.
**Action:** Always refactor `df.iterrows()` to `df.to_dict('records')` when processing rows as native dictionaries. This simple change yields a massive performance improvement (e.g., ~17x speedup, reducing 21s to 1.2s). Update any downstream attribute accesses from the Series object (e.g., removing `.to_dict()` calls on the row variables).
