## 2025-02-20 - Pandas DataFrame Iteration Bottleneck
**Learning:** Iterating over large Pandas DataFrames using `df.iterrows()` is extremely slow because it constructs a new Series object for every single row.
**Action:** When row-level iteration over a DataFrame is necessary, always convert the entire DataFrame to a list of standard native dictionaries first using `records = df.to_dict('records')`. This avoids Pandas object creation overhead in the loop, providing dramatic (often 10x+) speed improvements. Remember to update any row-level downstream code to expect a standard dictionary instead of a Series.

## 2025-02-20 - Ensure Test Artifacts are Not Committed
**Learning:** Some test suites produce temporary data files, directories, locks (like `.aselmdb-lock`), and cache files during execution.
**Action:** Before committing, always use `git status` to verify that untracked or modified test artifacts have not been accidentally staged. Ensure any artifacts not managed by standard tools like pytest (which handles its own `.pytest_cache`) are cleaned up properly (e.g. `git clean -fd`, `git restore <files>`) to prevent repository pollution.
