## 2024-03-21 - Optimize Pandas DataFrame Iteration
**Learning:** Iterating over large Pandas DataFrames using `df.iterrows()` is extremely slow. Converting the DataFrame to a list of dicts via `df.to_dict('records')` before iterating avoids immense overhead.
**Action:** Always prefer `df.to_dict('records')` when dictionary representations of row items are needed iteratively. Remember to update downstream logic to handle plain Python dictionaries instead of Pandas Series (e.g. replacing `row.to_dict()` with `dict(row)` or just `row`).
