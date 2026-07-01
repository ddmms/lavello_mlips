## 2024-07-01 - [Optimizing Pandas DataFrame Iteration]
**Learning:** Iterating over large Pandas DataFrames using `df.iterrows()` is a significant performance bottleneck due to the overhead of wrapping each row in a `Series` object and converting types.
**Action:** When iterating over rows in a DataFrame, convert it to a list of dictionaries first using `df.to_dict('records')`. Remember to treat the resulting row as a standard Python dictionary in downstream code, avoiding methods like `row.to_dict()` that were previously used on `Series` objects.
