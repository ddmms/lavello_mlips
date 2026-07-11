## 2024-06-11 - [Optimize dataframe iteration]
**Learning:** Iterating over large Pandas DataFrames using `df.iterrows()` is extremely slow because it creates a new Series object for each row.
**Action:** Replace `df.iterrows()` with `df.to_dict('records')` when a dictionary representation of the rows is needed. This converts the dataframe to native Python dictionaries at the C level, yielding significant performance gains.
