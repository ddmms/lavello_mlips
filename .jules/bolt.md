## 2024-03-25 - Pre-compiling Regex in performance-critical loops
**Learning:** Initializing `re` matches inside loops without pre-compiling adds significant overhead. Profiling regex performance specifically in `parse_charge_mult` showed that dynamic matching creates a ~1.5x-2x performance bottleneck over 100k invocations compared to `re.compile()` at the module level.
**Action:** Always extract regex expressions into pre-compiled module-level constants (e.g., `RE_CHARGE`, `RE_XYZ`) instead of defining them inline, especially in frequently called parsing loops.

## 2024-05-18 - Replacing `json` with `orjson` for large datasets
**Learning:** In pipelines handling large datasets via dictionaries containing metadata (e.g. millions of prefixes), `json.dump` and `json.load` can become significant bottlenecks, adding seconds or even minutes to startup and checkpointing phases. `orjson` provides a near drop-in replacement that is 4-10x faster for such operations.
**Action:** When working with large JSON files, especially in a framework requiring frequent disk checkpoints, replace Python's built-in `json` module with `orjson` wrapping `loads`/`dumps` to preserve API compatibility while gaining massive performance boosts.
## 2024-03-29 - ASE Custom JSON encoding vs standard JSON
**Learning:** ASE's custom JSON encoder (`ase.io.jsonio.encode`) will generate dicts with special keys like `__ndarray__` or `__complex__` (e.g. `{"__ndarray__": [[5], "int64", ...]}`). When optimizing JSON deserialization using faster alternatives like `orjson`, it's critical to realize that a normal `json.loads` or `orjson.loads` will deserialize this into a Python dictionary, while ASE's custom `decode` will properly reconstruct the underlying numpy array. Bypassing ASE's decoder without checking for these keys leads to downstream type errors (e.g. `KeyError: '__ndarray__'`).
**Action:** When replacing or wrapping ASE's jsonio with `orjson`, always fall back to ASE's `decode` if the payload string contains `__ndarray__` or `__complex__` markers, to ensure custom objects are correctly reconstructed.

## 2024-05-18 - Replacing O(N) list comprehensions with direct iterations
**Learning:** Finding the lowest unoccupied or highest occupied orbital indices using list comprehensions like `occ_idx = [i for i, o in enumerate(occs) if o is not None and o > thr]` is mathematically O(N) but adds significant overhead due to Python memory allocation and list construction. Profiling showed that for small `occ` arrays (1000 items), directly using a `for` loop with early `break` statements cuts execution time by over 80%.
**Action:** When calculating index boundaries for large arrays (checking limits or finding highest/lowest values), avoid using O(N) list comprehensions. Instead, iterate directly (forwards or backwards) using early `break` statements.

## 2024-05-18 - Avoid repeated hashing of iterative string conversions
**Learning:** In string-based molecular hashing (like `geom_sha1`), performing an iterative `for` loop that calls `.encode()` and `.update()` on smaller substrings incurs considerable Python overhead.
**Action:** For repetitive string-based hashing, use `"".join()` with a generator expression to build a single string and perform a single `.encode("ascii")` and hash operation. It avoids iterative string allocation overhead and lets the underlying C functions run on the complete block.
