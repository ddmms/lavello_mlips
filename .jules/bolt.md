## 2024-03-25 - Pre-compiling Regex in performance-critical loops
**Learning:** Initializing `re` matches inside loops without pre-compiling adds significant overhead. Profiling regex performance specifically in `parse_charge_mult` showed that dynamic matching creates a ~1.5x-2x performance bottleneck over 100k invocations compared to `re.compile()` at the module level.
**Action:** Always extract regex expressions into pre-compiled module-level constants (e.g., `RE_CHARGE`, `RE_XYZ`) instead of defining them inline, especially in frequently called parsing loops.

## 2024-05-18 - Replacing `json` with `orjson` for large datasets
**Learning:** In pipelines handling large datasets via dictionaries containing metadata (e.g. millions of prefixes), `json.dump` and `json.load` can become significant bottlenecks, adding seconds or even minutes to startup and checkpointing phases. `orjson` provides a near drop-in replacement that is 4-10x faster for such operations.
**Action:** When working with large JSON files, especially in a framework requiring frequent disk checkpoints, replace Python's built-in `json` module with `orjson` wrapping `loads`/`dumps` to preserve API compatibility while gaining massive performance boosts.
## 2024-03-29 - ASE Custom JSON encoding vs standard JSON
**Learning:** ASE's custom JSON encoder (`ase.io.jsonio.encode`) will generate dicts with special keys like `__ndarray__` or `__complex__` (e.g. `{"__ndarray__": [[5], "int64", ...]}`). When optimizing JSON deserialization using faster alternatives like `orjson`, it's critical to realize that a normal `json.loads` or `orjson.loads` will deserialize this into a Python dictionary, while ASE's custom `decode` will properly reconstruct the underlying numpy array. Bypassing ASE's decoder without checking for these keys leads to downstream type errors (e.g. `KeyError: '__ndarray__'`).
**Action:** When replacing or wrapping ASE's jsonio with `orjson`, always fall back to ASE's `decode` if the payload string contains `__ndarray__` or `__complex__` markers, to ensure custom objects are correctly reconstructed.

## 2024-05-19 - Bypassing Regex with Explicit Substring Checks
**Learning:** In text parsing of heavily repeated files (e.g., Orca output processing where `RE_COLS.search` iterates over every line), calling a pre-compiled regular expression engine on lines that mostly don't match creates unnecessary overhead. Adding an explicit, fast string literal check (like `if "E(eV)" in line and RE_COLS.search(line)`) skips regex evaluation for non-relevant lines and significantly boosts performance (up to ~50x speedup in cold paths).
**Action:** Always combine regex searches in tight loops with simple substring checks (`in`) when possible to quickly bypass lines that will never match.

## 2024-05-19 - Memory Allocation in List Comprehensions inside Tight Loops
**Learning:** Constructing list comprehensions purely for condition checking inside loops (like finding occupied orbitals via `[i for i, o in enumerate(occs) if o > thr]`) introduces heavy memory allocation and garbage collection overhead, especially when parsing large datasets. Replacing them with direct iteration (O(n)) that tracks max/min indices natively without building auxiliary lists drops runtime significantly.
**Action:** When searching for extrema (min/max) based on conditions in performance-critical code paths, avoid list comprehensions. Prefer manual O(1) state variables across an `enumerate()` loop with early breaking where possible.

## 2024-05-19 - String Hashing in Loops
**Learning:** Performing `"".encode()` and `.update()` iteratively inside a loop when computing a hash (e.g., SHA1 for structures) invokes high method call overhead in Python. Joining a generator expression into a single string (e.g., `s = "".join(...)`) and doing a single `.encode("ascii")` and `.update()` or hashing operation provides measurable performance gains.
**Action:** Consolidate string building using `"".join()` instead of iteratively `.update()`-ing a hash state when dealing with smaller inputs like molecular coordinates.
