## 2024-03-25 - Pre-compiling Regex in performance-critical loops
**Learning:** Initializing `re` matches inside loops without pre-compiling adds significant overhead. Profiling regex performance specifically in `parse_charge_mult` showed that dynamic matching creates a ~1.5x-2x performance bottleneck over 100k invocations compared to `re.compile()` at the module level.
**Action:** Always extract regex expressions into pre-compiled module-level constants (e.g., `RE_CHARGE`, `RE_XYZ`) instead of defining them inline, especially in frequently called parsing loops.

## 2024-05-18 - Replacing `json` with `orjson` for large datasets
**Learning:** In pipelines handling large datasets via dictionaries containing metadata (e.g. millions of prefixes), `json.dump` and `json.load` can become significant bottlenecks, adding seconds or even minutes to startup and checkpointing phases. `orjson` provides a near drop-in replacement that is 4-10x faster for such operations.
**Action:** When working with large JSON files, especially in a framework requiring frequent disk checkpoints, replace Python's built-in `json` module with `orjson` wrapping `loads`/`dumps` to preserve API compatibility while gaining massive performance boosts.
## 2024-03-29 - ASE Custom JSON encoding vs standard JSON
**Learning:** ASE's custom JSON encoder (`ase.io.jsonio.encode`) will generate dicts with special keys like `__ndarray__` or `__complex__` (e.g. `{"__ndarray__": [[5], "int64", ...]}`). When optimizing JSON deserialization using faster alternatives like `orjson`, it's critical to realize that a normal `json.loads` or `orjson.loads` will deserialize this into a Python dictionary, while ASE's custom `decode` will properly reconstruct the underlying numpy array. Bypassing ASE's decoder without checking for these keys leads to downstream type errors (e.g. `KeyError: '__ndarray__'`).
**Action:** When replacing or wrapping ASE's jsonio with `orjson`, always fall back to ASE's `decode` if the payload string contains `__ndarray__` or `__complex__` markers, to ensure custom objects are correctly reconstructed.

## 2024-05-18 - Replacing iterative `hashlib.update` with string `join`
**Learning:** Repetitively calling `.update()` and `.encode()` in a loop inside a hashing function (e.g., `geom_sha1`) introduces Python/C boundary overhead and is slower than generating the final string via list/generator comprehension, joining it with `"".join()`, and performing a single `.encode("ascii")` and `.update()`.
**Action:** Always prefer to build the complete string first and encode/hash it once rather than iteratively updating the hash object in a loop.

## 2024-05-18 - Fast paths for text blocks
**Learning:** Running `re.search` or string operations like `txt.splitlines()` over large text files without matches causes noticeable performance drops. Fast path checks using `if "Keyword" not in txt` provide instant short-circuits.
**Action:** When parsing large text blocks, check if critical string literal keywords required for a match are present in the text before running expensive operations.
