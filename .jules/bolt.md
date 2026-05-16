## 2024-05-16 - [Fast Text Parsing in Omol25 Data Processing]
**Learning:** Performing `re.finditer` or `re.search` over large text blocks (e.g. combined string dumps of multiple output files like Orca outputs) adds huge overhead if the target properties (dipole, quadrupole, eigens, charge/mult) don't even exist in the text.
**Action:** Adding a fast-path string literal check via Python's `in` operator (e.g. `if "E(Eh)" not in txt or "E(eV)" not in txt: return None`) before running the regex drastically drops parsing times from seconds to milliseconds for texts missing the targets.

## 2024-05-16 - [Fast Text Parsing in Omol25 Data Processing]
**Learning:** Performing `re.finditer` or `re.search` over large text blocks (e.g. combined string dumps of multiple output files like Orca outputs) adds huge overhead if the target properties (dipole, quadrupole, eigens, charge/mult) don't even exist in the text.
**Action:** Adding a fast-path string literal check via Python's `in` operator (e.g. `if "E(Eh)" not in txt or "E(eV)" not in txt: return None`) before running the regex drastically drops parsing times from seconds to milliseconds for texts missing the targets.
