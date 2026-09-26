---
"jazz-tools": patch
---

Each runtime tick no longer scans all cached evaluation and operator state, so a small write stays cheap as the number of live queries and cached results grows. Results are unchanged.
