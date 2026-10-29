---
"jazz-tools": patch
---

Reads over many rows whose relation-backed read policy refers to the row only by its id (an access edge naming the row, possibly reached through a recursive group closure) now evaluate that policy once per read instead of once per row. A first sync over many rows inheriting such a policy through their parent reads several times faster on the server. Write checks and rechecks of a few rows keep the per-row form, which seeks only that row's grants.
