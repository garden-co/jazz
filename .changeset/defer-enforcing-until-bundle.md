---
"jazz-tools": patch
---

Fix a permissions race where a permissions head arriving before its bundle would flip the client into Enforcing mode without an authorization schema, causing local writes against every table to fail with `policy denied` until the bundle arrived. The mode now only flips when the bundle is applied.
