---
"jazz-tools": patch
---

Commits, ingest validation and row reads no longer copy a table's schema, including its policy definitions, for each row they touch. Results are unchanged.
