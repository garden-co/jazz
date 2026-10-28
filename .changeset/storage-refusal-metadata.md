---
"jazz-tools": patch
---

Preserve optional storage-format identifiers when rebuilding storage refusal
errors from bindings or worker messages. Keep the original error unchanged when
its codec lists are incomplete or its format identifier is malformed.
