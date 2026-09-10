---
"jazz-tools": patch
---

Reduce transaction-status polling cost by reading fate and durability fields without decoding unrelated transaction payloads. Preserve pending-persistence and rejection checks.
