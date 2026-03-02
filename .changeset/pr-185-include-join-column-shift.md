---
"jazz-tools": patch
---

Fixed array subquery incremental updates so parent row fields stay correct. Previously, when related rows changed after subscribing, update payloads could return corrupted parent values (for example, garbled `id` or `name`).
