---
"jazz-tools": patch
---

Keep a failed client shutdown as the persistent-store closing barrier so later client creation cannot overlap a runtime that did not shut down.
