---
"jazz-tools": patch
---

Mark CoValue migration failures as unavailable instead of throwing behavior.

When a migration throws (for example, async migrations or write attempts without permissions), loading now resolves to a CoValue with `$jazz.loadingState === "unavailable"`.
