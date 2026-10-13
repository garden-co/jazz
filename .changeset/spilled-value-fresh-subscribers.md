---
"jazz-tools": patch
---

Fix fresh subscriptions that never delivered a first result once the table held a row with a value over 64 KiB. The client no longer holds its sync turn open for large-value chunks that only that turn can request, and it waits for those chunks before reporting a settled result.
