---
"jazz-tools": patch
---

Fix includes ordered (or sliced) by a column that is not selected. Such reads previously failed locally with `graph output descriptors do not match`, and remote reads and subscriptions were rejected by the server and never settled.
