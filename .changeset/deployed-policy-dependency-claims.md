---
"jazz-tools": patch
---

Fix inserts rejected with a server transport error under permissions published with `deploy` when the insert check reads a table whose read policy compares a session claim.
