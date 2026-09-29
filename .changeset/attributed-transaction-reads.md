---
"jazz-tools": patch
---

Reads inside transactions on a `Db` from `withAttribution` or `withAttributionForRequest` now use the backend authority that the attributed `Db` reads and writes with. They no longer fail with "open transaction identity does not match its bound identity", so the invite-link recipe works as written.
