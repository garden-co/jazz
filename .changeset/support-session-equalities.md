---
"jazz-tools": patch
---

Sync a membership check's supporting rows by the reader too: a read policy's `EXISTS` with `column = session.user` (or `session.identity.subject` / `.issuer`) now ships and watches only the reader's own matching row, not every member's, so first loads no longer grow with the number of members.
