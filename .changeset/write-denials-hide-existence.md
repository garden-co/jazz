---
"jazz-tools": patch
---

Write denials are consistent wherever they are decided, and no longer reveal whether a row exists. An update, upsert or delete a session may not make is refused the same way whether the row is hidden from the session or doesn't exist, on the server and in thin mode. A write denied at once, as it is staged (for example an update of a row the session may not read), now throws an error with `code: "permission_denied"`, the code a rejected `wait()` carries, so one check handles both. `TestDb.expectDenied` now counts a write denied at once, and fails with "expected the write to be denied, but it was accepted" when it isn't denied.
