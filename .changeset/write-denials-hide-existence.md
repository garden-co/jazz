---
"jazz-tools": patch
---

Write denials are consistent wherever they are decided, and reveal less about whether a row exists. An update or delete a session may not make is refused with the same message whether the row is hidden from the session, deleted while hidden from it, or doesn't exist, on the server and on a backend. An upsert can't hide this fully: on a row that doesn't exist it inserts, so it is refused only where the row exists. On a backend, a hidden row the backend already holds is refused at once, while a row it doesn't hold is refused after asking the server, so the time to the answer can still differ. A write denied at once, as it is staged (for example an update of a row the session may not read), now throws an error with `code: "permission_denied"`, the code a rejected `wait()` carries, so one check handles both. `TestDb.expectDenied` now counts a write denied at once, and fails with "expected the write to be denied, but it was accepted" when it isn't denied.
