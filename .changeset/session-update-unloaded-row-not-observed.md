---
"jazz-tools": patch
---

A backend `update()` inside `forReqUser`/`forSession` no longer needs the row to be loaded first. On a row this backend instance hasn't loaded yet (for example right after a restart, or on another instance), it reads the row through the caller's session (waiting up to 10 seconds for the server) and then stages the update, instead of failing with "read policy denied UPDATE ... requires read permission on the target row". When the row's read policy depends on other rows (for example `exists` on a membership table), the backend leaves the decision to Jazz Cloud, as it already did for upserts: the update shows to every session on that backend until Cloud answers, its `wait()` rejects if Cloud refuses it, and a write another session builds on it meanwhile is rejected with it. A row the session can't read, or that doesn't exist, is refused with "does not exist or this session cannot read it".
