---
"jazz-tools": patch
---

A Global write wait in a browser tab running over its worker no longer rejects when the sync server refuses a renewable credential (an expired sign-in) or the link has an outage. It stays pending and resolves once the write reaches the server after re-login or reconnect, as documented. Before, it failed after about 60 ms with "session missing: not signed in" while the write stayed queued, so an app that retried wrote it twice. Remote reads still fail fast.
