---
"jazz-tools": patch
---

Fix a Better Auth race condition where a slow `/get-session` request returning `null` after a successful sign-in would incorrectly log the user out. The client plugin now tracks an `authGeneration` counter, attaches it to `/get-session` requests via an `x-jazz-auth-generation` header, and ignores stale null responses whose generation no longer matches the current one.
