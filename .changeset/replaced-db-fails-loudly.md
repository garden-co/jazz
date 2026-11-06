---
"jazz-tools": patch
---

A `Db` that its session replaced (the background upgrade to the local-first client, a sign-in, sign-out or account switch) now ends its live queries with a `DbReplacedError` and throws it on writes, `subscribe` and `onMutationError`, instead of going silent. The error says to read the current `Db` from `session.getSnapshot().client` inside `session.subscribe(...)`. Framework providers already follow the session and are unaffected. The Local-First Client docs show the pattern for plain TypeScript.
