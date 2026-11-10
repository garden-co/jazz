---
"jazz-tools": patch
---

`createJazz` re-checks of open sync sessions now fail closed: when `authenticate` throws on three re-checks in a row, the session ends as a refusal does and the client reconnects, so a sign-out still reaches it. The first failed re-check of a session is logged with what a re-check is and, for errors like Next.js's "called outside a request scope", which helper to avoid. The `authenticate` and `recheckSeconds` docs now say that re-checks run outside your framework's request scope with the original request.
