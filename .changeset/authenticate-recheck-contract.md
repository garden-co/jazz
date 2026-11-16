---
"jazz-tools": patch
---

`createJazz` re-checks of open sync sessions now fail closed. A re-check whose `authenticate` throws a request-scope error (Next.js's "called outside a request scope", from `cookies()`, `withAuth()`, `auth()`) ends the session at once and logs an error; other errors end it after three failed re-checks in a row. The client then reconnects, so a sign-out still reaches the session. What a re-check may rely on is explained once in the log, and in the `authenticate` and `recheckSeconds` docs: re-checks run outside your framework's request scope, with the original request.
