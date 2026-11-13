---
"jazz-tools": patch
---

When your backend's `authenticate` throws or rejects (for example Better Auth without `BETTER_AUTH_SECRET`), a Jazz route now answers `500 authenticate_failed` (`Cache-Control: no-store`) and logs `createJazz: your authenticate threw…` with your error, once per request, instead of rejecting the request handler. On Vercel that rejection surfaced as an `unhandledRejection` that crashed the function. A sync socket whose sign-in hits a throwing `authenticate` logs the same error and closes so the client retries. `forReqUser`, `withAttribution` and `accountFor` still reject with your error, unchanged.
