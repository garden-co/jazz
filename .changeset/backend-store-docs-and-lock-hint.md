---
"jazz-tools": patch
"jazz-napi": patch
---

Clearer backend store docs and errors, from adopter feedback. `vercel()` now says first that it is meant for dev too (outside a deployment it runs `local`, by default `sqlite({ path: ".jazz/local" })`); `sqlite()` states its one-process lock (`<path>/jazz.lock`) and its refusal on serverless hosts; and `Jazz.next()` shows the catch-all route file. On Vercel the serverless refusal points to `vercel()`.
