---
"jazz-tools": patch
"jazz-napi": patch
---

Clearer backend store docs and errors, from adopter feedback. `vercel()` now says first that it is meant for dev too (outside a deployment it runs `local`, by default `sqlite({ path: ".jazz/local" })`); `sqlite()` states its one-process lock (`<path>/jazz.lock`) and its refusal on serverless hosts; `createJazz` says to create it once per process on the first request, cached on `globalThis`; and `Jazz.next()` shows the catch-all route file. A second core on a sqlite directory now fails with a hint at the usual cause (a build or dev worker importing a module that calls `createJazz` at module scope), and on Vercel the serverless refusal points to `vercel()`.
