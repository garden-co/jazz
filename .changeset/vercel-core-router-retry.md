---
"jazz-tools": patch
---

`vercel()` store: a request that reaches a core while it stops or restarts no longer fails. When Vercel's router answers for the Sandbox itself (`x-vercel-error`, e.g. `SANDBOX_NOT_LISTENING`), the request never reached the core: the backend wakes the core (health check included) and sends it once more. This covers the backend's own requests too, such as the schema fetch while the store opens. A dropped connection to the core makes the next request check the core again, instead of trusting it for up to 10 s. The "no Blob store is connected" warning is now logged once per process, not once per Next.js route that opens Jazz.
