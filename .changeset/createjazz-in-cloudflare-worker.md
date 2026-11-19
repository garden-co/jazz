---
"jazz-tools": patch
---

`createJazz` runs in a Cloudflare Worker (experimental): under the `workerd` condition, `jazz-tools/backend` runs on jazz-wasm instead of jazz-napi. `jazzBackend(...)` makes a Durable Object class that runs your `createJazz` and your own routes, `routeToJazzBackend` forwards a Worker's requests to it, and `durableObjects({ binding: env.JAZZ, secret })` reaches a `JazzCore` Durable Object in the same Worker. See "Run your backend in the Worker too" on the Cloudflare Durable Objects page.

Backend requests to a core (`remote()`, `vercel()`, `durableObjects()`) refuse a redirect with `the Jazz core at <url> answered with a redirect` instead of fetch's own error: they use `redirect: "manual"` now, since Cloudflare Workers don't support `"error"`. They still never follow one.
