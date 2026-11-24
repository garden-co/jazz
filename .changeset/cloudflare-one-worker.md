---
"jazz-tools": patch
"jazz-wasm": patch
---

Cloudflare: run your backend and its Jazz core in one Worker with `jazzWorker` (from the new `jazz-tools/cloudflare`), two Durable Object bindings and no secrets. `durableObjects({ binding: env.JAZZ })` no longer needs `secret`: without `JAZZ_CORE_SECRET`, the core makes its own secret on its first start and hands it to the backend over Workers RPC, which only code holding the binding can call. A Worker that sets `JAZZ_CORE_SECRET` keeps using it. The Cloudflare docs page now shows the one-Worker setup.
