---
"jazz-tools": patch
---

- Add `durableObjects({ name, credentials, secret })` to provision and connect to a persistent Cloudflare Durable Object core through `createJazz({ store })`.
- Support `createJazz`, `remote`, and `durableObjects` in Cloudflare Workers through the shared `jazz-tools/backend` import.
- Derive history's `$acceptedAt` from authority acceptance time, with deterministic ordering for versions accepted within the same millisecond.
