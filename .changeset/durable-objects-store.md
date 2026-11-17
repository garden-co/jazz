---
"jazz-tools": patch
---

New store, `durableObjects({ url, secret })` (experimental): `createJazz` reaches a Jazz core running in a Cloudflare Durable Object, in a Worker you deploy, the way `remote()` reaches a sync server. `secret` is the Worker's `JAZZ_CORE_SECRET` (at least 32 characters, the same across deployments); the core keeps its own app id, and browsers keep talking only to your backend. `url` must be `https://`, except for localhost.
