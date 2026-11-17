---
"jazz-tools": patch
---

New store, `durableObjects({ url, appId, secret })`: `createJazz` reaches a Jazz core running in a Cloudflare Durable Object, in a Worker you deploy, the way `remote()` reaches a sync server. `secret` is the Worker's `JAZZ_CORE_SECRET` (at least 32 characters, the same across deployments); browsers keep talking only to your backend. Experimental.
