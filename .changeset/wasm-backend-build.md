---
"jazz-wasm": patch
---

The browser build no longer carries what only a backend's `createJazz` uses (about 300 KB gzip less for every browser). A backend in a Cloudflare Worker now runs on jazz-wasm's own backend build, `jazz-wasm/backend`, which `jazz-tools/backend` picks under the `workerd` condition: nothing changes for apps.
