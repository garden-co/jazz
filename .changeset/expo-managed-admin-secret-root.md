---
"jazz-tools": patch
---

Honor the root `adminSecret` option in `ManagedDevRuntime` (used by the `vite`, `next`, `sveltekit`, and `expo` dev plugins). The managed local-server branch previously read only `server.adminSecret` and silently fell back to a random `jazz-dev-XXXXXXXX` when the root option was set; it now falls through to root `adminSecret`, mirroring the precedence already used for `appId`. The startup banner also surfaces the resolved admin secret, and the Expo plugin now logs the inspector link the same way `vite` and `next` do.
