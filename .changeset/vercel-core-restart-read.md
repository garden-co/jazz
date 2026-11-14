---
"jazz-tools": patch
---

`vercel()` and `remote()`: the backend's own connection to the core opens only once the core's side has. On `vercel()`, a connection that fails before it opens (the core is stopping or restarting, or Vercel's router refuses the upgrade) is tried again once the core is awake, within the wake deadline. A backend read made while the core restarts, such as an auth adapter's during sign-in, now waits for the core instead of failing with `websocket closed (code=1006, reason=connection to the core failed)`.
