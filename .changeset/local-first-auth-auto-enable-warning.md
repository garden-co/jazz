---
"jazz-tools": patch
---

The server now logs a prominent warning when local-first auth is silently auto-enabled because `NODE_ENV` is not set to `"production"`. Deployments that forget to set `NODE_ENV=production` will see the warning rather than running wide open with no indication.
