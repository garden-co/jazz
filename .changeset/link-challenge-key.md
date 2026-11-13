---
"jazz-tools": patch
---

Sign link challenges with a key that is secret to the server on every store. With `vercel()`, and with `remote()` reached through a backend credential, they were signed with a key not secret to the server. Now each instance uses a key of its own there, and `remote({ secret })` derives it from the app's secret.
