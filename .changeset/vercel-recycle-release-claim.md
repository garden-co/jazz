---
"jazz-tools": patch
---

`vercel()` gives back a session's recycle when Vercel refuses to stop it, so the next waiter can stop it instead of every waiter waiting out its deadline.
