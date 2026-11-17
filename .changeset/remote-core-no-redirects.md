---
"jazz-tools": patch
---

Don't follow redirects for backend core requests: an HTTP request from a `remote()` or `vercel()` backend to its core no longer follows a redirect, and fails instead.
