---
"jazz-tools": patch
---

A `vercel()` core no longer goes on using an expired OIDC token for its blob store. It keeps the freshest verified token, and once that token expires it reports the expiry instead of sending the stale token. When a fresh token arrives, any blob store work that had backed off after failing starts again straight away instead of waiting up to a minute.
