---
"jazz-tools": patch
---

- `jazz-tools validate` and the dev server now refuse a relation policy literal that is not a typed value, as a deploy already did.
- React Native: the native relay reports a refused credential with its classified reason (`expired`, `missing`, `invalid`, `disabled` or `revoked`) instead of the server's message, so the client no longer re-reads the server's wording to decide whether to renew.
