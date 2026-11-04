---
"jazz-tools": patch
---

A token signed with a key the cached JWKS lacks keeps failing (so the client retries) for as long as the identity provider is unreachable, instead of being signed out during the refresh cooldown. Only a JWKS refresh that succeeds refuses an unknown key.
