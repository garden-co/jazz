---
"jazz-tools": patch
---

A token signed with a key the cached JWKS lacks keeps failing (so the client retries) for as long as the identity provider is unreachable, instead of being signed out during the refresh cooldown. Only a JWKS refresh that succeeds refuses an unknown key. While that refresh cools down (up to 30 seconds), a token with a bad signature on a known key also gets a server error rather than a 401.
