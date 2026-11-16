---
"jazz-tools": patch
---

**Rust: reads made for a user evaluate with that handle's own claims.** Reads through a session-scoped client (`JazzClient::for_session`, and `jazz-app-support`'s `for_user` built on it) now carry their session's claims with each read, as the TypeScript runtime's do. Several handles acting for the same user with different claims each read by their own. Writes carry their claims as before.
