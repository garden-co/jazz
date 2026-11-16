---
"jazz-tools": patch
---

**`forUser` handles for the same user keep their own claims on writes.** A transaction opened from a handle acting for a user (`forSession`/`forUser` in TypeScript, `for_session`/`for_user` in Rust) is now checked with the claims its writes were staged with. Before, it took the claims registered for that user when it committed, so another handle for the same user with different claims could change them in between. Single writes outside a transaction already carried their own claims.
