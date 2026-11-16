---
"jazz-tools": patch
---

**Rust (`jazz-app`): a trusted backend client acts for users only when its permissions match the ones its main backend serves.**

- **Why it matters:** the reads of users it acts for (`for_user`) are evaluated by the client, against the `jazz.permissions.json` it was opened with. A stale or laxer copy would widen what they return.
- **What changes:** when the main backend names the bundle it serves (`X-Jazz-Bundle` on `backend/admit`), a client opened with different permissions fails `for_user` with `Error::BundleMismatch { own, served }` (TypeScript's `bundle_mismatch`).
- **Unaffected:** its own system reads and writes.
