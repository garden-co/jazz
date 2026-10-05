---
"jazz-tools": patch
---

Reuse authenticated accepted-only encryption history for disconnected operations
and persistent reopening. Pending metadata cannot hide an accepted change;
optional cache persistence cannot turn an accepted commit or verified read into
a rejection. Apply configured stale-write policy and reconcile on explicit
reconnection without claiming unseen offline revocations are detectable.

Validate retained membership snapshots independently of background online device
responder failures, including persisted equality-query reopening without an
explicit disconnect. Keep responder errors pending for explicit device
operations; accepted-history, membership and known-revocation checks still apply.
