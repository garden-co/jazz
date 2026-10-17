---
"jazz-tools": patch
---

Reuse authenticated accepted-only encryption history for disconnected operations
and persistent reopening. Pending metadata cannot hide an accepted change;
optional cache persistence cannot turn an accepted commit or verified read into
a rejection. Apply configured stale-write policy and reconcile when transport
connectivity returns without claiming unseen offline revocations are detectable.

Refresh accepted history before decrypting cells whose epochs are absent from
the retained snapshot. Persistent reopening after rotation reads valid newer
data and checks refreshed membership instead of reporting false corruption.

Validate retained membership snapshots independently of background online device
responder failures, including persisted equality-query reopening without an
explicit disconnect. Keep responder errors pending for explicit device
operations; accepted-history, membership and known-revocation checks still apply.

Directly hosted runtimes load accepted-history observations from their own
storage without a preceding ordinary query for each metadata table. Browser
foregrounds still hydrate from their durable worker before freezing the shared
snapshot. Schema validation, accepted-only rows and paired settlement checks
remain in place, including cold persistent reopening and pending local writes.
