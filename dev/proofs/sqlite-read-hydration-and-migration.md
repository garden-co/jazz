# SQLite read hydration and stored migration declarations

A remote one-shot read can receive its row image before the chunks containing a
large value. It now remains pending while normal sync fetches those chunks and
retries the same prepared read. Previously it returned a missing-chunks error at
that point, which prevented recipients from reading encrypted file uploads.

The retry preserves the original permission identity, request claims, root-ID
restriction and scoped snapshot. A caller's local-first deadline includes chunk
hydration. Remote hydration has a 15-second bound; disconnection or reconnection
ends the wait with `NotObserved`. Dropping the future cancels that read. Local-only,
immediate and frozen reads still return without waiting, and unrelated read
errors remain errors. The existing chunk transport and authorization checks serve
all missing content; this adds no alternative transport or unrelated data scan.

For example, Alice requests an encrypted file that Bob has shared with her. The
row arrives first and the read waits for its chunk tree. It then returns the
complete value. Mallory's rows remain excluded, including when a trusted backend
makes Alice's scoped request. If Alice disconnects before the chunks arrive, the
read ends; a fresh read after reconnect can fetch the missing content normally.

Stored migration table declarations intentionally omit single-column index
selection. Deployment validation now takes that selection from the canonical
endpoint schema before comparing the declaration. Previously a later deployment
could reject an already stored table-addition migration because its declaration
lacked the managed membership table's explicit index selection. The endpoint
schema still determines the physical indexes and schema identity. Columns,
branch bindings and composite indexes must still match exactly.

## Encoding and validation

Neither change alters storage tables, wire tags, serialized fields or durable
bytes. The existing catalogue lens and schema encodings retain their different
index fields; no backfill or experimental-store migration is introduced.

Public Rust regressions delay chunk frames and cover ordinary and scoped reads,
permission isolation, cancellation, local-only/immediate/frozen reads, deadline
expiry, disconnection and reconnect. Deployment regressions cover stored table
addition and removal declarations, retention of endpoint index selections and
rejection of mismatched columns, branches and composite indexes. Existing E2EE
file and migration scenarios exercise these paths through the actual SDK.
