# E2EE accepted-history cache

## Overview

The E2EE client retains coherent, accepted metadata snapshots so an existing
space can be used without waiting for the network, including after reopening a
persistent native database. This is an internal cache, not an authority receipt
or a new public API. Ordinary, non-E2EE queries do not use it.

This chapter defines the extracted offline-history layer. It does not establish
complete transport-recovery coverage or independent security qualification.
Current-source receipts belong to the owning PR; remaining limits are tracked in
[release qualification (#3125)](https://github.com/garden-co/jazz/issues/3125).

## Details

### Storage and format

The dedicated E2EE account store's existing local-device document can contain
an `acceptedHistoryV1` array. Each entry contains:

- `scope`: the encoded account registry, environment and account ID;
- `key`: the encoded space scope and identifier;
- `reads`: a JSON string containing query identities, metadata rows and row
  settlements;
- optional `initial`: the root ID, initial row IDs, recipient IDs and accepted
  transaction ID for a newly created space.

The local snapshot encoding reserves the single-key object
`{ "e2eeBytesV1": [0, 127, 255] }` for byte arrays. Every element must be an
integer from 0 to 255. Settlement positions are canonical unsigned 64-bit decimal
strings. Duplicate row or settlement IDs and malformed entries are rejected.
The encoding has a fixed corpus in `accepted-history-format.test.ts`.

Cached snapshots contain public metadata and encrypted envelopes, not decrypted
keys or persisted authorisation verdicts. Updates preserve other entries in the
local-device document. Byte-identical history does not trigger another update.
The current durable cache has no eviction or total-size bound; this remains a
qualification concern rather than a promised storage limit.

### Reopening and validation

Restoration reruns the existing metadata verifier. Its freshly built queries
must match the saved query identities exactly, with no missing or unused reads.
Saved query text is never executed. Initial recipient IDs reproduce the original
preparation's dependency reads, including those needed when group tables exist.

Before reuse, all metadata rows, absences and settlements must match the local
accepted-only observations. Pending local writes cannot hide an accepted change.
A mismatch requires reconciliation; a cached bundle alone cannot establish
current authority or reveal revocations the disconnected client has not received.

Local-only propagation does not wait for a server connection or fail merely
because that connection failed. The read still uses the globally accepted tier
of the local snapshot; bypassing transport must not admit pending metadata.

### Initial creation and failure

Atomic creation retains the coherent preparation before staging the signed root
and grants. It becomes eligible only after global acceptance, identified by the
actual transaction ID. Before offline reuse, every initial row must have that
accepted transaction ID and the other dependencies must still match.

Acceptance attempts to save the bundle locally without another remote query.
A cache failure must not turn an already accepted transaction into a rejected
commit receipt. The in-memory bundle remains available in that case, but durable
offline reopening is not guaranteed. Uncommitted preparation is discarded on
rollback or rejection, including cancellation while preparation is finishing.

### Local use and reconciliation

After an online read has verified its accepted history, saving the optional
offline cache is best-effort. A persistence failure must not fail that verified
read. The verified in-memory bundle may still be retained, but offline reuse
continues to require matching accepted-only observations. Failure to persist
does not promise offline availability after reopening.

Key preparation first tries the locally accepted history. A usable bundle lets
the operation proceed without waiting for online reconciliation. If local
preparation cannot supply it, an online client falls back to reconciliation;
an explicitly disconnected client reports the local result. A callback that has
already started is never replayed through that fallback.

An online local-cache use schedules background reconciliation. Explicit
reconnection also schedules it for spaces encountered by this client instance.
Concurrent background attempts for the same space are coalesced. This does not
scan every persisted space or cover every automatic transport-recovery event.
Background failure does not fail the completed local operation; a later use or
explicit reconnect can retry, and explicit explanation exposes maintenance state.

An online refusal is reconciled before returning it: a newer grant may now
permit access. Local key preparation can use local catalogue IDs as candidates,
but they establish neither catalogue coverage nor membership. Retained accepted
space history must validate before use. Without that history, online preparation
uses the covered catalogue path. Catalogue activation installs the complete
schema mapping, including the data-table and column identities bound into AEAD.

Local device preparation and online enrolment use separate in-flight promises.
A stalled online enrolment must not block an otherwise valid local encrypted
operation. Once online enrolment starts, it must complete or reject; a disconnect
must not make a skipped enrolment look complete to later reconnecting callers.

## Verification

The format corpus pins canonical bytes and malformed-record rejection. The
Node and browser offline suites exercise accepted history under pending writes,
observed revocation, optional persistence failure, reconnect and persistent
reopening. Their presence is not a passing receipt: executed commands and exact
source revisions are recorded in the owning PR. Historical stage results are not
reused as qualification for an extracted tip.

## Open questions

- [#3125](https://github.com/garden-co/jazz/issues/3125): durable cache size and
  eviction policy. Eviction must make missing history explicit, never infer access.
- [#3125](https://github.com/garden-co/jazz/issues/3125): automatic transport recovery
  and controlled reconnect concurrency, including nested membership and revocation.
