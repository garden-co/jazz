# E2EE retained content and provisional offline writes

## Contract

A device may decrypt retained content and stage provisional encrypted writes
using keys genuinely validated online earlier. Offline `ready` describes usable
retained content, not current membership, absence of revocation, a latest epoch
or complete accepted history. Local durability and pending state are distinct
from Global acceptance.

A removed recipient can retain an old key. Provisional writes may therefore
remain readable by that recipient, even if authority later rejects the write.
The caller's configured known-stale policy can warn or refuse; it cannot detect
an unseen offline revocation. Reconnection uses genuine policy, read-set and
accepted-evidence validation. A refusal remains a refusal.

Ordinary unencrypted and no-scope operations do not acquire remote authority
merely because this cache exists. Remote, accepted, transaction and subscription
query paths retain their own authority and lifetime contracts.

## Private storage and binding

The account store's existing local-device document may contain signed retained
content and provisional-key records. The named encodings and byte fixtures are
specified in `dev/proofs/e2ee-retained-content-cache-v1.md`. They contain sealed
keys and authenticated bindings, not persisted membership verdicts. Unsupported
versions or invalid signatures refuse; unrelated device-store fields survive.

Existing C-backed records use genuinely evidenced positive device/root rows.
C observations remain partial candidates, never complete query coverage or
absence proof. They are not relabelled as accepted history.

A separate private online-anchor variant supports a fresh memory owner. A real
online accepted transaction reads the exact device-key and root rows at the same
schema/cut, verifies paired state/content origins and waits for Global. The
binding includes the actual authenticated account/session, device/key identity,
application namespace, schema, scope/table/row IDs, exact row bodies and owner
lifetime. An owner-private proof is required in addition to the signed record;
a different owner cannot inherit it from private-store bytes alone. This does
not export evidence into C or grant offline authority.

Borrowed keys retain revision, revocation, schema, device and lifetime checks
around asynchronous work and are wiped on release. Changed content, identity,
claims, origins, scope, closed or retired ownership refuses. Known device denial
is terminal for that cached owner. Failed or stale online work cannot restore a
newer denial. Cache persistence failure cannot undo an already accepted commit;
it can prevent durable offline reopening.

## Local reads

A narrow direct LocalFirst retained-content query may use one scope-indexed
candidate result while offline or connecting. It keeps the native actual
identity, governance, branch and plaintext predicates. It removes encrypted
blind-token pruning, authenticates/decrypts every candidate, then performs
canonical equality, ordering and pagination. An unknown epoch refuses the whole
operation, including on nonmatching rows or rows beyond the requested page.

This path supports at most 256 candidate rows, 1,024 encrypted cells, 1 MiB per
cell and 4 MiB of counted JavaScript values, with value depth 32 and 65,536 value
nodes. A sentinel row detects row overflow; overflow refuses rather than
truncating. Includes, hops, gather/union, computed or partial projections,
encrypted ordering and unsupported compositions refuse explicitly.

These limits are not a native scan or allocation budget. SQL LIMIT bounds
returned rows, not all policy/history/index work; JavaScript checks follow native
whole-cell materialisation. The path makes no universal resource-bound or
complete-history claim. Other query modes keep the shared authority helper and
epoch guards.

## Reopening and reconnection

Persistent reopening uses the same device/account store and exact bound content
identities. Missing keys, invalid bindings and unknown epochs refuse; a warm-up
read is not an authority substitute. Native mergeable LocalFirst reads may
hydrate the foreground through the normal local relay while retaining their
original transaction guard, staged prefix and actual policy context.

Offline provisional writes preserve atomic pending state across supported
persistent reopening. They do not mint membership evidence or silently become
accepted. When connected, normal authority validation reconciles the pending
work and can reject it. A transport event is a wake-up, not a write receipt.

## Compatibility and verification

`accepted-history-format.ts` retains the legacy data codec and its byte corpus
for compatibility only. It is not consumed as membership or query-coverage
proof. New private anchor bytes have an explicit versioned tag and fixture;
older readers fail closed on that variant. No wire format is silently replaced.

The original offline/reopen/reconnect and revocation assertions remain targets.
Tests also cover fresh-owner persistence, account/device/claim/scope isolation,
body/origin tampering, lifetime withdrawal, unknown epochs, equality pagination
and overflow. Actual execution heads, source-equivalence and browser/native
limits belong to the owning PR receipts; this document is not a passing result.
