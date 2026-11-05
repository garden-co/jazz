# Accepted-evidence notifications for encrypted subscriptions

A local grant can appear in a subscription before the authority accepts its
write. E2EE correctly excludes that provisional grant when checking accepted
history. Acceptance preserves the row's contents, however, so an ordinary row
diff does not cause another check. A removed recipient could consequently keep
receiving the previously materialised encrypted query result without its
subscription reporting the loss of access.

E2EE dependency subscriptions now opt into notifications when fresh authority
evidence arrives. For example, Alice removes her space grant. The optimistic
grant notification cannot authorise that removal; the later authority evidence
causes a second check, which terminates the encrypted subscription with
`E2eeDataError`. It does not publish an empty replacement for the earlier result.

## Invariants

- Notifications are invalidation signals, not acceptance proofs. The existing
  paired-read and cryptographic checks still decide whether content is accepted
  and whether the reader can use its key.
- A proof and its authority coordinate can arrive in different transport turns.
  The replica tracks both the proof-admission ordinal and its known authority
  position. One primary-key range finds newly admitted proofs already covered by
  that position; another indexed range finds older proofs whose position just
  became known. Notifications wait until both are available. A reopen starts at
  the retained admission ordinal and known position, preserving this distinction.
  Duplicate deliveries and rolled-back records cannot advance these frontiers.
- Existing table/key watches route each changed row to affected queries,
  including nested dependencies. Only opted-in members of a shared query receive
  the extra event. Ordinary subscriptions keep their existing row-diff behaviour.
- The event contains no added, updated or removed rows. It cannot change the
  subscription's settlement tier or bypass permissions, and frozen views do not
  receive these live invalidations. Queries waiting for chunks retain their
  existing publication gate.
- The SDK marker is internal. Public query-option normalisation strips it;
  internal dependency subscriptions preserve their normal read tier and identity.

## Compatibility and validation

The additive `_jazz_accepted_rows_by_seq_v1` index covers `(seq, observed)` on
`_jazz_accepted_rows_v1`. Existing stores create it during their normal open;
row keys, proof encodings and stored values are unchanged. It bounds newly
eligible evidence by authority position instead of scanning retained history.
No wire bytes change. The host-only `observe_accepted_evidence` subscription
option enables the behaviour through the existing shared WASM/NAPI binding.
The event uses the existing empty delta encoding; both notification frontiers
are in memory and are neither persisted nor transmitted.

Two public Rust regressions reproduce the failures before their fixes. They
cover direct and relayed connections, root and nested queries, unchanged
ordinary subscriptions, unrelated-key isolation, duplicate-proof delivery and
separate transport turns for the proof, row and authority receipt.
The existing UUID equality-query regression checks both canonical and alternate
UUID spellings, terminal revocation and preservation of the last valid result.
A client-boundary test checks internal forwarding and public option stripping.
