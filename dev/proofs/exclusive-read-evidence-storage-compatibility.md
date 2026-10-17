# Retained exclusive proof and epoch-two node admission

## Decision

E13 retains an exclusive transaction's captured read proof for its whole
stored lifetime, including acceptance and rejection. Joe approved preserving
this requirement while updating main's conflicting deletion test on
2026-10-17 (garden-co/jazz#4152).

The source is epoch 1 with `jazz.node-storage.v2`. The target is epoch 2 with
`jazz.node-storage.v3`. Exactly one Jazz root marker is selected. Both retain
Groove dictionary v1, durable-index v2, ordered-kv v1, large-value v2 and
ordered-chunk-storage v2. Physical class layout remains v3 and row history
remains linear; no parent DAG is reintroduced.

The historical `jazz_pending_transactions` table is retained. Its primary
key is `(time: U64, node_id: U64)`. Nullable byte slots 2–5 are base snapshot,
row reads, absent reads and predicate reads. They use the explicitly specified
`jazz.exclusive-read-evidence.v2` codec in the protocol layer. Snapshot, row
and absent records retain format byte 1; predicate records use format byte 2
and canonical aggregate-result v1 bytes. Query/binding representations keep
their existing canonical, byte-pinned codecs. No default serializer encoding
is added. The permanent transaction header and provenance layout are unchanged.

Settlement writes status and retains the proof in the same batch. Touched-row
lists are still released independently. A complete view redaction carries no
proof; replay must never overwrite captured observations with different proof.
Entirely absent proof remains absent. New partial evidence is refused, while
an older absent proof is not synthesized.

## Mixed versions

Opening an exact source root acquires the backend's exclusive admission guard,
scans transaction headers and evidence read-only, validates canonical evidence
and its exclusive owner, then atomically publishes the target manifest and
`JSA1` receipt. Payload bytes are unchanged. Failure leaves source metadata and
rows unchanged. An old reader refuses the newer epoch/profile before decoding.
A target reader rejects mismatched profiles/receipts and unadmitted durable
handles before a Groove constructor can mutate layout state. Browser admission
uses the same Rust-owned scanner and byte contract.

For example, a pending exclusive read of an absent row retains its complete
proof through upgrade, relay, acceptance and reopen. An accepted source
transaction whose proof main already deleted has no recoverable observations:
it remains absent, and any path requiring complete proof fails closed. The
upgrade cannot restore historical deleted data. A malformed slot or a proof
owned by a mergeable transaction fails admission without publishing epoch two.

## Security and correctness

Creation-aware policy support remains limited to independently authorised
same-transaction inserts with absence evidence and empty linear row bases.
It does not relax ordinary exists, update/delete permissions or authority
receipt checks. Canonical evidence comparison preserves signed-zero and NaN
bit distinctions. Durable proof is distinct from permission identity.

## Limits and non-goals

This upgrade changes proof lifetime, not wire framing, row ordering, history
retention or auxiliary root formats. There is no new automatic proof GC;
retained proof lives as long as its transaction. Already deleted proof cannot
be reconstructed. Native/WASM artifacts must still be built in their own
checkout with existing provenance checks enabled.

## Evidence

Byte corpus and malformed-version receipts live beside the protocol evidence
and aggregate codecs. Root manifest bytes and JSA1 receipt bindings are pinned
in storage codec/profile and Groove manifest tests. Node reopen tests exercise
exact evidence before and after settlement; backend admission tests exercise
atomic receipts and payload preservation. The registry links these concrete
receipts and does not give each incidental byte family its own root marker.
