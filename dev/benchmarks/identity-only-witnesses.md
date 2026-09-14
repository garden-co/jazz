# Identity-only maintained witnesses

Experiment and outcome log: [#2913](https://github.com/garden-co/jazz/issues/2913).
Parent: `e7ce25ecda9a92f5b1edcec46736c189eaa4f329` (#2954).

Replace full row-body witnesses with exact native-version references where the
source resolver proves history backing. Carry the proof through terminal
lowering, weighted retention, replacement selection and publication. Sources
without that proof retain explicit materialized payloads; never fabricate a
partially populated VersionRow. Physical table identity, branch, transaction,
layer and authored schema are part of resolution. Source/route authorization
and independent role weights remain separate from physical body identity.

The ordinary publication boundary already reloads authored history because a
query-projected witness body is not canonical. Resolve once there, not once
before and again during bundling. A missing proven native body fails closed;
it is not an instruction to use a query projection or broaden a lookup.

No intended public API, wire or durable-storage encoding change. Compiler
proofs and retained references are process-local. Immutable payload and
malformed/conflicting-input checks remain at their existing admission boundary.

Measure the existing example-owned permissioned cold and four todo workloads,
unchanged timer boundaries, perf profile/mimalloc/RocksDB WalNoSync. Preserve
exact parent/candidate source and binary receipts and alternate clean timing
arms. Claims about native/materialized counts, retained payload bytes or
history lookups require separate opt-in counters; these clean timings do not
measure those counts. Do not infer calls from sampled stack occurrences.
Public integration assertions cover
row contents, versions, permissions and delivery; internal representation
checks are necessary to prove that ordinary witnesses no longer carry bodies.

The native path also avoids the second witness-source join when the authorized
relation declares every required coordinate and route carrier. Flat joins can
omit schema/branch metadata even while retaining row/transaction keys; those
keep the narrow coordinate-recovery join. Opaque output declarations likewise
do not confer this optimization. Unproven sources keep full payload recovery.
This differs from the preserved narrow-witness experiment (#2882): native
history proof removes the requirement to obtain a body from the witness join.
The public flat-join, permission, route and retraction tests remain unchanged.

Source/binary receipts and measured outcomes belong in #2913, including the
initial checkpoint's smaller-than-predicted cold-load gain. Do not treat a
reduction in retained payload size as an established end-to-end improvement.
