# Repeated payloads across overlapping subscriptions

This diagnostic separates sender publication, canonical message encoding and
decoding, receiver ingestion, and result materialization for fresh bindings on
one live peer. Both arms use existing runtime behavior. The resident arm supplies
`KnownStateDeclaration::ExactVersionSet` only after the receiver has applied and
settled the earlier bodies.

## Scope and reproducibility

The fixture uses 600 synthetic rows, 18 columns, independent accepted mergeable
transactions, memory storage, and SYSTEM serving. It uses public schema/query
builders; the Node/Peer boundary exposes carrier counts and receiver phases that
the client API does not expose. It is a serial subscription workload, not an
asynchronous browser or authorization benchmark.

```sh
cargo bench -p jazz --profile perf --no-default-features \
  --features testing,transport-compression-zstd \
  --bench overlapping_payloads --no-run
JAZZ_PAYLOAD_ARM=control target/perf/deps/overlapping_payloads-<hash>
JAZZ_PAYLOAD_ARM=resident target/perf/deps/overlapping_payloads-<hash>
```

Use separate processes in control/resident/resident/control order, twice. The
benchmark guard rejects competing build/test processes. Default field widths are
0 and 128 extra text bytes per field. `JAZZ_PAYLOAD_ROWS`,
`JAZZ_PAYLOAD_QUERIES`, and `JAZZ_PAYLOAD_WIDTHS` select a smaller diagnostic.
`JAZZ_PAYLOAD_ARM=both` is useful for a smoke check, but its sequential arms share
process-global caches and are not a fair timing comparison.

The receipt was measured on engine revision
`cffc59d6bae44802008c6a6ad082d0a4761f7e29` plus this benchmark, before its
propagation to the review stack. It is not a timing comparison between that
integration revision and the PR parent. Source and binary hashes, all eight
observations, phase medians, and invariant body counts are in
[`receipts/overlapping-payloads.json`](receipts/overlapping-payloads.json).

## Results

Median sum of registration, serving, encode, decode, apply, and materialization
phases, four separate processes per arm:

| Query pattern                      | Extra bytes per field | Control ms | Resident ms | Speedup |
| ---------------------------------- | --------------------: | ---------: | ----------: | ------: |
| 12 disjoint category bindings      |                     0 |     87.134 |      87.033 |   1.00x |
| Empty / small / full list / detail |                     0 |     67.952 |      63.491 |   1.07x |
| 12 nested overlapping ranges       |                     0 |    309.128 |     155.515 |   1.99x |
| 12 disjoint category bindings      |                   128 |     93.467 |      93.317 |   1.00x |
| Empty / small / full list / detail |                   128 |     75.958 |      69.617 |   1.09x |
| 12 nested overlapping ranges       |                   128 |    343.247 |     182.010 |   1.89x |

The mixed pattern opens two empty views, two small overlapping tails, a full
list, then a one-row detail. It emits 700 result rows with 600 unique bodies.
Declarations remove the 100 repeated bodies. At width 128, receiver ingestion
falls from 33.323 to 28.485 ms; serving changes from 34.905 to 33.888 ms.

The nested pattern emits 3,900 result rows with 600 unique bodies. Declarations
remove 3,300 repeated bodies. At width 128, receiver ingestion falls from
153.547 to 33.227 ms and serving from 154.472 to 123.187 ms. Canonical semantic
response bytes fall from 22,496,098 to 3,773,885; the serialized declaration
options add 296,771 bytes. Disjoint queries send identical response bodies but
pay 148,401 additional declaration bytes.

## Interpretation and bounds

- Repeated ingestion can dominate heavily overlapping subscriptions. This
  fixture establishes a native opportunity, not a browser startup speedup or a
  runtime optimization delivered by this PR.
- Every binding checks exact row IDs, cardinality, and all 18 field values.
  Result signatures match between arms. No repair replies are required in this
  deliberately fully supplied sequence.
- A duplicate body is the complete encoded `(TxId, VersionRecord)`, including
  physical schema, branch, layer, and payload. A logical row ID is insufficient.
- Supporting input membership is still delivered and evaluated by the receiver.
  Payload possession does not substitute for authorization, membership, or an
  authority receipt. The fixture never treats publication as acknowledgment.
- Subscriptions open serially after previous ingestion. Concurrent subscriptions
  opened before any reply have no such resident inventory to declare. A sender
  deduplication protocol would separately need receiver acknowledgment, eviction,
  reconnect, and repair behavior.
- Registration includes declaration construction and serialization. Declaration
  byte counts use the serialized `Option<KnownStateDeclaration>` and exclude
  request framing and transport compression. The fixture assumes an already
  available exact inventory; it does not measure discovery from storage or a
  full request transport/validation path. Response bytes use the canonical sync
  codec and are semantic bytes, not compressed network bytes.
- Diagnostic body accounting, correctness validation, seed setup, and inventory
  bookkeeping are excluded from the six-phase total. The raw wall clock includes
  them and is labeled accordingly. CPU time in a real worker and foreground tab
  may overlap and must not simply be added to predict elapsed app latency.

Further investigation is tracked in [#3569](https://github.com/garden-co/jazz/issues/3569).

Tooling-friction: retaining a receiver-aware native fixture avoids mistaking
smaller wire payloads or a warmed second arm for an end-to-end improvement.
