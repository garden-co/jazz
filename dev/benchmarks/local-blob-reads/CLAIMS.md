# Reuse immutable admitted claim snapshots

## Work removed

`CanonicalPolicyClaims` already represents an immutable, exact policy snapshot.
Cloning its previous representation copied every named value, nested record
and the canonical comparison bytes. One covered read touches many policy-scoped
maps; an owner serving other live subscriptions repeats this work for those
subscriptions as well.

The candidate shares the claims and comparison bytes in one `Arc` snapshot.
Peer publication state retains the `PolicyBindingKey` selected at admission,
and owner maintenance carries that exact key instead of rebuilding it from
an identity/claims tuple. There is no global interning or new cross-session cache.

The explicit serializers, comparison encoding and durable directory encoding
remain unchanged. Equality, ordering and hashing still use the complete
canonical bytes. Two sessions with different claims remain different scopes;
refresh creates a new snapshot. Fresh owner coverage, revocation, subscription
teardown, and byte materialization keep their existing semantics.

For example, a local asset read can wake an owner that serves 36 existing
metadata queries. Each publication keeps its admitted key rather than encoding
the same structured author claims again. If that account's permissions change,
admission installs a new key and ordinary view replacement still runs. A
same-subject sibling keeps its own snapshot.

## Native comparison

This is incremental to the collector change at
`738203cbdcc0a4c2bca569da7e4a428b7df6f106`. It does not compare against main or
establish a browser/application gain. See `claims-setup.json` for frozen source
and binary hashes and the phase breakdown.

Both arms use an identical synthetic benchmark: independent preloaded memory
stores, a fresh ordinary Local/Full read per repetition, one asset with a
foreign key, 32 schema tables, and an account-bound identity with eight derived
claims. The owner is an explicitly admitted local scope. This exercises policy
snapshot bookkeeping, not server policy enforcement, IndexedDB, wire encoding,
or image/PDF rendering.

Two balanced comparison rounds, eight processes per arm/case in total, twelve
reads per process. Exclude only each process's first read and report the median
of the remaining 88 reads. All reads assert exact bytes; message and full
reconstruction counts are unchanged. No builds or tests ran during timing.

| Retained queries |    Blob |   Control | Shared claims |        Time reduction |
| ---------------: | ------: | --------: | ------------: | --------------------: |
|                0 |  32 KiB |  3.592 ms |      3.284 ms |                  8.6% |
|                0 | 256 KiB |  5.208 ms |      4.986 ms |                  4.3% |
|                0 |   4 MiB | 27.024 ms |     27.039 ms | effectively unchanged |
|               36 |  32 KiB |  4.472 ms |      3.410 ms |                 23.8% |
|               36 | 256 KiB |  6.233 ms |      5.207 ms |                 16.5% |
|               36 |   4 MiB | 27.933 ms |     27.171 ms |                  2.7% |

The owner phase supplies most of the saving. This fixed setup reduction matters
more for small reads and active subscriptions; it is not a large-blob copy
optimization. Do not multiply these numbers by unrelated performance receipts.

```sh
JAZZ_BLOB_COVERAGE=1 JAZZ_BLOB_REFERENCE=1 JAZZ_BLOB_ACCOUNT=1 \
JAZZ_BLOB_SCHEMA_TABLES=32 JAZZ_BLOB_BACKGROUND=36 \
JAZZ_BLOB_BYTES=32768,262144,4194304 JAZZ_BLOB_REPEATS=12 \
cargo bench -p jazz --no-default-features \
  --features testing,transport-compression-zstd --profile perf \
  --bench local_blob_reads
```

Build and preserve both executables before interleaving runs. The benchmark
emits owner ticks, foreground ticks, read polling and complete elapsed time.
The hashes identify the archived measured source before formatting.

## Application comparison

A separate packaged A/B/A check uses control
`8eb69d5330a19d579bfc32233b474ea5a3f1550e` and candidate
`558c07a7f74611d73d13b92b2f4e235b3416ac66`. These are integration builds with
other performance changes; they are not this review branch's main-base result.
The candidate adds the collector change and shared claims. The collector alone
was flat on these files, as recorded in `README.md`.

The same existing image and PDF were read through the unchanged application's
whole-file adapter, with ordinary Local/Full coverage and all normal background
queries active. Each run made twelve interleaved reads per file. Two runs per
arm supply 22 warm observations per file after excluding the first read of each
run. Every read checks size and media type; first reads also verify the exact
content digest outside the timed interval. No builds or tests ran during timing.

| File  |   Bytes | Control median | Candidate median | Time reduction |
| ----- | ------: | -------------: | ---------------: | -------------: |
| Image | 201,136 |       19.70 ms |         18.80 ms |           4.6% |
| PDF   |  32,592 |       18.35 ms |         13.45 ms |          26.7% |

The reverse control run returned to 20.0 ms / 18.4 ms. The image difference is
small and its distributions overlap. Click-to-preview measurements did not
establish a rendering improvement. These results support a PDF adapter-read
benefit, not a general 20% image/preview or startup claim. No application query,
authentication, stored data, history or permission behavior was changed.

## Rejected adjacent trial and qualification

An additional shortcut for an exact live publication registration did not add
a reliable endpoint improvement. Its source and receipts were preserved and
its production changes removed. Carrying the encoded key without sharing the
snapshot also failed to establish a consistent isolated-read gain; deep copies
remained. The account/background workload is the changed premise versus the
earlier SYSTEM-only policy-key experiment.

The final source, after removing the registration shortcut, passed 2,255 library
tests, all three incremental delivery canaries and five differential seeds at
depths 10 and 1,000. The library uses the CI stack size of 4 MiB.

The later integration CI-equivalent attempt passed 4,684 workspace tests and
failed one coverage-group scaling timing guard (15.82× against a 15× ceiling);
14 tests were skipped. The exact failed binary passed a quiet rerun at 10.88×.
No assertion changed. Downstream partitions did not run, and the private
sensitive-data guard was unavailable. This is not a full-gate pass. Final review
stack correctness and performance qualification remain required.

Tooling friction: an account-bound fixture with active background subscriptions
exposed repeated claim work hidden by the earlier SYSTEM-only fixture.
