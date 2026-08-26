# BigLabel benchmark variant

This package is a self-contained Rust model of BigLabel's read-heavy record-label
workload. It intentionally duplicates the schema and deterministic fixture needed
for measurement; it does not import application runtime helpers.

The fixture creates four catalogues, eight labels, 32 artists, and either 512 or
4,096 releases. Release rows carry indexed label, primary-artist, catalogue, and
release-time fields. The measured workloads are prepared, ordered Jazz reads for:

- a label's releases;
- an artist's releases;
- a catalogue's releases.
- a 1,000-row import at batch sizes 1, 10, 100, and 1,000, plus a 10k-row
  import at batch size 1,000 for performance thesis
  [#1964](https://github.com/garden-co/jazz/issues/1964).

The hosted simulation matrix currently stops at 10k rows. A first trial
completed 10k but timed out during 100k; larger scales stay in local or
wall-time experiments until they fit the hosted workflow budget.

Database opening, schema compilation, fixture insertion, local-durability waits,
and query preparation happen before each measured closure. The returned row count
is black-boxed so the read cannot be optimized away.

The ingest benchmark measures release construction and insertion. Schema
compilation, database opening, and dimension-row seeding are supplied as untimed
per-iteration Divan inputs. Existing read fixtures retain their original
one-transaction-per-release history shape so their CodSpeed series stays
comparable.

Run locally with:

```sh
cargo test -p jazz-example-big-label-benchmark
cargo bench -p jazz-example-big-label-benchmark --bench loads
```
