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

Database opening, schema compilation, fixture insertion, local-durability waits,
and query preparation happen before each measured closure. The returned row count
is black-boxed so the read cannot be optimized away.

Run locally with:

```sh
cargo test -p jazz-example-big-label-benchmark
cargo bench -p jazz-example-big-label-benchmark --bench loads
```
