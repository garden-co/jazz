# RecordPlayer benchmark variant

This isolated native package duplicates RecordPlayer's metadata column names
and indexed ordered-playlist query shape. It measures a CoverFlow album window
and a bounded playlist window. Streaming audio/range buffering is intentionally app-facing
expected-red coverage until #1833, #1839, and #1844 land.

[metadata.ts](metadata.ts) owns the wall-clock descriptions and timing
boundaries for `benches/walltime.rs`, which CodSpeed runs on every
`benchmark`-labelled PR and nightly: opening the CoverFlow library, opening a
4,096-track playlist, and adding a track to a live playlist window.
`benches/queries.rs` keeps the one-shot read microbenchmarks for local use.

```sh
cargo test -p jazz-example-record-player-benchmark
cargo bench -p jazz-example-record-player-benchmark --bench walltime
```
