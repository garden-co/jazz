# RecordPlayer benchmark variant

This isolated native package duplicates RecordPlayer's metadata column names
and indexed ordered-playlist query shape. It measures a CoverFlow album window
and a bounded playlist window. Streaming audio/range buffering is intentionally app-facing
expected-red coverage until #1833, #1839, and #1844 land.

```sh
cargo test -p jazz-example-record-player-benchmark
cargo bench -p jazz-example-record-player-benchmark --bench queries
```
