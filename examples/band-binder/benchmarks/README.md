# BandBinder benchmark variant

This native package deliberately duplicates the app schema and its bounded query
shapes without importing the application. It measures ordered blocks, one page
tree step, and the task, calendar, song, suggestion, and attachment surfaces.
Keeping this duplicate explicit makes schema/query drift visible in review while
letting the benchmark run without a browser or React.

```sh
cargo test -p jazz-example-band-binder-benchmark
cargo bench -p jazz-example-band-binder-benchmark --bench queries
```
