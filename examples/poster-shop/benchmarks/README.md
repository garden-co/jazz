# PosterShop benchmark variant

This isolated native package duplicates PosterShop's canvas, layer, shape,
cursor, asset-metadata and checkpoint schema and its ordered query shapes.

[metadata.ts](metadata.ts) owns the wall-clock descriptions and timing
boundaries for `benches/walltime.rs`, which CodSpeed runs on every
`benchmark`-labelled PR and nightly: opening a canvas, drawing a shape on a
live canvas, and a collaborator's cursor moving without waking the canvas.

```sh
cargo test -p jazz-example-poster-shop-benchmark
cargo bench -p jazz-example-poster-shop-benchmark --bench walltime
```
