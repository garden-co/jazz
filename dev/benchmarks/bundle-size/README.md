# Bundle sizes quoted in the docs

The docs say how much the thin client adds to an app and how large the Wasm
engine is. Those numbers come from here, not from hand-typed text.

`measure.mjs <version> <out.json>` builds two minimal Next.js apps
(`fixtures/base` and `fixtures/jazz`, identical except for `<JazzProvider>`)
against the published `jazz-tools@<version>`, with the Next.js and React
versions the docs use. It sums the gzipped JS each prerendered page loads before
first render, and records the difference as `thinClientAddedGzipBytes`. React and
Next.js cancel out, and lazy chunks such as the local-first client aren't
counted. It also records the `jazz-wasm` engine's size, gzipped and raw.

A real Next.js build is used on purpose: esbuild tree-shakes and splits more
aggressively and reports about 10% less than adopters see.

`.github/workflows/bundle-sizes.yml` runs it after every publish (or by hand)
and uploads `bundle-sizes.json` to the `perf-timeline-data` pre-release. The docs
build reads that file once (`docs/lib/bundle-sizes`) and replaces `%bundle.thin%`,
`%bundle.wasm%` and `%bundle.wasmRaw%` in pages and partials with rounded figures.
If the file can't be fetched, it uses the committed
`docs/lib/bundle-sizes/fallback.json`. Refresh that copy now and then from the
release asset.
