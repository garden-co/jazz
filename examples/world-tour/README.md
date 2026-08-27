# Jazz World Tour example

A local-first tour management app built with Vue + Vite and the Jazz Vite plugin. The globe is rendered as a custom 2D canvas dot-art projection — illustrative, not cartographic.

## Getting started

```bash
pnpm dev
```

`pnpm dev` starts the Jazz dev server and the Vite dev server together via the Jazz Vite plugin.

## Benchmark variant

`benchmarks/` is a self-contained native workload variant. It duplicates the
public schedule and venue shapes needed to measure the app's two browse paths:
the member calendar and the confirmed-only public calendar. Both are ordered,
bounded three-week itinerary reads with their venue relation included.
It does not import frontend code or claim to cover the app's unresolved
membership/venue-ownership policy decisions.

```bash
cargo test -p jazz-example-world-tour-benchmark
cargo bench -p jazz-example-world-tour-benchmark --bench queries
```
