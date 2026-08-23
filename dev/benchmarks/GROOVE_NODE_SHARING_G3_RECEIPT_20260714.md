# Groove node sharing G3 receipt - 2026-07-14

Commit under test: `ca9c6718d`

## Native policy graph

Command shape:

```sh
JAZZ_POLICY_GRAPH_FIXTURE_DIR=<private fixture dir> \
JAZZ_POLICY_GRAPH_IDENTITY=member \
cargo bench --profile perf -p jazz-sim --bench policy_graph_concurrent
```

Fixture fidelity: 39 subscriptions, 20,726 materialized rows on every phase.

| Run | Phase | Subscribe ms | Server open bundle ms | Total wall ms |
| --- | ----- | -----------: | --------------------: | ------------: |
| 1   | cold  |          593 |                  2150 |          6137 |
| 1   | warm  |          603 |                  2117 |          6075 |
| 2   | cold  |          584 |                  2127 |          6083 |
| 2   | warm  |          591 |                  2078 |          5992 |

## Wasm ingest replay

Command shape:

```sh
JAZZ_WASM_INGEST_FIXTURE=<private wasm ingest fixture> \
node dev/benchmarks/wasm-ingest/replay-wasm-ingest.mjs
```

Fixture fidelity: 39 subscriptions, 46 fed frames, 16,371,601 fed bytes,
39 callbacks ready, 0 errors on both runs.

| Run | Open memory ms | Subscribe ms | Replay to callbacks ms | Total wall ms |
| --- | -------------: | -----------: | ---------------------: | ------------: |
| 1   |          572.8 |        288.2 |                 3625.5 |        4512.3 |
| 2   |          579.6 |        306.9 |                 3637.0 |        4550.2 |

Receipt files were written outside the public repository next to the private
fixture capture.
