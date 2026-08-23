# PERF-5 maintained versus rehydrate scaling

`maintained_rehydrate_scaling` compares two ways of bringing the same filtered
subscription result up to date while increasing the source and retained-view
sizes:

- an already-maintained subscription receives one source-row change; and
- a new peer fully rehydrates the resulting view from persisted state.

The fixture alternates active and inactive documents, primes the maintained
subscription, and changes exactly one inactive document to active. Each rung
hard-gates identical final result membership, its expected cardinality, and the
number of emitted additions and version bundles. The retained footprints must
also describe the same result size.

Run the default 100/1,000/10,000-row ladder with:

```sh
cargo bench -p jazz --bench maintained_rehydrate_scaling
```

Override the comma-separated source sizes with `JAZZ_PERF5_ROWS`.

## Initial result

One default-ladder run on the local development box produced:

| Source rows | View rows | Maintained time | Maintained bytes | Maintained reads | Rehydrate time | Rehydrate bytes | Rehydrate reads | Retained heap |
| ----------: | --------: | --------------: | ---------------: | ---------------: | -------------: | --------------: | --------------: | ------------: |
|         100 |        51 |           84 us |            491 B |                1 |       1,285 us |        20,750 B |             401 |     263,047 B |
|       1,000 |       501 |          158 us |            494 B |                1 |      10,987 us |       204,702 B |           4,001 |   2,633,197 B |
|      10,000 |     5,001 |        2,240 us |            494 B |                1 |     132,845 us |     2,056,606 B |          40,001 |  26,406,697 B |

The maintained path emits one addition and one version bundle at every rung;
its encoded bytes and storage work remain flat. Full rehydrate emits the whole
result, and its bytes and reads grow linearly. Both paths retain the same view
state, whose measured heap grows linearly with the source and result sizes.

The maintained wall time is not fully flat even though its delivery work is.
Every outgoing maintained update currently recomputes the metrics footprint by
walking the retained view, so the timed path includes an O(view) observability
cost. This receipt therefore establishes delta-bounded I/O and output, not a
constant-time CPU claim. That footprint refresh is a separate optimization
thread with a straightforward structural explanation.

Timing values are directional single-run measurements. The deterministic
bytes, reads, additions, bundles, retained rows, and exact result equality are
the acceptance evidence.

Tooling friction: a clean worktree had to rebuild the RocksDB benchmark once;
subsequent runs reused its local target artifacts.
