# PERF-4 known-state scaling

`known_state_scaling` holds a 1,000-row persisted whole-table rehydrate fixed
while sweeping the number of exact row-version identities declared by the
receiver. It measures the variable declaration payload and response using
Jazz's postcard wire encoding; fixed Subscribe framing is intentionally excluded
from every rung.

The hard validity gates require every rung to:

- preserve the same 1,000 result additions and membership digest;
- emit exactly the row-version bodies not covered by the declaration; and
- cover the complete expected version set when declared and emitted versions
  are combined.

Run the default ladder with:

```sh
cargo bench -p jazz --bench known_state_scaling
```

Override it with `JAZZ_KNOWN_STATE_ROWS` and the comma-separated
`JAZZ_KNOWN_STATE_COVERAGE` percentages.

## Initial result

One default-ladder run on the local development box produced:

| Known rows | Known | Declaration |  Response | Variable exchange | Version bodies | Storage reads |
| ---------: | ----: | ----------: | --------: | ----------------: | -------------: | ------------: |
|          0 |    0% |         1 B | 382,586 B |         382,587 B |          1,000 |         4,000 |
|        250 |   25% |    12,004 B | 306,323 B |         318,327 B |            750 |         4,000 |
|        500 |   50% |    24,004 B | 229,823 B |         253,827 B |            500 |         4,000 |
|        750 |   75% |    36,004 B | 153,323 B |         189,327 B |            250 |         4,000 |
|      1,000 |  100% |    48,004 B |  76,936 B |         124,940 B |              0 |         4,000 |

Known-state dedup is structurally effective: response bodies and bytes fall
linearly with receiver coverage. Even after charging the explicit declaration,
full coverage reduces the measured exchange by about 67%. Result membership is
not deduplicated, as required by `INV-SYNC-24`, so the response retains a roughly
77 KB floor at full coverage.

The serving-side storage work does not fall with coverage: every rung performs
4,000 aggregate reads. Wall time from a single run was noisy (roughly 8–11 ms) and is
not used as an acceptance claim. The next optimization question, if this lane
justifies one, is whether known-state can avoid assembling or reading payload
witnesses that will subsequently be suppressed; that requires a separate
profile and correctness-preserving design.

Tooling friction: sharing compiled artifacts between clean benchmark worktrees
would have avoided rebuilding RocksDB for this receipt.
