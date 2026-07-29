# Cold settle profile — 2026-07-28

## Verdict

- **CLEARLY-BAD:** the isolated low-load cold receipt settled in 12.409s, above the <10s goal.
- **CLEARLY-BAD:** the critical path is receiver-side bulk view ingest, not transfer or connection.
- **UNCLEAR:** the final exact-timer receipt ran while host contention rose (one-minute load 2.74 to 16.00), so its 27.766s wall time is not a latency baseline. Its counters and phase ordering remain useful.

## Command

```sh
JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold \
JAZZ_CUSTOMER_SCALE=1.0 JAZZ_CUSTOMER_MAX_TICKS=200000 \
JAZZ_REHYDRATE_TRACE=1 JAZZ_PROFILE_OUT=target/cold-settle-profile-receipt-20260728 \
JAZZ_PROFILE_FREQUENCY=997 \
  cargo bench -p jazz-sim --bench customer_cold_start --features profiling -- --nocapture
```

Only the anonymized in-repo customer-shape fixture was used. The benchmark has
39 subscriptions and produced 27,518/27,518 final rows. The profile process
ran on one foreground CPU; no RocksDB background CPU samples were observed.

## Exact-timer phase accounting

The final pass used in-process `SyncMessage` queues, not the byte-wire adapter:
receive/decode is therefore effectively zero here. The separately measured
streaming zstd probe was 35.277ms encode + 16.278ms decode over 20.191MB raw /
0.492MB streaming-zstd payload, and is not the wall-time cause.

| Settle phase | Time | Share | Rows / edges processed |
|---|---:|---:|---|
| Receive/decode | 0.039ms | 0.0% | 215 messages; in-process semantic messages, no wire decode |
| Core ingest | 17.761s | 64.0% | 74,258 receiver bulk bundle ingests (46,740 relay + 27,518 client); 75,574 result-membership/program-fact edges |
| Maintained-view settle | 7.026s | 25.3% | 47,437 core snapshot member edges, then 28,137 relay-to-client member edges |
| Subscription adapter/materialization | 1.371s | 4.9% | 39 streams, 27,518 final rows |
| Residual: scheduling, outbound queueing, checks | 1.607s | 5.8% | no additional row/edge transform counted |
| **Total settle** | **27.766s** | **100.0%** | |

The low-load CPU capture of the same shape (12.409s settle) independently put
`apply_view_updates_in_batch` at 3,736/7,235 foreground samples (51.6%). That
corroborates the exact counter ordering despite the final pass's contention.

## Dominant child

`res_l_child_3` gates readiness: 23,831 final rows reached ready at 28.304s,
the all-holder endpoint. Its upstream maintained rehydrate processed 43,065
member edges (794ms open + 361ms bundle); the relay then settled 23,867 visible
member edges (583ms drain + 395ms bundle). The 43k-to-23.8k reduction is the
policy boundary, and all of that work still lands before readiness.

## Interpretation

**Grind:** receiver bulk reset-view ingest and record/edge conversion dominate.
The CPU profile's leading frames are sequence serialization, record projection,
hashing, and `view_update_chunk_from_units`; transport compression is tiny.
This is a concrete optimization lane, not an undifferentiated settle bucket.

**Design:** the system insists on ingesting and settling the complete snapshot
for every holder before any ready state. The dominant child alone requires
43,065 upstream / 23,867 visible membership edges. Progressive first-page
readiness and a persisted materialized snapshot would remove large portions of
that work from the critical path.

The prior claim that grinding was exhausted is **contradicted**: ingest is a
measured majority cost. The design conclusion still stands for the larger goal:
even a good ingest improvement leaves full-snapshot-to-all-holders work on the
critical path.

Raw logs and pprof artifacts are intentionally local under `target/`; temporary
source instrumentation was removed before this receipt was committed.

Tooling-friction: the optional pprof feature triggered a cold RocksDB/pprof
release build before measurement; a prebuilt profiling bench target would save
wall-clock.
