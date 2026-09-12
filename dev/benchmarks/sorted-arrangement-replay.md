# Sorted arrangement replay experiment

Research branch based on `f56e5f3b4982995f8c4c8b4ecd3817b1b9c37e38`.
Separate from the release checkpoint and the unsuccessful prepared-accessor trial.

## Changed premise

The rejected #2864 changed construction of the same nested maps. This experiment
compares those maps against sorted keys, bucket offsets, record references and
weights. It initially changes no query execution or Jazz lowering.

The opt-in `arrangement-replay` feature records actual arrangement updates and
join probes during the native cold-load phase. Each case contains the complete
visible input arrangement plus the update or probe keys. Capture overhead and
serialization are excluded from replay timings; the capture run is not a
performance receipt. The existing anonymized fixture is unchanged.

Replay compares weighted contents after each update and keeps an old snapshot
alive to check isolation. This deliberately forces a retained snapshot for every
update; it does not establish the frequency of retained snapshots in production.
Signed-weight cancellation has a separate internal container test.

Build times reconstruct captured states for replay and must not be summed as
actual end-to-end construction cost. Probe and update cases are observed calls;
key extraction and joined-output encoding are excluded. The first sorted update
implementation rebuilds the batch, explicitly exposing its copying cost.

This capture currently covers ordinary join probes and arrangement updates. It
does not capture every semijoin/antijoin lookup or the full arrangement lifetime.
A favorable result would justify fuller integration and end-to-end measurement,
not establish a product speedup on its own.

## Replay result

Five uncontended runs, optimized native `perf` profile, mimalloc. All 3,649 cases
passed weighted-content, exact-probe and preserved-snapshot comparisons. Captured
calls include 2,923 updates (2,252 replacements) and 2,606,965 probe keys.

| Operation                                      | Existing maps median | Sorted batch median |
| ---------------------------------------------- | -------------------: | ------------------: |
| Replace                                        |            585.34 ms |            71.79 ms |
| Accumulate                                     |             85.33 ms |            17.68 ms |
| Probe                                          |             24.73 ms |            26.87 ms |
| Reconstruct captured before-state (diagnostic) |             33.16 ms |            20.55 ms |

The update saving is approximately 581 ms; it is not an end-to-end saving.
99.62% of update-batch key groups contain one record (990,417 of 994,176 groups).
Incoming records average 386.5 bytes, versus 21.2-byte keys. This supports testing
compact per-bucket storage that avoids full-record hashing while retaining cheap
bucket-local incremental updates. It does not justify a claim of faster probes.

External receipts: `permissioned-profile/arrangement-replay/` contains the exact
binary, capture, source patch, five raw logs, timing JSON and cardinality script.
The comparison always times maps before sorted batches within a case; a production
trial should use alternating process-level before/after runs and full correctness
gates before retention. The synthetic cancellation test also passes.
