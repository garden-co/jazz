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
