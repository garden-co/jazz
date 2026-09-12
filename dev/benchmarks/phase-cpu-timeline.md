# Match CPU samples to benchmark phases

Build `customer_cold_start` with `cold-settle-attribution,bench-perf-control`.
Set `JAZZ_PHASE_TIMELINE` to an output prefix, alongside the existing perf control
and acknowledgement FIFO variables. The benchmark writes `<prefix>.cold.json`
or `<prefix>.warm.json` after readiness. Capture with `perf record --clockid mono`
and the existing acknowledged `--delay=-1 --control fd:3,4` setup. Keep CPU
sampling and allocation sampling separate.

Extract and summarize:

```sh
perf script -i capture.data --no-inline --ns \
  -F pid,tid,time,period,event,ip,sym,dso > capture.script
python3 dev/benchmarks/phase-cpu-summary.py phases.cold.json capture.script
```

The parser assigns each owner-thread sample to the deepest active interval.
Other threads and samples outside all intervals remain explicit buckets.
Percentages use sampled cycle periods, not sample counts or wall time. The
optional `--kernel-symbols` file must come from the same host boot as the capture;
without it unresolved kernel addresses stay unknown.

Capture is bounded at one million intervals; a nonzero dropped count rejects
analysis. Intervals use Linux CLOCK_MONOTONIC directly. They do not depend on
recovering an async caller or on correlating two independently sampled epochs.
The interval buffer is reserved before measurement and written afterward.
These are diagnostic builds: clock reads, tracing and sampling have overhead.
In particular this host's HPET clock path makes phase instrumentation noticeable.
Use the feature-free executable for absolute latency.

## First receipt

At runtime #2841, the timeline capture materialized all 27,518 rows and recorded
641,651 intervals without truncation. Perf reported 10,164 samples, none with an
empty stack, and no sample loss. About 0.97% of sampled cycles belonged to other
threads and 0.86% were outside recorded phases. Incomplete repository stacks no
longer prevent phase attribution.

The largest exclusive role/phase pairs were relay ingest (7.06%), relay storage
apply (6.90%), relay IVM update (5.66%), Core query-output decoding (5.07%), and
relay query setup (4.99%). No single small helper dominates. Allocation, freeing
and copying occur across these phases, so reducing common value representation
cost is a stronger next thesis than optimizing the roughly 2% fresh join-index
construction seen in the preceding uninstrumented CPU capture.

Compiler-emitted layout constants confirm that `Value` occupies 152 bytes,
`LargeValueRef` 152 bytes, `OwnedRecord` 32 bytes and `EnumValue` 40 bytes on this
native target. The inline large-reference variant makes even ordinary scalar
`Value` entries large. Moving that uncommon payload behind indirection is a
candidate, not yet a measured improvement in this receipt.
