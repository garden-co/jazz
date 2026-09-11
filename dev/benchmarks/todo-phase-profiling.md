# Attribute CPU to a todo phase

`local_batch_phases` already reports monotonic start/end timestamps for each
phase. Its optional external-perf control now bounds capture to one backend and
phase, with acknowledgements before starting the phase timer and after stopping
it. `cpu_profiled` explicitly identifies the selected phase. Profiled runs are
not clean latency receipts.

Set `JAZZ_PERF_BACKEND=rocksdb_wal` and, for example,
`JAZZ_PERF_PHASE=batch_worker_ingest` or `batch_publish`. The control and ack FIFO
paths use `JAZZ_PERF_CONTROL_FIFO` and `JAZZ_PERF_ACK_FIFO`, as in the permissioned
benchmark. Start perf disabled with `--delay=-1 --control fd:3,4`, where those
file descriptors are opened read/write on the respective FIFOs. Repeat the
benchmark process to collect enough samples for these short phases. Each process
enables only the requested phase and disables capture before printing its result.

Use an explicit event (`-e cycles`) and retain the event configuration alongside
the capture. Use `perf script --no-inline --ns` for stack extraction, then select
samples inside the matching JSON timestamp windows. Kernel symbol limitations,
lost samples and unresolvable callers must remain explicit; do not infer callers
from an inclusive call graph filtered by a leaf symbol.

The first whole-process 999Hz/32KiB capture lost approximately 14% of samples.
A second 499Hz/64KiB capture with a larger buffer still reported lost chunks and
produced over 4GiB, mostly outside the requested phase. Those captures are
preliminary diagnostic evidence, not precise caller percentages. Limiting
recording at the source avoids paying for and storing unrelated setup work.

The last clean native receipt before this instrumentation change was 274.082ms
for 1,350 updates among 1,500 rows. Worker ingest was 91.041ms, publication
85.635ms, and receiver ingest 36.007ms. The worker's recorded storage-write phase
was 5.493ms within ingest. These are native synthetic measurements, not browser
or IndexedDB results.
