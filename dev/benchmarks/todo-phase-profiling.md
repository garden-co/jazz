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
fixture within one process to collect enough samples for these short phases,
using a repeated list such as `JAZZ_BATCH_ROWS=1500,1500,1500`. Each fixture
enables only the requested phase and disables capture before printing its result.
Repeated child processes while recording is disabled left later executable
mappings unavailable to the unwinder in this environment; their empty stacks
must not be treated as zero CPU cost.

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

A selected worker-ingest capture at retained runtime #2827, with these benchmark
changes, recorded 996 samples in 62.6MiB. Filtering by the 20 actual phase windows
left 961 samples, all with nonempty stacks, and perf reported no sample loss.
Named allocation/free routines account for approximately 21% of weighted samples,
byte comparison 7%, and copying 6%. Some outer callers remain unresolved; these
are sampling estimates, not independently additive elapsed-time savings.
