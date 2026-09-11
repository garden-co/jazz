# CPU profiling through cold-load readiness

The `customer_cold_start` benchmark's `bench-perf-control` feature uses external
Linux perf control/ack FIFOs. Sampling starts before connect/subscribe and ends
when every subscription has its expected rows. Post-readiness diagnostic queries
are excluded. JSON reports `cpu_profile_scope: connect_subscribe_settle`.
Ordinary benchmark builds reject the control variables. Profiling timings are
not clean performance receipts.

Build with `cargo build -p jazz-sim --bench customer_cold_start --profile perf
--features cold-settle-attribution,bench-perf-control`. Use the emitted benchmark
executable under `target/perf/deps/` in this shell pattern:

```bash
ulimit -n 65536
profile_control_dir=$(mktemp -d /tmp/jazz-cold-perf.XXXXXX)
mkfifo "$profile_control_dir/control" "$profile_control_dir/ack"
exec 3<>"$profile_control_dir/control"
exec 4<>"$profile_control_dir/ack"
JAZZ_PERF_CONTROL_FIFO="$profile_control_dir/control" \
JAZZ_PERF_ACK_FIFO="$profile_control_dir/ack" \
JAZZ_CUSTOMER_IDENTITY=member JAZZ_CUSTOMER_PHASES=cold \
JAZZ_CUSTOMER_SCALE=1.0 JAZZ_CUSTOMER_MAX_TICKS=200000 \
perf record -F 99 --call-graph dwarf,65528 --delay=-1 --control fd:3,4 \
  -o cold-cpu.data -- "$profile_benchmark"
profile_result=$?
exec 3>&-
exec 4>&-
unlink "$profile_control_dir/control"
unlink "$profile_control_dir/ack"
rmdir "$profile_control_dir"
exit "$profile_result"
```

Set `profile_benchmark` to that executable first. Both FIFOs are opened read/write
before perf starts so their opening cannot deadlock with benchmark startup.
Perf acknowledgements may include NUL framing after the newline; accept that
framing explicitly. A first run exposed this and aborted after disabling samples;
use the subsequent completed runs, not that incomplete receipt.

On runtime #2815, the completed 199Hz/default-stack and 99Hz/65,528-byte-stack
captures both materialized all 27,518 rows and reported zero lost samples.
The larger snapshots improve caller recovery through large async frames, but
some outer stacks remain unresolved. Kernel symbols are unavailable on this
host. Inclusive function percentages overlap and are not an additive phase
breakdown; the existing exclusive phase timers provide that breakdown.

The scoped samples still show roughly a quarter of CPU in allocation/freeing,
about 6% in memory movement, plus record field access, hashing and result handling.
This does not imply a quarter of allocations can be removed for a quarter of
elapsed time: the small #2813–#2815 cleanups moved cold settling from 23.881s to
22.934s, and todo roundtrips remained around 319–328ms versus 317ms previously.
Allocation-count rankings need CPU callers and code walks before prioritization.
The cold-load target of about 5s remains unmet.

Tooling friction: default 8KiB stack snapshots hide many async callers; perf's
NUL-framed acknowledgement is not obvious from the short help text. The scoped
feature and this command make both requirements explicit for future profiles.
