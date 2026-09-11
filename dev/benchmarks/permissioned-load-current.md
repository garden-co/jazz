# Current permissioned graph load receipt

Measured with the production runtime at `d51c965f0a` (PR #2791) plus the
benchmark compatibility/diagnostic repairs described below. Native optimized
`perf` build; full-scale `customer_cold_start`, member identity, 39 subscriptions.
This is one successful cold-load phase, not a repeated-run median. The combined
cold/warm process exits unsuccessfully later during warm relay reopen.

| Cold phase                                                   |      Time |
| ------------------------------------------------------------ | --------: |
| Connection setup                                             |     <1 ms |
| Subscription setup                                           |    502 ms |
| Drive synchronization to expected subscription counts        | 51,418 ms |
| Last subscription reaches its expected row count, from start | 51,914 ms |
| Final one-shot query materialization/count checks            |  1,550 ms |
| Total reported wall, including diagnostics                   | 53,772 ms |

Expected and observed visible rows both equal **27,518**. The dominant child
table contributes **23,831**. Per-table checks pass across all 39 subscriptions.
Only three outer settle-loop iterations were needed; this is substantial work
within ticks, not a long sequence of empty network polling iterations.

Cold means an empty relay and fresh client connected to a populated core.
Warm first primes a separate relay, closes it, reopens its RocksDB state and
connects a fresh client. These are in-process native core/relay/client links,
without network RTT, browser/WASM startup or UI rendering. Fixture seeding is
outside the timed load (352 ms using this invocation's accepted seed cache).

The existing harness performs extra codec comparisons during transport; these
remain included. Its `wall_ms` also includes storage/runtime diagnostic work
after materialization. These are therefore harness timings, not a clean
production end-to-end latency estimate. Allocation/RSS metrics emitted as zero
in this build are unavailable and must not be interpreted as zero memory use.

## Iteration targets

The working target is approximately **5 seconds** to correct cold subscription
readiness; the stretch target is **under 1 second**. Against this 51.9-second
baseline, those require approximately 10x and 52x improvements respectively.
Track the final one-shot materialization and diagnostic overhead separately.
The historical harness's `target_ms=1000` remains the stretch-target marker;
its existing `under_target` field uses total harness wall time, not readiness.
Use the readiness metric above for this iteration's product target.

Profile core, relay and client work separately, including recursive permission
and inherited-child evaluation, publication, ingestion and local materialization.
Measure codec probes separately before attributing their work to the runtime.

## Warm failure

Warm priming passes its row-count checks, but reopening the newly created relay
fails with `scalar enum registry case provenance disagrees with authority
identities`. The failure also reproduces at 1% scale. No warm timing exists.
Tracked in #2792; do not bypass that validation or report the failed run as a
performance success.

## Compatibility repairs needed to run this historical fixture

- Compare the UUID membership column with `user.account`, not structured `user`;
  seed the account UUID derived by the fixture identity helper. The old form
  fails with `value does not match type Uuid`.
- Finalize core seed writes as accepted after persisting them. The old seed
  leaves every downstream table empty and times out. This follows the existing
  `s3_permissions` core seed path. The seed cache version changes to keep the
  old generated datasets separate.
- Derive test-only byte-meter column families from the current schema. The
  hard-coded removed `__groove_class_content` family previously crashed the
  diagnostics after successful loading.

No production query, permission, sync or storage-format behavior is changed.
The benchmark's visibility assertions are preserved. The original account and
seed implementations failed those assertions/execution; the repaired cold
phase reaches exact expected counts. The remaining reopen failure is separate.
