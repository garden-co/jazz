# One client runtime for browser applications

This diagnostic compares the current two-runtime topology with binding the
application directly to a single storage-owning client. It changes only the
native benchmark. It does not implement a browser binding or establish an
application speedup.

## Motivation and preflight

The current persistent browser path maintains a SharedWorker client relay and
a fresh foreground replica. Both retain query graphs and arrangements; covered
input versions cross peer sync and the foreground computes its own results.
Current profiles show substantial WASM work in both realms. Their times overlap
and must not be added as wall time.

Before the trial we read `rejected-experiments.md` and searched preserved
branches and open/closed PRs. Admission handoff, typed query families, fragment
reuse and descriptor caches already exist (#3239, #3241, #3254, #3275, #3279,
#3280, #3449). This experiment changes runtime ownership instead of adding
another compilation cache.

The rejected authority-output forwarding designs (#2496, #2387) are not used:
no terminal result is substituted for a peer's receiver-local IVM. The proposed
application-facing evaluator would be the ordinary client in the worker.

## Workload and results

The existing `publication_fairness` mixed-local fixture seeds 600 items,
60 references and 15 auxiliary tables, and starts 37 subscriptions in three
stages: boot, list/background, then detail/references after the full list.
Every arm uses the same schema, queries, selected columns, stage dependencies,
materialization and exact ID/field validation. Ordering is not validated.
Setup is excluded. This uses MemoryStorage and synthetic SYSTEM identity;
there is no browser, IndexedDB, MessagePort, authentication or document decode.

[The receipt](receipts/browser-owner-topology.json) contains every observation,
phase attribution and source/binary hashes. One frozen binary serves all three
arms. For each string width, one warmup per arm is excluded, followed by four
runs per arm in this order: R/B/C/C/B/R/R/B/C/C/B/R. R is relay + foreground,
B is the relay's own Local subscription binding, and C is an ordinary client
opened through `Db::open` using its own Local subscription binding.

| Native endpoint                   | Relay + foreground | One relay binding | One ordinary client |
| --------------------------------- | -----------------: | ----------------: | ------------------: |
| Small strings, all 37 complete    |         396.104 ms |        164.811 ms |          167.376 ms |
| Small strings, full list          |         303.416 ms |         94.269 ms |           96.946 ms |
| 256-byte strings, all 37 complete |         441.148 ms |        183.538 ms |          181.674 ms |
| 256-byte strings, full list       |         352.792 ms |        115.308 ms |          113.218 ms |

The ordinary-client arm is 2.37–2.43x faster overall and 3.12–3.13x faster to
the list on this fixture. Its wide-row total ranges from 169.631 to 189.673 ms,
versus 439.635 to 443.713 ms for the two-runtime arm. All exact-result signatures
match. The similar relay-binding result supports that removing a replica is the
important difference, rather than selecting relay privileges.

Across the two runtimes, retained graph nodes fall from 3540 to 1770 and
arrangement rows from 10264 to 5132. Wide-row hydration computations fall from
1499 + 1635 to 1453. Total query program compilations remain **74 in both arms**
(37 + 37 versus 74). These counters do not establish unique live/peak memory.
Phase times are active wall time, not operating-system CPU samples.

The measured parent is `ce03faa539ee9d5654932c418061226699dde782`, an integration
of the performance stack. The review stack lacks some of those surrounding
optimizations. These are same-binary topology comparisons on the measured
integration, not exact review-head/base performance claims.

## Reproduce

```sh
# Current topology: leave JAZZ_FAIR_OWNER_BINDING unset.
JAZZ_FAIR_LAYOUT=mixed-local JAZZ_FAIR_HOST_SCHEDULER=1 \
JAZZ_FAIR_ROWS=600 JAZZ_FAIR_MIXED_WIDTH=256 JAZZ_FAIR_REPEATS=1 \
cargo bench -p jazz --profile perf --no-default-features \
  --features testing,transport-compression-zstd --bench publication_fairness

# Same command with JAZZ_FAIR_OWNER_BINDING=relay or =client selects one Db.
```

The receipt uses Rust 1.93.1. All modes retain phase breakdowns and exact
selected-field checks. Unknown ownership values fail rather than silently
choosing a different topology.

Browser implementation, same-scope tab/transaction ownership, pending-write
recovery, exact remote reads, auth changes, reconnect, cancellation and storage
reset remain required before replacing the browser path. Durable follow-up:
[#3569](https://github.com/garden-co/jazz/issues/3569). The unchanged application
endpoint must be compared after a qualified implementation exists.

No production code, existing test, storage encoding or sync wire format changes.

Tooling friction: an explicit ownership mode in the existing staged fixture
avoids package rebuilds while testing whether duplicated runtimes matter.
