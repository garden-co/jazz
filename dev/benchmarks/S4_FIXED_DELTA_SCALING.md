# S4 fixed-delta propagation scaling

The `fixed_delta_propagation_scaling` phase in `s4_order_processing` holds one
accepted Payment transaction fixed while increasing the retained whole-table
view with unrelated customer rows. The transaction changes four current rows:
one warehouse, district, customer, and newly inserted payment.

The gate primes all eight S4 table subscriptions across both core-to-edge and
edge-to-client hops before the change. Every rung must then preserve:

- the SQLite-reference final state;
- exactly four result additions, three replaced-result removals, and fixed
  version bundle and record counts on each hop;
- identical program-fact additions and removals; and
- identical storage reads and ranges on each node.

Encoded wire bytes may differ from the first rung by at most 72 bytes per hop:
the maximum nine-byte growth of a postcard-encoded `u64` `settled_through`
cursor across eight view-update envelopes. This is a fixed framing allowance,
not a view-size ratio.

Run the default ladder with:

```sh
JAZZ_S4_PROPAGATION_SCALE_ONLY=1 cargo bench -p jazz-sim --bench s4_order_processing
```

Override customer counts with the comma-separated
`JAZZ_S4_PROPAGATION_CUSTOMERS`. The smoke scenario runs a smaller 2/20/200
ladder with the same hard gate.

## Initial result

One 10/1,000/10,000-customer run on the local development box produced:

| View rows | Changed rows | Core → edge bytes | Edge → client bytes | Adds/removes per hop | Bundles/records per hop | Core reads/ranges | Edge reads/ranges | Wall time |
| --------: | -----------: | ----------------: | ------------------: | -------------------: | ----------------------: | ----------------: | ----------------: | --------: |
|        15 |            4 |           4,570 B |             4,570 B |                  4/3 |                    4/10 |               4/4 |           184/192 |  5,949 us |
|     1,005 |            4 |           4,594 B |             4,594 B |                  4/3 |                    4/10 |               4/4 |           184/192 |  7,469 us |
|    10,005 |            4 |           4,602 B |             4,602 B |                  4/3 |                    4/10 |               4/4 |           184/192 | 27,542 us |

Propagation I/O, result deltas, facts, and version payload work remain fixed as
the unrelated retained view grows. The 32-byte wire difference is cursor
framing across eight messages on each hop.

Wall time is directional and is not the acceptance claim. It still grows with
the retained view despite flat I/O and output, consistent with retained-view
bookkeeping such as the metrics-footprint refresh identified by PERF-5. This
receipt establishes structurally delta-bounded propagation, not constant CPU
time.

Tooling friction: a shared release target would have avoided rebuilding the
RocksDB dependency graph in the clean benchmark worktree.
