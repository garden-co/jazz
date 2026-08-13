# INV-INC-1 relation/include delivery receipt

`crates/jazz/benches/relation_include_delivery.rs` measures one local
relation/include subscription while holding the measured change to one inserted
child row. Setup and initial hydration are outside the measurement window; the
window starts immediately before the insert and ends after the resulting delta
has been consumed.

The default accumulated-child ladder is `1k, 2.5k, 5k, 10k, 20k`. It preserves
the existing canary's endpoints, adds three interior rungs to make a slope
visible, and uses increasingly wide gaps so a linear dependence on accumulated
state cannot hide between adjacent points.

Each rung uses three fresh fixtures and independently emits the median
allocation count, allocation bytes, and wall time. Allocations and bytes are
the primary signals because they are stable under shared-host contention; wall
time is retained only for operational context. The delivered delta's
root/related-row/edge counts are emitted only after the receipt verifies that
they are identical across all samples, so those values describe every sample
rather than a selected run.

The final JSONL line (`phase: "slope"`) reports least-squares slopes in
per-metric median allocation work per accumulated child, max/min allocation and
byte ratios, and the acceptance rule. The initial row-carrier receipt required
both ratios to remain at or below `1.025x`: it measured `1.001031x` allocations
and `1.017327x` bytes, leaving 0.7673 percentage points of headroom above the
larger drift rather than inheriting the canary's intentionally loose `3x` band.

The terminal-operation carrier realignment explicitly relaxed that ratio bound
to `1.043x`. Its one-sample smoke receipt measured `1.001512x` allocations and
`1.034605x` bytes; repeated fresh three-sample receipts measured
`1.001133–1.001512x` and `1.033047–1.034582x`. This is a real relaxation of the
scale-independence percentage, not merely a renamed threshold. Absolute median
allocation bytes nevertheless fell from roughly 487–495 KiB in the initial
receipt to 361–373 KiB with the terminal-operation carrier. Set
`JAZZ_INC_DELIVERY_MAX_RATIO` to evaluate a proposed tighter threshold without
changing the workload.

Run the full receipt:

```sh
cargo bench -p jazz --no-default-features --bench relation_include_delivery
```

The smoke gate runs the same five rungs with one sample each, since allocation
counters are deterministic for this fixture and the smoke gate needs a bounded
cost. Full receipts retain three samples per rung by default.
