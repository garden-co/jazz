# alpha.54 historical benchmark harness

This branch is a new harness commit atop the exact released engine
`7cb8a9b4e088c5810540f221c0d3434069566e6b` (`v2.0.0-alpha.54`). It is
not the original release commit and must never be merged into main.
The source harness is `a2cd41ba1453a575f4e72a3ed17a1e5276265543`.

Dispatch after pushing the historical branch:

```sh
gh workflow run codspeed.yml --repo garden-co/jazz --ref bench/alpha54-codspeed-backfill
```

The two independent CodSpeed macro jobs cover every CodSpeed benchmark added
between alpha.54 and the source harness commit: permissioned-resource first sync,
and todo sequential insert, sequential update, batch update, and reopen. Existing
release-era CodSpeed suites are not rerun by this branch. The other newly added
native bench, Groove `record_validation`, is an allocation-counting standalone
receipt, not a CodSpeed benchmark. It requires a separate matched harness port if
historical native receipt coverage is desired.

Comparison retains benchmark names, row counts, three/five samples, setup and
timed boundaries, correctness verification, mimalloc, Rust 1.93.1, cargo-codspeed
5.0.1, CodSpeed action v5.0.3, and the `codspeed-macro` runner class from main.
This is the same hardware class, not a claim of the same physical machine.
The release lockfile retains all released package versions; only harness package
entries and the benchmark allocator feature are added. All engine crate trees,
including jazz-sim, are pinned and checked before upload.

API adaptations are confined to the new harness: alpha.54 uses its released
validated wire decoder (the trusted decoder did not exist), its complete
supporting-row snapshot representation, and its storage trait without the newer
comparison method. The optional history-cost profiler rejects use because its
experimental engine APIs did not exist. These preserve the old engine's actual
work rather than importing newer optimizations. Wire encoding and internal work
may differ between engine versions; logical fixture and observable results match.

Both jobs allow 180 minutes. The current 45-minute budget is unsafe for old
sequential writes: 1,350 writes times three samples, plus fixture preparation and
verification, can dominate. No alpha.54 macro timing has yet been measured, so
180 minutes is a conservative operational ceiling, not a runtime prediction.
Cold native compilation can add several minutes. Inspect individual job logs
and retain failed/timeout receipts instead of silently reducing the workload.

`alpha54-backfill.json` records engine and harness-source provenance. The guard
adds actual harness SHA and workflow clock to an uploaded JSON artifact.
CodSpeed records the true harness commit and actual run date. Its pinned action
has no timestamp override input:
https://github.com/CodSpeedHQ/action/blob/4296e51e7041e24dadb86d1d6e8b9320d223dbe8/action.yml
An explicit timeline mapping can place these points at `effectiveDate` (the
npm jazz-tools release publication date), labeled historical backfill while retaining measured-at,
run ID, engine SHA and harness SHA. Never backdate Git commits or replace raw
measurement dates. Only actual measurements count; CodSpeed's skipped/carry-forward
partial-run values are not fresh historical receipts.
