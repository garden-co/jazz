# Rejected performance experiments

Read this before selecting another performance implementation. Also search
existing local branches and open/closed PRs: the table is a navigation aid,
not an exhaustive inventory. The linked PR descriptions hold the decision and
measurement context. These are completed experiments, not a pending backlog.

Each comparison is against that experiment's own parent. Different rows used
different revisions; their absolute timings are not comparable to one another.
Lower cumulative allocation requests are not proof of lower live memory or
end-to-end latency. Revisit a rejected idea only with an explicit changed
premise and a measurement that can test it.

| Experiment                                                                                                                                                          | Decision / evidence                                                                                                                                                                                                                                                                      |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [#2828](https://github.com/garden-co/jazz/pull/2828): compose through total prepared projections                                                                    | Cold settle effectively unchanged (18.912s vs 18.866s). Extra compiler logic was not retained.                                                                                                                                                                                           |
| [#2830](https://github.com/garden-co/jazz/pull/2830): direct supporting-witness metadata encoding                                                                   | 18.729s vs 18.866s, within observed baseline variation. Not retained.                                                                                                                                                                                                                    |
| [#2848](https://github.com/garden-co/jazz/pull/2848): universally share immutable `OwnedRecord` bytes                                                               | Cold load regressed about 4%; todo improved about 4%. Bookkeeping also affects short-lived records that are never cloned. Not retained.                                                                                                                                                  |
| [#2851](https://github.com/garden-co/jazz/pull/2851): render root projections from encoded fields                                                                   | About 0.5% cold / 2.8% todo improvement did not justify projection/arena machinery. Not retained.                                                                                                                                                                                        |
| [#2853](https://github.com/garden-co/jazz/pull/2853): share compilation memos across query outputs                                                                  | About 1.1% cold improvement; todo regressed. Not retained.                                                                                                                                                                                                                               |
| [#2861](https://github.com/garden-co/jazz/pull/2861): reuse retained prepared graphs during bind                                                                    | Less than 1% cold improvement, small todo regression. Not retained.                                                                                                                                                                                                                      |
| [#2864](https://github.com/garden-co/jazz/pull/2864): build fresh join indexes directly in base maps                                                                | Cold 12.404s versus 12.212s base; todo 229.8ms versus 237.6ms. No useful cold-load gain; closed and preserved. Do not repeat merely because overlay construction appears in a new profile.                                                                                               |
| [#2869](https://github.com/garden-co/jazz/pull/2869) + [#2872](https://github.com/garden-co/jazz/pull/2872): compact private event enums and vector/sort coalescing | Combined warm comparison improved cold about 2.8% and todo about 1%; insufficient for the additional machinery. Both excluded.                                                                                                                                                           |
| [#2884](https://github.com/garden-co/jazz/pull/2884): duplicate compact-event trial                                                                                 | Closed after identifying #2869. Terminal accumulation requested bytes fell about 65%, total requested bytes about 3%, but no clear cold-load win or peak-memory reduction. Does not reverse the earlier decision.                                                                        |
| [#2928](https://github.com/garden-co/jazz/pull/2928): fuse shared-witness source-presence transitions                                                               | Local 1–2% did not survive hosted confirmation. Unchanged-source repeat: cold 55.9831s parent / 55.9129s head minima (−0.13%), medians −0.17%; inconclusive. A smaller phase alone did not justify retention. Excluded from the overnight stack; original branch and receipts preserved. |
| [#2944](https://github.com/garden-co/jazz/pull/2944): borrow retained witness rows through publication staging                                                      | Cold 46.0968s / 45.8023s minima (−0.64%) but 46.1386s / 46.1560s medians (+0.04%); updates 7.3632s / 7.4351s (+0.98%). Local writes also slightly slower. Removed clones did not establish an end-to-end win; excluded, branch and receipts preserved.                                   |

The [#2946](https://github.com/garden-co/jazz/pull/2946) direct multisink
source-transition accumulator trial was also excluded: cold 45.6690s /
45.5751s minima (−0.21%) and 45.9187s / 45.8727s medians (−0.10%), versus a
0.5–2% prediction. Updates were +0.86% minimum / flat median; warmed local
cold and write pairs did not establish a win. Avoided intermediate copies did
not justify the extra call interface. Branch and receipts are preserved.

Three later overnight trials were likewise excluded, preserving branches and
original-parent receipts in [#2913](https://github.com/garden-co/jazz/issues/2913):

- [#2947](https://github.com/garden-co/jazz/pull/2947), lazy empty join-bucket
  ownership: cold 45.6690s / 45.7889s minima (+0.26%), medians +1.08%.
  Local 0.5–1.6% improvements did not survive hosted confirmation. Keep #2940's
  inline singleton representation; this rejection concerns the extra wrapper.
- [#2949](https://github.com/garden-co/jazz/pull/2949), staged operation-vector
  handoff: cold minima +0.47% / medians −0.33%; sequential update minima +0.44%
  / medians −0.30%. All 75 cases classified unchanged. Small local W1 gains
  did not explain or resolve its earlier hosted update-floor regression.
- [#2950](https://github.com/garden-co/jazz/pull/2950), shared immutable
  application schemas: cold 44.7753s / 44.7325s minima (−0.10%), medians +0.07%;
  updates +0.47% minimum / +0.42% median. The matching schema-clone frame no
  longer appeared in the new sampled cold graph, but this was not an endpoint
  win. Do not infer savings from disappearance of an optimized stack frame.

## E2EE duplicate discovery as a CI repair (2026-09-18)

The `joe/e2ee-ci-group-discovery-trial` branch at `c83cb5e872` moves group
membership discovery to the callers of delivery, avoiding a second discovery
inside inspection while retaining fresh write-time authority validation.
Compared with `0c62c00eb5` on Apple M4, with release NAPI, dev WASM at
`opt-level=1`, two Vitest workers, and the same 13 group/space cases, both runs
passed. Test execution changed from 247.6s to 224.6s (-9.3%). Discovery call/time
attribution confirmed less work; however, nested-space reconciliation changed
from 35.3s to 36.1s (+2.4%), and native denied-group rotation changed only from
23.6s to 23.3s (-1.3%).

The changed premise was remaining timeouts after bounding worker concurrency
and optimizing dev-WASM code generation. This trial is not retained as their
CI repair: it does not resolve all affected paths, and one local pair is not a
hosted or general product-performance receipt. Preserve the caller-owned
discovery tradeoff for a separately justified performance change rather than
coupling crypto lifecycle changes to correctness-fixture execution budgets.

## Before building another trial

1. Identify the actual allocation/copy/work site with a current profile and code walk.
2. Search this ledger, preserved branches, and PR descriptions for the same mechanism.
3. If it was rejected, state what changed before paying another build/test cycle.
4. Preserve exact source/binary provenance and compare identical workloads.
5. Record negative results as carefully as retained improvements.
