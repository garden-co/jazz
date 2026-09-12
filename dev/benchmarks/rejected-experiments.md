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

| Experiment                                                                                                                                                          | Decision / evidence                                                                                                                                                                                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [#2828](https://github.com/garden-co/jazz/pull/2828): compose through total prepared projections                                                                    | Cold settle effectively unchanged (18.912s vs 18.866s). Extra compiler logic was not retained.                                                                                                                    |
| [#2830](https://github.com/garden-co/jazz/pull/2830): direct supporting-witness metadata encoding                                                                   | 18.729s vs 18.866s, within observed baseline variation. Not retained.                                                                                                                                             |
| [#2848](https://github.com/garden-co/jazz/pull/2848): universally share immutable `OwnedRecord` bytes                                                               | Cold load regressed about 4%; todo improved about 4%. Bookkeeping also affects short-lived records that are never cloned. Not retained.                                                                           |
| [#2851](https://github.com/garden-co/jazz/pull/2851): render root projections from encoded fields                                                                   | About 0.5% cold / 2.8% todo improvement did not justify projection/arena machinery. Not retained.                                                                                                                 |
| [#2853](https://github.com/garden-co/jazz/pull/2853): share compilation memos across query outputs                                                                  | About 1.1% cold improvement; todo regressed. Not retained.                                                                                                                                                        |
| [#2861](https://github.com/garden-co/jazz/pull/2861): reuse retained prepared graphs during bind                                                                    | Less than 1% cold improvement, small todo regression. Not retained.                                                                                                                                               |
| [#2869](https://github.com/garden-co/jazz/pull/2869) + [#2872](https://github.com/garden-co/jazz/pull/2872): compact private event enums and vector/sort coalescing | Combined warm comparison improved cold about 2.8% and todo about 1%; insufficient for the additional machinery. Both excluded.                                                                                    |
| [#2884](https://github.com/garden-co/jazz/pull/2884): duplicate compact-event trial                                                                                 | Closed after identifying #2869. Terminal accumulation requested bytes fell about 65%, total requested bytes about 3%, but no clear cold-load win or peak-memory reduction. Does not reverse the earlier decision. |

## Before building another trial

1. Identify the actual allocation/copy/work site with a current profile and code walk.
2. Search this ledger, preserved branches, and PR descriptions for the same mechanism.
3. If it was rejected, state what changed before paying another build/test cycle.
4. Preserve exact source/binary provenance and compare identical workloads.
5. Record negative results as carefully as retained improvements.
