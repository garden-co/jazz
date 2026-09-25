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

The source-contract fragment traversal trial on #3254 was also excluded
(`6395773f93`, branch `codex/source-contract-fragment-trial`). Revalidating
compact source certificates allowed skipping internal compiler traversal, but
native 600-row/60-list fanout ABBA medians were 1.113124s control versus
1.113457s trial (+0.03%, six samples per arm). The predicted 2–6% endpoint
gain did not materialize. The simpler exact-fragment reuse remains; this
rejection concerns the additional certificate machinery. Source, binaries and
receipts are preserved in the [#2913](https://github.com/garden-co/jazz/issues/2913)
log. Do not infer a win from fewer compiled nodes alone.

The first-result unary-boundary relaxation trial on #3545 was also rejected;
receipts and the follow-up are in [#3542](https://github.com/garden-co/jazz/issues/3542).
It allowed a disposable hydration frame to contract locally private unary edges
despite observers elsewhere in the runtime. Twelve matched native initial-SELECT
runs gave 348.778 ms control / 353.198 ms trial medians (+1.3%); the largest query
was 202.392 / 205.027 ms (+1.3%). All four expensive 43,000-row map stages remained.
Lower standalone map counters partly moved work into fused pipelines and did not
establish fewer total copies. The runtime change was removed; its exact patch,
tests, binaries and every sample remain in `target/saas-read-first-result-pipelines-ab/`.
Revisit only with evidence that the eligibility change reaches the dominant maps
and with separate fused-output byte counts.

The contiguous prepared-record copy trial after #3546 was also rejected
([#3542](https://github.com/garden-co/jazz/issues/3542)). It grouped adjacent
fixed/variable fields into copy ranges while keeping every interior offset check.
The first native SELECT round improved 2.2%, but the reversed-order confirmation
regressed 2.8%; pooled medians were 309.172 / 304.293 ms overall and
161.962 / 158.851 ms on the largest SELECT. This did not establish a robust gain
for the added encoding-kernel complexity. All 39 encoded results matched in all
twelve runs; 53 focused tests passed, including malformed interior offsets and
partial-output rollback. The runtime/test changes were removed. Exact source,
binaries and every observation remain in `target/saas-read-copy-runs-ab/`.
Revisit only with a changed mechanism or workload that demonstrates a repeatable
endpoint benefit, not merely fewer copy calls.

## Before building another trial

1. Identify the actual allocation/copy/work site with a current profile and code walk.
2. Search this ledger, preserved branches, and PR descriptions for the same mechanism.
3. If it was rejected, state what changed before paying another build/test cycle.
4. Preserve exact source/binary provenance and compare identical workloads.
5. Record negative results as carefully as retained improvements.
