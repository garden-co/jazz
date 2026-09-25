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

## Browser WASM profiles, 2026-09-25

Receipts and retained changes are tracked in
[#3536](https://github.com/garden-co/jazz/issues/3536). These trials use main
`7a113ff8b6` plus shared WASM storage type erasure. The changed premise versus
[#783](https://github.com/garden-co/jazz/pull/783) is removal of duplicate engine
instantiations, cross-crate fat LTO, and package-specific optimization of hot
engine crates. Function names and all default features remain enabled.

- Global Rust `opt-level=s`, fat LTO, one codegen unit: 15.17 MB raw / 5.29 MB
  gzip, but ten repeated 2,000-row reads slowed 16–18% versus main. Excluded.
- Global `opt-level=z` with Groove at level 3: 15.00 MB raw / 5.12 MB gzip,
  but repeated memory/IndexedDB reads slowed about 23%/21%. Excluded.
- Global `s` with Groove at level 3: 15.83 MB raw / 5.58 MB gzip, with about
  4%/6% slower repeated memory/IndexedDB reads. The retained profile also keeps
  Jazz at level 3 to avoid the observed memory-read regression; that costs
  1.66 MB raw / 0.56 MB gzip. Read these as bounded fixture receipts, not
  general guarantees for every query.
- Additional wasm-opt `--merge-similar-functions -O2`, `--code-folding -O2`,
  and `-Oz` passes over the fat-LTO level-3 output saved at most 0.7% gzip.
  No additional pass is retained. The existing `-O -g` setting stays in place.

The retained profile and runtime tradeoffs are documented in
[`browser-wasm-size/RELEASE_PROFILE.md`](browser-wasm-size/RELEASE_PROFILE.md).
Raw size and compressed transfer savings must be reported separately.

## Before building another trial

1. Identify the actual allocation/copy/work site with a current profile and code walk.
2. Search this ledger, preserved branches, and PR descriptions for the same mechanism.
3. If it was rejected, state what changed before paying another build/test cycle.
4. Preserve exact source/binary provenance and compare identical workloads.
5. Record negative results as carefully as retained improvements.
