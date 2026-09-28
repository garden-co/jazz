---
"jazz-tools": patch
---

Apply small subscription deltas row by row in the Vue, Svelte and Solid adapters instead of re-merging the whole result list, so untouched rows keep their object identity and receive no reactive writes. Frames with many changes still reconcile the whole list in one pass. The exported `applyDelta` no longer repairs a target array that was edited outside Jazz; call `reconcileArray(target, all)` for a full repair.
