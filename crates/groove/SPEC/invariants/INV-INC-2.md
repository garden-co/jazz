# INV-INC-2

- Status: target
- Coverage: untested

## Invariant

For a touched `CollectBy` root group `g`, indexed state maintenance MAY cost `O(|D_g| log(1 + |G_g|))`; selecting, rendering, comparing, and delivering MUST cost only `O(|D_g| + R_g(limit))`, where `R_g(limit)` is the larger row-and-byte footprint of old/new selected windows across every slot in the rendered tree, including encoded descendants. A finite tree sums its finite selected slot windows; an unbounded descendant contributes its actual selected output, never retained state outside it. Collect mode may emit one whole-root-parent replacement; expand mode emits the actual selected flat occurrence diff. Neither mode may scan accumulated group or view state; this custom contract MUST be pinned by a scale canary.

## Enforced by (tests)

NONE-FOUND

## Implementation

planned `CollectBy` terminal evaluator and delivery canary
