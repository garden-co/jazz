---
"jazz-tools": patch
---

Direct write conflicts now resolve with MRCA-based per-column LWW for visible merge previews and merge-on-write rebases, including accepted transactional rows.

Visible rows also persist compact winner provenance ordinals so tier-aware reads can reuse merged previews without re-walking row history when tiers have already converged.
