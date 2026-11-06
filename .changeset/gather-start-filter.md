---
"jazz-tools": patch
---

`gather({ start })` now seeds the gather with only the rows `start` (or an earlier `where`) matches. Before, the seed filter was dropped and every row of the table seeded the gather, so `start` and `maxDepth` looked ignored.
