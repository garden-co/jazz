---
"jazz-tools": patch
---

Row versions in a sync message now share one descriptor table: the `JVRR` row blob is version 3, and a row's schema descriptor is written once per message, at its first use, then referenced by later rows instead of repeated. A 1,000-row, 10-column view update drops from 2.40 MB to 458 KB. Version-2 rows are rejected; this ships inside the alpha.60 wire bump, so upgrade clients, relays and Core servers together.
