---
"cojson": patch
---

Fixed deterministic ordering of transactions with equal timestamps by comparing sessionIDs from the end, which provides better performance for sessionIDs that share common prefixes
