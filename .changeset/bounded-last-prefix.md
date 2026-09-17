---
"jazz-tools": patch
---

Bound latest-record storage lookups to one visible entry instead of collecting the entire matching history prefix, reducing repeated-update cost for rows with deep histories.
