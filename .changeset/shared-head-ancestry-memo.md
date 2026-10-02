---
"jazz-tools": patch
---

Merge-head updates now reuse complete row-ancestry checks across concurrent heads, avoiding repeated walks of their shared history. Merge results are unchanged, and incomplete history is never reused as a settled negative.
