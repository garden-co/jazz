---
"jazz-tools": patch
---

A JSON column's `uniqueItems` check now treats 0 and -0.0 as equal in arrays of any length, natively and in the browser. Before, native arrays of more than 15 items were checked with a randomly seeded hash, so the verdict on such an array could change from run to run.
