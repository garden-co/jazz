---
"jazz-tools": patch
---

Keep slow-query reports to a readable size. A report line now shows the first 40 steps of the query plan, with the count of the rest, and the first 2,000 characters of the SQL, with its length. A policy that expanded into thousands of `UNION ALL` branches used to make a single `console.warn` line tens of megabytes long. `createPolicyTestApp` and `startTestJazz` now take `slowQueryThresholdMs` for their test clients (`0` turns the slow-query log off).
