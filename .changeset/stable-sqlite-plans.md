---
"jazz-tools": patch
---

Stop SQLite from re-preparing every cached read on each run. Rebinding a `LIMIT ?` parameter expired the statement, so live queries were parsed and planned again whenever they re-ran. Every engine connection now enables SQLite's query planner stability guarantee.
