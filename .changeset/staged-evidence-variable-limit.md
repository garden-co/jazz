---
"jazz-tools": patch
---

A transaction whose permission checks read many of the rows it writes (for example a long chain of rows that each reference the one before) no longer fails on a Durable Object SQLite store with "variable number must be between ?1 and ?100". The check now passes those rows as one parameter.
