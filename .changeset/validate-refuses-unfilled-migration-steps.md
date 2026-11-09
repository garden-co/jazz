---
"jazz-tools": patch
---

`jazz-tools validate` now refuses a migration that still has a step its generated stub left to fill in, naming the file and line, instead of passing it to fail at deploy. Generated stubs fill a required column they add or drop from its declared `.default()`, so they leave such a step only where no default is declared. Policy and schema errors now read in schema terms: comparing `session.user` with a string column says it is the account's id and belongs in an `s.uuid()` column, and a join on a column that isn't declared as a reference says which table it should reference and how to declare it with `s.rel(...)`.
