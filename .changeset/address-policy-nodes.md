---
"jazz-tools": patch
---

Core: three new permission-policy nodes, for Jazz's upcoming end-to-end encryption tables. `InheritsFrom` inherits a policy from the row named by a (table identity, row ID) pair. `table_id('T')` is a table's catalogue identity, and an identity a store doesn't know yet grants nothing. `InsertedByThisTransaction` holds only for a row the checking transaction itself freshly inserts. A deploy refuses an `InheritsFrom` that reaches another `InheritsFrom`, or one reached from a read policy. Existing apps behave as before, and a permission bundle that doesn't use the new nodes keeps its ID; the only cost is that a transaction now records the rows it freshly inserts.
