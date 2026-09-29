---
"jazz-tools": patch
---

Compile write-policy support views with many policy branches faster: lowering no longer copies the table schema (with every policy branch) into each step, derives the parameter domain once per query instead of once per branch, and derives each graph node's output fields once.
