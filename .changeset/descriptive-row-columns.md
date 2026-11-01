---
"jazz-tools": patch
---

The store's own row columns now have descriptive names: `_branch`, `_clock`, `_writer`, `_state`, `_base`, `_touched`, and `_partition` reserved for sharded deployments. An app column can no longer take any of these names.
