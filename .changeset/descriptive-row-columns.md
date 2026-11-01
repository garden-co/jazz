---
"jazz-tools": patch
---

The store's own row columns now have descriptive names: `_branch`, `_clock`, `_writer`, `_state`, `_base`, `_touched`, and `_partition` reserved for sharded deployments. An app column can no longer take any of these names (it could take `_b`, `_t`, `_w`, `_st`, `_cols` and `_p` again). Local stores from earlier builds of this alpha are not readable by this one: delete the local store and resync.
