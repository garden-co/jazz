---
"jazz-tools": patch
---

A write made through a schema version the server has not deployed yet is no longer lost in silence. Offline it stays pending and uploads under its version once the server has it; if the server still lacks that version when the write syncs, the server refuses it with the new reason `schema_version_not_deployed` (code `write_rejected`), which points to an app released ahead of its schema. A store that wrote before it became a replica now keeps those rows and refuses its upstream with an explicit error, rather than losing them on its first connect.
