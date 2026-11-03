---
"jazz-tools": patch
---

Large-value chunk nodes are now identified by plain SHA-256 over their encoding (previously domain-separated BLAKE3), so object stores such as S3 and R2 can verify direct client uploads against the node hash. Local stores from earlier builds of this alpha that hold large values are not readable by this one: delete the local store and resync.
