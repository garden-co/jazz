---
"jazz-tools": patch
---

Jazz now uses SHA-256 for every hash it computes, replacing BLAKE3. Large-value chunk nodes are identified by plain SHA-256 over their encoding, so object stores such as S3 and R2 can verify direct client uploads against the node hash, and JavaScript can compute any Jazz id with WebCrypto. Schema hashes change too, so `jazz-tools validate` rewrites the hashes in your generated `migrations/index.ts`. Local stores from earlier builds of this alpha are not readable by this one: delete the local store and resync.
