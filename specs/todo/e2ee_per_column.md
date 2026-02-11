# End-to-End Encryption (Per-Column) — TODO

Per-column E2EE with simpler key management than Jazz 1.

## Overview

E2EE is applied at the column level, not the row or table level:

- Developers mark specific columns as encrypted in the schema
- Values are encrypted on the client before sync; the server only sees ciphertext + size
- Unencrypted columns (including metadata columns) remain queryable and indexable
- Key management should be dramatically simpler than Jazz 1's group-based approach

## Design Constraints

- **Server sees**: encrypted blob size, unencrypted column values, row structure
- **Server cannot see**: encrypted column values
- **Encrypted columns cannot be**: indexed, filtered, sorted, or compressed by the server
- **Trade-off**: encrypted files/blobs are more expensive (no compression, no dedup on server)

## Key Management

Jazz 1's E2EE key management (based on ownership groups) was complex and made migration difficult. The new approach should be simpler:

- Per-user keys? Per-table keys? Per-app keys?
- Key rotation without re-encrypting all data
- Sharing encrypted data between users (key exchange)
- Recovery when a user loses their key

## Open Questions

- Which encryption algorithm? (AES-GCM, XChaCha20-Poly1305?)
- Key derivation: from user password, device key, or external KMS?
- How does E2EE interact with schema lenses? (Encrypted columns can't be transformed server-side)
- Can the sync server enforce upload limits on encrypted blobs without seeing content?
- Migration path from Jazz 1 E2EE (different key model entirely)
- Column-level granularity vs. "encrypted table" shorthand for tables where all data columns are encrypted?
