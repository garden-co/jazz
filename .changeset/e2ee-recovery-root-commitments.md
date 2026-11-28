---
"jazz-tools": patch
---

End-to-end encryption: registering a recovery root (an app account's recovery phrase or passkey, or a device account's secret at its first start) now also writes a post-quantum commitment for the root, in the new public `__e2ee_commitments` system table: an SLH-DSA-SHA2-128s public key derived from the recovery secret, signed by the root. Users see no new step, since the secret is already in hand at registration. Nothing reads or checks commitments yet, so behaviour is otherwise unchanged; a later release will use them to verify accounts after a break of today's signatures. The table is added to version 1 of the encryption system tables, which has not been released. The exported recovery-material fallback, which had no implementation, is removed from the protocol spec.
