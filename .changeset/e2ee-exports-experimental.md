---
"jazz-tools": patch
---

`jazz-tools/e2ee`, `jazz-tools/e2ee/browser` and `jazz-tools/e2ee/native` are experimental. They export the low-level crypto building blocks for end-to-end encryption (cell and large-value ciphers, key envelopes, device signers, crypto contexts), not an end-to-end encrypted app yet. Their API may change in any release before 2.0, and they have not been audited, so don't rely on them to protect production data.
