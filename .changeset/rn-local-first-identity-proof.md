---
"jazz-tools": patch
---

Fix `getLocalFirstIdentityProof` on React Native by minting the proof token through `jazz-rn` instead of the (unavailable) WASM module, restoring the local-first → upgrade-to-account sign-up flow on RN.
