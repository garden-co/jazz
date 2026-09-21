---
"jazz-tools": patch
---

Use wire protocol v4 for deployment-aware catalogue policy snapshots and complete exclusive transaction evidence. Wire v3 peers now fail the handshake explicitly; upgrade clients, native runtimes, and Edge/Core servers together. Hosted clients require a compatible server deployment. Exclusive evidence uses the independent `jazz-exclusive-evidence.v1` storage codec; legacy pending records remain blocked until exact authored evidence is available.
