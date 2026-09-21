---
"jazz-tools": patch
---

Use wire protocol v4 for deployment-aware catalogue policy snapshots and complete exclusive transaction evidence. Wire v3 peers now fail the handshake explicitly; upgrade clients, native runtimes, and Edge/Core servers together. Hosted clients require a compatible server deployment. Storage remains unchanged: pending exclusive transactions with incomplete read evidence remain blocked after restart rather than being uploaded with weaker semantics. Durable restart recovery is deferred to [#3228](https://github.com/garden-co/jazz/issues/3228).
