---
"jazz-tools": patch
"jazz-napi": patch
---

vercel(): the Sandbox core now runs on the Node addon (`@garden-co/jazz-napi-linux-x64-gnu`) through its new `serveCore`, with the same connection and memory limits and graceful drain as before, instead of the `jazz-tools server` executable. `coreTarballUrl` overrides where the Sandbox downloads that addon from.
