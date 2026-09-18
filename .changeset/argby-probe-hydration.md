---
"jazz-tools": patch
---

Avoid building a retained ArgMin/ArgMax candidate index for one-shot snapshot probes. Subscription hydration still seeds incremental state, including when a probe populated the shared result cache first.
