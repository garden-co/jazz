---
"cojson": patch
---

Added lazy loading optimization for CoValues from storage. When handling `load` requests, the server now first loads only the `knownState` (header presence + session counters) to check if the peer already has all content. If so, it responds with a `known` message without loading the full CoValue, reducing memory usage and improving response times. This is particularly beneficial for sync servers handling many CoValues where clients often already have the latest data.
