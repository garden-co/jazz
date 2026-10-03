---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Classify prepared current-result projection failures as `QueryResultProtocol`, downgrade the code to `Internal` for peers without the negotiated feature, and keep other relay rejection reasons unchanged. Older clients must reattach after the server retires the failed route.
