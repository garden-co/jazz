---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-napi": patch
"jazz-rn": patch
---

Classify prepared current-result projection failures as `QueryResultProtocol` in the base wire-v5 vocabulary. Retire only the affected served route and terminate its query stream; applications can resubscribe to recover the committed result. Other relay rejection reasons, low-level subscription continuity, and remote-window fallback behaviour are unchanged. Wire v5 is unreleased; the frozen v4 encoding is unchanged and older wire versions are rejected at session negotiation.
