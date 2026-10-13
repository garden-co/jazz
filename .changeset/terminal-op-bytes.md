---
"jazz-tools": patch
"jazz-napi": patch
---

Speed up include-subscription child edits: native bindings hand terminal-operation bytes to JavaScript as `Uint8Array`s, React Native decodes them without a per-byte round trip, and large multi-table sync streams no longer re-decode row descriptors. `jazz-napi`'s `SubscriptionTerminalOperation`, `KeyPathSegment` and terminal edit types now carry keys and values as `Uint8Array` instead of `Array<number>`.
