---
"jazz-tools": patch
"jazz-wasm": patch
"jazz-rn": patch
"jazz-napi": patch
---

Publish descriptor-identified terminal payload layouts for native subscription events. WASM emits their layouts as plain JavaScript objects, and root terminal payloads hydrate with their root descriptors while event layout tables remain descendant-only. Numeric React Native relay ABI 3 adds event envelopes and maps inserted or updated row fields by logical identity instead of physical slot order; the stable `NATIVE_RELAY_ABI_V1` export name carries the current numeric version.
