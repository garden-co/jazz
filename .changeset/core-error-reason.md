---
"jazz-tools": patch
"jazz-rn": patch
---

Core errors from the WASM binding, NAPI promise rejections and React Native now carry their stable reason name as `error.reason` (for example `authorization_denied`), and rejected writes are classified by it instead of by their message. The React Native native relay ABI is now 3, so the JavaScript bundle and native build must match.
