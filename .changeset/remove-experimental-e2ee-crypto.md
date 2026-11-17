---
"jazz-tools": patch
"jazz-napi": patch
---

**Breaking:** the experimental `jazz-tools/e2ee`, `jazz-tools/e2ee/browser` and `jazz-tools/e2ee/native` crypto entry points are removed ahead of end-to-end encryption's Rust implementation, which every binding will share. They were published in jazz-tools 2.0.0-alpha.60 through alpha.66 but never documented or supported. `jazz-napi` drops the libsodium functions behind them (`e2eeSodium*`, `E2eeSodiumStream`) and its `libsodium-sys-stable` dependency, and `libsodium` and `libsodium-wrappers` are no longer development dependencies of jazz-tools.
