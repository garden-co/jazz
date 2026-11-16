---
"jazz-tools": patch
---

**Breaking:** the experimental `jazz-tools/e2ee`, `jazz-tools/e2ee/browser` and `jazz-tools/e2ee/native` crypto entry points are removed ahead of end-to-end encryption's Rust implementation, which every binding will share. They were never a supported feature; `libsodium` and `libsodium-wrappers` are no longer development dependencies.
