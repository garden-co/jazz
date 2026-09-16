---
"jazz-tools": patch
---

Register mutation waits with the Rust runtime before the upstream connection is ready, so writes made during connection startup can still settle correctly.

[Implementation](https://github.com/garden-co/jazz/commit/d084613778f01f213490a4b6f1826d8ad0cad118).
