---
"jazz-tools": patch
---

Register mutation waits with the Rust runtime before the upstream connection is ready, so writes made during connection startup can still settle correctly.
