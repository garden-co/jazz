---
"jazz-tools": patch
---

A Jazz server running inside a Node process (`createJazz` with its own core, `sqlite()` storage, or `jazz-tools` serve) now prints its warnings and errors to stderr. Before, these went nowhere unless a telemetry collector was configured. For example, an `s3()` store that refused the configured credentials was silent: uploads seemed to succeed, but nothing ever reached the bucket. `RUST_LOG` adds more detail as usual.
