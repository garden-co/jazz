---
"jazz-tools": patch
---

An `s3()` blob store that rejects the configured keys now stops the server at startup with a clear error. Before, uploads seemed to succeed while nothing ever reached the bucket, and nothing was logged. A bucket that is down or slow at startup still lets the server start; the store retries in the background. A prefix that another store owns is also refused at startup.

A Jazz server running inside a Node process (`createJazz` with its own core, `sqlite()` storage, or `jazz-tools` serve) now prints its warnings and errors to stderr. Before, they went nowhere unless a telemetry collector was configured. `RUST_LOG` adds more detail as usual.
