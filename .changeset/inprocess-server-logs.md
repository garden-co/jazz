---
"jazz-tools": patch
---

An `s3()` blob store with static keys now checks the bucket at startup and stops the server with a clear error when the bucket refuses for good: wrong keys, a missing bucket, a policy that forbids the prefix, or a prefix that another store owns. Before, uploads seemed to succeed while nothing ever reached the bucket, and nothing was logged. A bucket that is down or does not answer delays startup by at most 3 seconds, then the server starts and the store retries in the background. Keys assumed from a workload identity are not checked at startup.

A Jazz server running inside a Node process (`createJazz` with its own core, `sqlite()` storage, or `jazz-tools` serve) now prints its warnings and errors to stderr; a served core also prints its info logs. Before, they went nowhere unless a telemetry collector was configured. `RUST_LOG` replaces the default filter.
