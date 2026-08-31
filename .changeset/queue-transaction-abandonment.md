---
"jazz-tools": patch
---

Make dropped Rust transaction handles abandon promptly without blocking, preserve cleanup queued behind node work, and close transaction admission before terminalizing all open work during database shutdown.
