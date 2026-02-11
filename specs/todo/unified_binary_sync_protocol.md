# Unified Binary Sync Protocol — TODO

Single binary protocol for network sync and worker communication.

## Overview

Currently network sync uses HTTP POST with binary bodies + SSE for streaming, and the worker bridge uses `postMessage` with typed arrays. Unify these into a single binary protocol that works across:

- Client ↔ Server (WebSocket or HTTP/2 streams)
- Main thread ↔ Worker (`postMessage` with `Transferable`)
- Server ↔ Server (peer replication)

Benefits: one serialization format, one parser, consistent framing, easier debugging.

## Open Questions

- WebSocket vs. HTTP/2 vs. keep both as transports with shared framing?
- Message framing: length-prefixed, varint, or self-describing?
- Backpressure and flow control across different transports
- Compression (per-message vs. stream-level)?
- How does this interact with the existing SSE → binary streaming path?
