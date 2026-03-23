# Unified Binary Sync Protocol

## What

Single binary protocol for network sync (client-server), worker communication (main thread-worker), and peer replication (server-server).

## Why

Currently network sync uses HTTP POST + SSE, worker bridge uses postMessage with typed arrays. Multiple serialization formats, parsers, and framing — duplicated work and inconsistent behavior.

## Who

All Jazz consumers (one protocol means fewer bugs and easier debugging across all transport layers).

## Rough appetite

big

## Notes

Open questions: WebSocket vs HTTP/2 vs both as transports with shared framing, message framing format, backpressure/flow control, compression strategy, interaction with existing SSE path. Comlink spike exists in dev-tools but core protocol is still HTTP+SSE.
