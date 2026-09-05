# Jazz Fixture Canaries

This directory is reserved for codec fixtures that are independent of any
binding runtime.

The old direct-call `AbiRuntime` fixtures were removed with the command/event
ABI runtime. Future fixtures should target stable byte payloads directly:

- `AbiRowBatch` descriptor/raw row batches
- `AbiEncodedCellPatch` write/probe payloads
- `AbiSubscriptionStreamChunk` subscription payloads
- wire `WireFrame` envelopes
- WebSocket v1 admission preludes (the one JSON message before binary wire frames)

Fixture generators should use core `Db`/`Node` APIs directly rather than routing
through a binding object manager.
