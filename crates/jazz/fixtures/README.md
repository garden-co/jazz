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
- `relation_shape_id_preimage.json`: canonical shape-identity preimages
  (`jazz-query-v0` plus a retained relation's `jazz-relation-v1` tree) and the
  resulting shape ids. Receivers recompute shape ids on registration, so these
  bytes are a cross-release contract.

Fixture generators should use core `Db`/`Node` APIs directly rather than routing
through a binding object manager.
