---
"cojson": patch
"cojson-transport-ws": patch
---

Fixed a bug in `BatchedOutgoingMessages.push()` where messages sent via the fast path (when WebSocket is ready and not backpressured) were added to the queue but never removed. This caused the `pushed - pulled` metric to grow indefinitely even when the system was idle.

The fix moves the `queue.push()` call to only happen when taking the slow path (when WebSocket is not ready), since the fast path sends messages directly without using the queue.
