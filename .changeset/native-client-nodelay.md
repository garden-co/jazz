---
"jazz-tools": patch
---

Disable Nagle (TCP_NODELAY) on native Node and React Native client WebSocket connections so small sync frames are not delayed by the server's delayed ACK.
