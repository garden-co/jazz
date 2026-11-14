---
"jazz-tools": patch
---

The standalone `jazz-server` (and any server built with its `ServerBuilder` and served from a listener) no longer closes a WebSocket as idle while a large message is still arriving. Its 60 s idle timeout now counts any bytes from the client, not only whole messages, so a multi-MB upload on a slow uplink completes instead of being closed and resent in a loop. A client that sends nothing, or stops partway through a message, is still closed. Sockets carried by an embedding `createJazz` host (Node `ws`, Vercel, the HTTP stream) are unchanged and still judge idleness per whole message. Reported and diagnosed by Xelson.
