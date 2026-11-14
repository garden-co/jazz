---
"jazz-tools": patch
---

The sync server no longer closes a WebSocket as idle while a large message is still arriving. Its 60 s idle timeout now counts any bytes from the client, not only whole messages, so a multi-MB upload on a slow uplink completes instead of being closed and resent in a loop. A client that sends nothing, or stops partway through a message, is still closed. Reported and diagnosed by Xelson.
