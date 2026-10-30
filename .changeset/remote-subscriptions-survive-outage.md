---
"jazz-tools": patch
---

A remote-tier subscription no longer stops for good when the sync server is unreachable for longer than 7.5 seconds. The client keeps reconnecting, and the subscription picks up changes again once the server is back.
