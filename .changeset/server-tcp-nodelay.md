---
"jazz-tools": patch
---

Enable TCP_NODELAY on accepted standalone and embedded server connections. This avoids a Nagle/delayed-ACK stall between small multiplexed WebSocket writes, which can add roughly 40ms to remote reads on Linux after the alpha.56 transport update. Wire and storage formats are unchanged.
