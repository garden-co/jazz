---
"jazz-tools": patch
---

Account-session websocket connections no longer look up the session registry on every incoming message. They re-check only when the registry changes, so servers with many open connections spend much less time per message. Session revocation still takes effect.
