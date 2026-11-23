---
"jazz-tools": patch
---

`vercel()`: a backend read made while the core wakes no longer fails with "the server did not answer the connection within 10 s" when the wake takes longer than 10 s. The 10 s handshake timeout now starts only once the connection to the core is actually made, and it pauses while a connection that never opened waits for another wake. The wake keeps its own 60 s deadline and its own error. The timeout still applies in full to a core that accepts the connection and never answers, and to every other transport, as before (#6294).
