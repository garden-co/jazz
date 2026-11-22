---
"jazz-tools": patch
---

`vercel()`: a backend no longer stops the core Sandbox session it has just woken. When the core is stopped and waking it takes longer than the 10 s handshake timeout, the reconnects that give up while they wait no longer count as connections that "never reached the core". A wake that brings up a new session also starts that count afresh. Before, those reconnects counted against the session, so the first connection to the woken core that didn't open in time made the instance stop the session it had just woken and wake it again. With every warm instance doing the same, backends could keep failing long after the core was back, until a redeploy (#6028).
