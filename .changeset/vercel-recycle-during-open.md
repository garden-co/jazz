---
"jazz-tools": patch
---

`vercel()` no longer stops a core's session while the core is still opening its store. A large store can take tens of seconds to open, and Vercel's router answers "not listening" all that time, which the store used to read as a router glitch after 12 s, so every waiter stopped each new session before it could listen. A session is now recycled only when a check inside the Sandbox finds its core listening while the router still can't reach it, and at most once per session across all function instances. A wake that runs out of time while the core opens says so. The core's launcher logs every stop signal with how long after its launch it arrived.
