---
"jazz-tools": patch
---

Let idle clients go idle. A browser worker or Node client connected to a server no longer re-arms its server pump after every core tick (about 60 times a second, forever), and a React Native relay no longer polls every 25–50 ms. Instead the core asks the host for a tick at the next deadline it is waiting on, such as a read's `firstLoadRemoteWaitMs` or a link attempt's window, so those deadlines still fire on time.
