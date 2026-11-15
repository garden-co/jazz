---
"jazz-tools": patch
---

Let an idle browser worker go idle. A connected worker no longer re-arms its server pump and tab pumps after every core tick (about 60 times a second, forever); instead the core asks the host for a tick at the next deadline it is waiting on, such as a read's `firstLoadRemoteWaitMs` or a link attempt's window, so those deadlines still fire on time without the busy loop.
