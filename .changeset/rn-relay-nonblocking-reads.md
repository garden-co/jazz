---
"jazz-rn": patch
"jazz-tools": patch
---

React Native: subscription drains, ticks, subscribe/unsubscribe, upload status and the slow-query report no longer wait for the relay's owner thread, so the JS thread stays free while the relay works through a burst of server frames. A subscription that fails to open now arrives as a rejection through its events, carrying the core error's code.
