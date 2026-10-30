---
"jazz-tools": patch
---

A Global wait or read issued right after a short server outage no longer rejects with a stale "websocket closed" error while the client is about to reconnect. The outage is now published only when a reconnect attempt fails after the link has been down for 7.5 s, and retries run at up to 1 s intervals until then.
