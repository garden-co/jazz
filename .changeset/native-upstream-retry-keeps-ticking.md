---
"jazz-rn": patch
---

Keep React Native foreground ticks running when an established native upstream connection drops and the socket worker reconnects, instead of throwing a recurring `Jazz native foreground runtime failed during tick` on a healthy `Db`. The outage is reported as disconnected by `nativeConnectionStatus()` until the next successful connection. A server that rejects the session, or a socket worker that gives up, still fails the tick, and `reconnectNativeUpstream()` now replaces a worker that gave up instead of leaving its failure latched.
