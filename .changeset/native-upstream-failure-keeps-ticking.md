---
"jazz-tools": patch
"jazz-rn": patch
---

React Native: a native upstream connection that fails for a reason other than the credential (TLS behind a captive portal, a proxy refusing the upgrade) no longer crashes release builds. The failure is reported as connection state, as on the web: the remote link shows as unavailable, local reads and writes keep working, and the link recovers when the connection does.
