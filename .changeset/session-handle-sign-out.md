---
"jazz-tools": patch
---

A handle from a session (`JazzSession::jazz()`, jazz-ffi's `session.jazz()`, Go's `Session` DB) refuses `sign_out` and `link` (`Error::Unsupported`, kind `notSupported`) and stays open. They used to sign out or change the client's user behind the session's back, leaving the session `ready` on a client that no longer worked. Use the session's `sign_out` and `link_device_account`.
