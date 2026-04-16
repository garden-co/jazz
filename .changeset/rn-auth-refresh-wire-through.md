---
"jazz-tools": patch
---

Fix auth-refresh regressions on the React Native path and on main-thread direct clients.

- `JazzRnRuntimeAdapter` now forwards `updateAuth` and `onAuthFailure` to the UniFFI binding. Previously JWT rotation was a silent no-op on React Native and server auth-rejection callbacks never fired for the Rust transport.
- `JazzClient.updateAuthToken` now preserves `admin_secret` and `backend_secret` from context when pushing refreshed credentials into the transport. Previously the serialized payload carried only `{ jwt_token }`, silently erasing privileged credentials the transport was connected with on every refresh.
