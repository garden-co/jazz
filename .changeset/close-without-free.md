---
"jazz-tools": patch
---

Closing the Rust client and the native bindings no longer waits for calls that wait on the server.

- **Rust (behaviour change):** `Jazz::shutdown` fails calls waiting on the server or network (`wait_for_sync`, a global tier, remote reads, `for_user`, `link`, `refresh`) with `Error::Stopped` at once, and `Jazz::close(timeout)` when its write drain ends. Before, each could hold it up for 30 seconds. Local work already started still finishes. Their writes may still apply, and a refusal of one whose wait was interrupted is still reported as a mutation error.
- **Rust:** `JazzSession::close` doesn't wait for a sign-in or refresh under way, which fails with `Error::Stopped` and installs no client. A closed session's `Jazz` reports `Error::Stopped`, not `Error::Replaced`, which is now only for a session that goes on.
- **C ABI (additive):** `jazz_call(h, "close", {"timeoutMs"?})` and `jazz_session_call(s, "close", {})` close without freeing and may run beside any other call, so a host can end the calls in flight, wait for them, and then free. Freeing also waits for a token callback that closed the handle from inside.
- **Swift, Kotlin and Go bindings:** `shutdown` on a `for_user` or session handle ends that handle's waiting calls, and closing a session reports `closed`, not `replaced`.
