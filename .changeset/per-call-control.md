---
"jazz-tools": patch
---

Per-call cancellation and timeouts for the Rust client and the native bindings (contract 2, additive).

- **Rust (behaviour change):** dropping a call's future (as `tokio::time::timeout` does), a `JazzSession` operation's included, now ends its waits on the server on the client's thread, not only the caller's wait. What it wrote may still apply, and a refusal of it is still reported to `mutation_errors()` and to the next `wait_for_sync()`. A `CallControl` (`CallControl::new(timeout)`, `cancel()`) and `Jazz::controlled(control)` cut calls short explicitly: they fail `Error::Interrupted`, or `Error::StillQueued` when the call committed a write locally. A call made once its control has ended does nothing. `JazzSession::controlled(control)` does the same for the session's operations (refresh, sign-in, link, sign-out, retry): cut short, they fail `Error::Interrupted`, and the snapshot reports the failure for `retry`.
- **UniFFI (Swift, Kotlin):** `CallControl(timeoutMs)` with `cancel()`, and `JazzHandle.controlled(control)`, a view on the same app whose calls obey it, and `Session.controlled(control)` for the session's operations. A call it cuts short fails `JazzError.Interrupted { interruption: "cancelled" | "timeout", queued }`.
- **C ABI:** `jazz_call(h, "callBegin", {"timeoutMs"?}) -> {"call"}`, then `"call"` in any call's args, `"cancel" {call}` and `"callEnd" {call}`, on a handle (`jazz_call`) and on a session (`jazz_session_call`). A call that is cut short fails with kind `"interrupted"`, carrying `"reason"` and `"queued"`.
- Rollbacks and upload aborts ignore controls, so a binding can always clean up after a cancel.
- Only calls made with a control can fail with `interrupted`, so older hosts see nothing new.
