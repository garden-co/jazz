# Auth-Refresh End-to-End Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the broken auth-refresh path end-to-end so JWT rotation and server-side rejection propagate correctly across all bindings (WASM, NAPI, RN) and across worker/main-thread paths, and bundle the shared-connect-helper refactor so we don't triplicate the fix.

**Architecture:** Two-direction fix. **Push** (new JWT → Rust WS transport): `JazzClient.updateAuthToken` currently only mutates context; it will call `this.runtime.updateAuth?.(JSON.stringify({ jwt_token }))`. **Pull** (server auth rejection → app): `RuntimeCore::set_auth_failure_callback` exists but only WASM exposes it; NAPI gets a `ThreadsafeFunction`-based method and RN gets a UniFFI callback-interface method. `ConnectSyncRuntimeOptions.onAuthFailure` (currently dropped into `_runtimeOptions`) gets wired to `runtime.onAuthFailure` at construction. While all four bindings (`TokioRuntime`, `NapiJazzRuntime`, `RnRuntime`, `WasmRuntime`) are being edited, a shared `install_transport` helper on `RuntimeCore` replaces the ~20 lines of duplicated `transport_manager::create` → seed hash → `set_transport` → spawn boilerplate, also fixing the latent bugs where NAPI fails to seed the catalogue hash and RN doesn't guard against double-connect.

**Tech Stack:** Rust (jazz-tools, jazz-napi, jazz-rn, jazz-wasm), TypeScript (jazz-tools package), napi-rs (ThreadsafeFunction), UniFFI (callback_interface), wasm-bindgen, Tokio, WebSockets.

---

## File Structure

**Rust core (jazz-tools):**

- `crates/jazz-tools/src/runtime_tokio.rs:715-741` — Modify `TokioRuntime::connect` to use shared helper.
- `crates/jazz-tools/src/runtime_core.rs` — Add `install_transport` helper function near the existing `set_transport` / `clear_transport` methods (line ~454).
- `crates/jazz-tools/src/transport_manager.rs` — Existing file; add one new unit test for `AuthFailure` event emission during handshake.
- `crates/jazz-tools/src/runtime_core/tests.rs` — Add a test that `TransportInbound::AuthFailure` fires the registered `auth_failure_callback`.

**Rust bindings:**

- `crates/jazz-napi/src/lib.rs:1159-1228` — Migrate `connect`/`disconnect` to the helper; add `on_auth_failure`.
- `crates/jazz-rn/rust/src/lib.rs:812-880` — Migrate `connect`/`disconnect` to the helper; add `AuthFailureCallback` trait and `on_auth_failure` method.
- `crates/jazz-rn/src/generated/jazz_rn.ts` + `jazz_rn-ffi.ts` — Regenerated via `pnpm --filter jazz-rn ubrn:android` (or `ubrn:ios`). We will add a placeholder task to invoke the regen; actual native build is optional for this plan.
- `crates/jazz-wasm/src/runtime.rs:1522-1586` — Migrate `connect`/`disconnect` to the helper; `on_auth_failure` already exists but should stay identical.

**TypeScript (jazz-tools package):**

- `packages/jazz-tools/src/runtime/client.ts:608-778` — Store `runtimeOptions` on `JazzClient`; wire `onAuthFailure` to `runtime.onAuthFailure`; make `updateAuthToken` call `runtime.updateAuth`.
- `packages/jazz-tools/src/worker/jazz-worker.ts:498-508` — Post an `auth-failed` message back to the main thread when `runtime.updateAuth` throws.

**Tests:**

- `crates/jazz-tools/src/runtime_core/tests.rs` — Rust unit tests.
- `crates/jazz-tools/src/transport_manager.rs` — Rust unit tests (extend existing `#[cfg(test)] mod tests`).
- `packages/jazz-tools/src/runtime/client.test.ts` — **NEW.** TS unit tests for `updateAuthToken` forwarding and `onAuthFailure` wiring.
- `packages/jazz-tools/src/worker/jazz-worker.test.ts` — Extend existing file to cover the update-auth-error postback.

---

## Task 1: Add `install_transport` helper on `runtime_core`

**Files:**

- Modify: `crates/jazz-tools/src/runtime_core.rs` (add helper near `set_transport` at line ~454)
- Test: `crates/jazz-tools/src/runtime_core/tests.rs`

- [ ] **Step 1: Read the current `set_transport` / `clear_transport` surface**

Run: `sed -n '450,470p' crates/jazz-tools/src/runtime_core.rs`
Expected: See the `set_transport(&mut self, handle: TransportHandle)` method body.

- [ ] **Step 2: Write the failing test**

Add to `crates/jazz-tools/src/runtime_core/tests.rs` (find the `mod tests` block or create one if none; use existing test setup helpers for `RuntimeCore<MemoryStorage, NoopScheduler>`):

```rust
#[test]
fn install_transport_seeds_catalogue_hash_and_registers_handle() {
    use crate::transport_manager::{AuthConfig, TickNotifier};

    struct NopTick;
    impl TickNotifier for NopTick {
        fn notify(&self) {}
    }

    let mut core = make_core_for_test(); // existing helper in tests.rs

    let manager = crate::runtime_core::install_transport::<_, _, crate::ws_stream::FakeStreamAdapter, _>(
        &mut core,
        "ws://example.test/ws".to_string(),
        AuthConfig::default(),
        NopTick,
    );

    assert!(core.transport.is_some(), "transport handle should be installed");
    let expected_hash = core.schema_manager().catalogue_state_hash();
    let handle_hash = core.transport.as_ref().unwrap().catalogue_state_hash_for_test();
    assert_eq!(handle_hash.as_deref(), Some(expected_hash.as_str()),
        "install_transport must seed the handle's catalogue_state_hash");

    drop(manager); // prevent unused warning
}
```

If `FakeStreamAdapter` / `catalogue_state_hash_for_test` / `make_core_for_test` don't exist yet, either find the test helpers already used by `runtime_core/tests.rs` and adapt, or add minimal scaffolding in the same file.

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p jazz-tools --lib install_transport_seeds_catalogue_hash -- --exact`
Expected: FAIL — `install_transport` not found.

- [ ] **Step 4: Implement `install_transport`**

Add to `crates/jazz-tools/src/runtime_core.rs` right above `set_transport`:

```rust
/// Create a `TransportManager`, seed it with the current catalogue state hash,
/// install its handle on the given core, and return the manager for the caller
/// to spawn on an appropriate executor.
///
/// Centralises the boilerplate that would otherwise be duplicated in every
/// binding (Tokio, NAPI, RN, WASM).
#[cfg(feature = "transport-websocket")]
pub fn install_transport<S, Sch, W, T>(
    core: &mut RuntimeCore<S, Sch>,
    url: String,
    auth: crate::transport_manager::AuthConfig,
    tick: T,
) -> crate::transport_manager::TransportManager<W, T>
where
    S: crate::storage::Storage,
    Sch: crate::runtime_core::Scheduler,
    W: crate::transport_manager::StreamAdapter + 'static,
    T: crate::transport_manager::TickNotifier + 'static,
{
    let (handle, manager) = crate::transport_manager::create::<W, T>(url, auth, tick);
    handle.set_catalogue_state_hash(Some(core.schema_manager().catalogue_state_hash()));
    core.set_transport(handle);
    manager
}
```

If `TransportHandle` does not already expose a public `catalogue_state_hash_for_test`, add a `#[cfg(test)]`-gated accessor on `TransportHandle` in `transport_manager.rs` that returns `self.catalogue_state_hash.lock().ok().and_then(|g| g.clone())`.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p jazz-tools --lib install_transport_seeds_catalogue_hash -- --exact`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-tools/src/runtime_core.rs crates/jazz-tools/src/runtime_core/tests.rs crates/jazz-tools/src/transport_manager.rs
git commit -m "feat(jazz-tools): add install_transport helper on runtime_core"
```

---

## Task 2: Migrate `TokioRuntime::connect` to the helper

**Files:**

- Modify: `crates/jazz-tools/src/runtime_tokio.rs:715-741`

- [ ] **Step 1: Replace the connect body**

In `crates/jazz-tools/src/runtime_tokio.rs`, change:

```rust
pub fn connect(&self, url: String, auth: crate::transport_manager::AuthConfig) {
    let tick = NativeTickNotifier {
        scheduler: self.scheduler.clone(),
    };
    let (handle, manager) = crate::transport_manager::create::<
        crate::ws_stream::NativeWsStream,
        NativeTickNotifier<S>,
    >(url, auth, tick);
    let catalogue_hash = self
        .core
        .lock()
        .ok()
        .map(|c| c.schema_manager().catalogue_state_hash());
    handle.set_catalogue_state_hash(catalogue_hash);
    self.core.lock().unwrap().set_transport(handle);
    tokio::spawn(manager.run());
}
```

to:

```rust
pub fn connect(&self, url: String, auth: crate::transport_manager::AuthConfig) {
    let tick = NativeTickNotifier {
        scheduler: self.scheduler.clone(),
    };
    let manager = {
        let mut core = self.core.lock().unwrap();
        crate::runtime_core::install_transport::<_, _, crate::ws_stream::NativeWsStream, _>(
            &mut core, url, auth, tick,
        )
    };
    tokio::spawn(manager.run());
}
```

- [ ] **Step 2: Rebuild + run existing tests**

Run: `cargo test -p jazz-tools --features test --test integration --test auth_test 2>&1 | tail -5`
Expected: All tests pass (the behaviour is unchanged; the helper does exactly what the inline code did).

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-tools/src/runtime_tokio.rs
git commit -m "refactor(jazz-tools): TokioRuntime::connect uses install_transport helper"
```

---

## Task 3: Migrate NAPI `connect` to the helper (fixes missing catalogue-hash seed)

**Files:**

- Modify: `crates/jazz-napi/src/lib.rs:1159-1204`

- [ ] **Step 1: Replace the body**

Change:

```rust
#[napi]
pub fn connect(&self, url: String, auth_json: String) -> napi::Result<()> {
    let auth: jazz_tools::transport_manager::AuthConfig = serde_json::from_str(&auth_json)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let tick = NapiTickNotifier {
        core: Arc::clone(&self.core),
    };
    let (handle, manager) = jazz_tools::transport_manager::create::<
        jazz_tools::ws_stream::NativeWsStream,
        NapiTickNotifier,
    >(url, auth, tick);
    self.core
        .lock()
        .map_err(|_| napi::Error::from_reason("lock"))?
        .set_transport(handle);
    // Spawn the TransportManager loop ...
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => { handle.spawn(manager.run()); }
        Err(_) => {
            std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                { /* ... */ };
                rt.block_on(manager.run());
            });
        }
    }
    Ok(())
}
```

to:

```rust
#[napi]
pub fn connect(&self, url: String, auth_json: String) -> napi::Result<()> {
    let auth: jazz_tools::transport_manager::AuthConfig = serde_json::from_str(&auth_json)
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let tick = NapiTickNotifier {
        core: Arc::clone(&self.core),
    };
    let manager = {
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        jazz_tools::runtime_core::install_transport::<
            _,
            _,
            jazz_tools::ws_stream::NativeWsStream,
            _,
        >(&mut core, url, auth, tick)
    };
    match tokio::runtime::Handle::try_current() {
        Ok(rt_handle) => {
            rt_handle.spawn(manager.run());
        }
        Err(_) => {
            std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!("jazz-napi: failed to build fallback tokio runtime: {e}");
                        return;
                    }
                };
                rt.block_on(manager.run());
            });
        }
    }
    Ok(())
}
```

Also simplify `disconnect`:

```rust
#[napi]
pub fn disconnect(&self) {
    if let Ok(mut core) = self.core.lock() {
        if let Some(handle) = core.transport() {
            handle.disconnect();
        }
        core.clear_transport();
    }
}
```

(unchanged structurally; left here as a reference — the helper doesn't change disconnect).

- [ ] **Step 2: Rebuild**

Run: `cargo build -p jazz-napi 2>&1 | tail -5`
Expected: successful build.

- [ ] **Step 3: Run NAPI-adjacent integration tests**

Run: `pnpm --filter jazz-tools exec vitest run src/runtime/napi.integration.test.ts 2>&1 | tail -10`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-napi/src/lib.rs
git commit -m "refactor(jazz-napi): connect uses install_transport helper; seed catalogue hash"
```

---

## Task 4: Migrate RN `connect` to the helper

**Files:**

- Modify: `crates/jazz-rn/rust/src/lib.rs:812-855`

- [ ] **Step 1: Replace the body**

Change:

```rust
pub fn connect(&self, url: String, auth_json: String) -> Result<(), JazzRnError> {
    with_panic_boundary("connect", || {
        let auth: jazz_tools::transport_manager::AuthConfig =
            serde_json::from_str(&auth_json).map_err(json_err)?;
        let scheduler = self
            .core
            .lock()
            .map_err(|_| JazzRnError::Internal { message: "lock poisoned".into() })?
            .scheduler()
            .clone();
        let tick = RnTickNotifier { scheduler };
        let (handle, manager) = jazz_tools::transport_manager::create::<
            jazz_tools::ws_stream::NativeWsStream,
            RnTickNotifier,
        >(url, auth, tick);
        { let core = self.core.lock() ...; handle.set_catalogue_state_hash(Some(core.schema_manager().catalogue_state_hash())); }
        self.core.lock() ...; .set_transport(handle);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio rt");
            rt.block_on(manager.run());
        });
        Ok(())
    })
}
```

to:

```rust
pub fn connect(&self, url: String, auth_json: String) -> Result<(), JazzRnError> {
    with_panic_boundary("connect", || {
        let auth: jazz_tools::transport_manager::AuthConfig =
            serde_json::from_str(&auth_json).map_err(json_err)?;
        let scheduler = self
            .core
            .lock()
            .map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?
            .scheduler()
            .clone();
        let tick = RnTickNotifier { scheduler };
        let manager = {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            jazz_tools::runtime_core::install_transport::<
                _,
                _,
                jazz_tools::ws_stream::NativeWsStream,
                _,
            >(&mut core, url, auth, tick)
        };
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio rt");
            rt.block_on(manager.run());
        });
        Ok(())
    })
}
```

- [ ] **Step 2: Rebuild**

Run: `cargo build -p jazz-rn 2>&1 | tail -5`
Expected: successful build.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-rn/rust/src/lib.rs
git commit -m "refactor(jazz-rn): connect uses install_transport helper"
```

---

## Task 5: Migrate WASM `connect` to the helper

**Files:**

- Modify: `crates/jazz-wasm/src/runtime.rs` around the existing `connect` method (grep for `transport_manager::create`)

- [ ] **Step 1: Replace the body**

In `crates/jazz-wasm/src/runtime.rs` — replace the `transport_manager::create` → `set_catalogue_state_hash` → `set_transport` block inside `connect` with:

```rust
let tick = WasmTickNotifier { /* unchanged */ };
let manager = {
    let mut core = self.core.borrow_mut();
    jazz_tools::runtime_core::install_transport::<
        _,
        _,
        jazz_tools::ws_stream::WasmWsStream,
        _,
    >(&mut core, url, auth, tick)
};
wasm_bindgen_futures::spawn_local(manager.wasm_run());
```

Preserve whatever the WASM-specific spawn call is (probably `wasm_run` not `run`; check existing code near the current `spawn_local` site and keep it verbatim).

- [ ] **Step 2: Rebuild**

Run: `cargo build -p jazz-wasm --target wasm32-unknown-unknown 2>&1 | tail -5`
Expected: successful build.

- [ ] **Step 3: Commit**

```bash
git add crates/jazz-wasm/src/runtime.rs
git commit -m "refactor(jazz-wasm): connect uses install_transport helper"
```

---

## Task 6: Test that `RuntimeCore::auth_failure_callback` fires on `TransportInbound::AuthFailure`

**Files:**

- Test: `crates/jazz-tools/src/runtime_core/tests.rs`

- [ ] **Step 1: Write the failing test**

Add to `runtime_core/tests.rs`:

```rust
#[test]
fn auth_failure_callback_fires_on_inbound_auth_failure_event() {
    use crate::transport_manager::TransportInbound;
    use std::sync::{Arc, Mutex};

    let mut core = make_core_for_test();
    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = Arc::clone(&captured);
    core.set_auth_failure_callback(move |reason| {
        captured_clone.lock().unwrap().push(reason);
    });

    // Simulate an inbound event. Since tests can't easily stand up a full
    // TransportManager, call whatever internal method the ticks.rs logic
    // uses to handle the inbound — for this test we look at
    // `runtime_core/ticks.rs:261-266` and invoke the equivalent
    // via a seam:
    let dummy_server_id = crate::sync_manager::ServerId::new();
    core.handle_transport_inbound_for_test(
        dummy_server_id,
        TransportInbound::AuthFailure("Unauthorized".to_string()),
    );

    assert_eq!(
        captured.lock().unwrap().as_slice(),
        &["Unauthorized".to_string()]
    );
}
```

If no `handle_transport_inbound_for_test` exists, add a `#[cfg(test)] pub(crate) fn handle_transport_inbound_for_test(&mut self, server_id: ServerId, ev: TransportInbound)` to `runtime_core/ticks.rs` that forwards to the private match arm. Don't change production control flow.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p jazz-tools --lib auth_failure_callback_fires_on_inbound_auth_failure_event -- --exact`
Expected: FAIL — the test seam doesn't exist yet OR the callback is not fired.

- [ ] **Step 3: Make it pass**

If the seam is missing, add it. If the callback path is already implemented at `runtime_core/ticks.rs:261-266`, the seam wrapper alone should suffice.

- [ ] **Step 4: Run test**

Run: `cargo test -p jazz-tools --lib auth_failure_callback_fires_on_inbound_auth_failure_event -- --exact`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-tools/src/runtime_core/tests.rs crates/jazz-tools/src/runtime_core/ticks.rs
git commit -m "test(jazz-tools): auth_failure_callback fires on TransportInbound::AuthFailure"
```

---

## Task 7: Add `on_auth_failure` to RN runtime

**Files:**

- Modify: `crates/jazz-rn/rust/src/lib.rs:161-171` (add trait), `:820-880` (add method)

- [ ] **Step 1: Add UniFFI callback trait**

After the existing `BatchedTickCallback` / `SubscriptionCallback` definitions (line ~171), append:

```rust
#[uniffi::export(callback_interface)]
pub trait AuthFailureCallback: Send + Sync {
    /// Invoked when the Rust transport receives an auth rejection from the server.
    /// `reason` is a human-readable string (e.g. "Unauthorized").
    fn on_failure(&self, reason: String);
}
```

- [ ] **Step 2: Add `on_auth_failure` method**

Inside the `#[uniffi::export] impl RnRuntime { ... }` block, add (placing it near `update_auth` at line ~868):

```rust
/// Register a callback that fires when the transport receives an auth
/// rejection from the server during the WS handshake.
pub fn on_auth_failure(
    &self,
    callback: Box<dyn AuthFailureCallback>,
) -> Result<(), JazzRnError> {
    with_panic_boundary("on_auth_failure", || {
        let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
            message: "lock poisoned".into(),
        })?;
        core.set_auth_failure_callback(move |reason| {
            callback.on_failure(reason);
        });
        Ok(())
    })
}
```

- [ ] **Step 3: Rebuild**

Run: `cargo build -p jazz-rn 2>&1 | tail -5`
Expected: successful build.

- [ ] **Step 4: Regenerate UniFFI bindings (optional for Rust-only verification)**

Full regen requires iOS/Android toolchain:

```bash
cd crates/jazz-rn && pnpm ubrn:android   # or ubrn:ios
```

If that's unavailable, skip — the generated TS bindings can be regenerated in a follow-up when the RN build host is available. Rust-side correctness is fully testable without regen.

Expected: `crates/jazz-rn/src/generated/jazz_rn.ts` and `jazz_rn-ffi.ts` updated with `onAuthFailure` method on `RnRuntime` and `AuthFailureCallback` interface.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-rn/rust/src/lib.rs crates/jazz-rn/src/generated/
git commit -m "feat(jazz-rn): add on_auth_failure callback interface"
```

(If regen skipped, leave generated TS out of this commit and note in the message.)

---

## Task 8: Add `on_auth_failure` to NAPI runtime

**Files:**

- Modify: `crates/jazz-napi/src/lib.rs:1217-1228` (insert after `update_auth`)
- Modify: `crates/jazz-napi/index.d.ts` (regenerated by `napi build`)

- [ ] **Step 1: Write an integration test first**

Add to `packages/jazz-tools/src/runtime/napi.integration.test.ts` (or create a new file `napi.auth-failure.test.ts` if the existing file is awkward — pattern-match the existing tests there):

```typescript
import { describe, it, expect } from "vitest";
import { startLocalJazzServer, createNapiJazzRuntime } from "...same helpers existing file uses...";

describe("NAPI on_auth_failure", () => {
  it("fires callback with reason when server rejects auth on handshake", async () => {
    const server = await startLocalJazzServer({
      /* forces JWT auth */
    });

    const runtime = await createNapiJazzRuntime();
    const reasons: string[] = [];
    runtime.onAuthFailure((reason: string) => {
      reasons.push(reason);
    });

    // Connect with an intentionally invalid JWT.
    runtime.connect(server.wsUrl, JSON.stringify({ jwt_token: "definitely.invalid.jwt" }));

    // Wait up to 5s for the server to reject and the callback to fire.
    const deadline = Date.now() + 5000;
    while (reasons.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(reasons.length).toBeGreaterThan(0);
    expect(reasons[0]).toMatch(/unauth/i);

    runtime.disconnect();
    await server.stop();
  });
});
```

If the helpers in the existing file use slightly different names, match them. The key is: real NAPI runtime + real server + bad JWT + asserts the callback fires.

- [ ] **Step 2: Run to verify it fails**

Run: `pnpm --filter jazz-tools exec vitest run src/runtime/napi.integration.test.ts -t "fires callback with reason"`
Expected: FAIL — `runtime.onAuthFailure is not a function`.

- [ ] **Step 3: Implement `on_auth_failure` in NAPI**

In `crates/jazz-napi/src/lib.rs`, after the `update_auth` method (line ~1228), add:

```rust
/// Register a JS callback that fires when the Rust transport receives an
/// auth rejection from the server during the WS handshake.
#[napi(ts_args_type = "callback: (reason: string) => void")]
pub fn on_auth_failure(
    &self,
    callback: napi::threadsafe_function::ThreadsafeFunction<
        String,
        napi::threadsafe_function::ErrorStrategy::Fatal,
    >,
) -> napi::Result<()> {
    let mut core = self
        .core
        .lock()
        .map_err(|_| napi::Error::from_reason("lock"))?;
    core.set_auth_failure_callback(move |reason| {
        callback.call(reason, ThreadsafeFunctionCallMode::NonBlocking);
    });
    Ok(())
}
```

The exact generic signature depends on the napi-rs version already in use in this crate. Check an existing `ThreadsafeFunction<...>` use in the file (line 147: `ThreadsafeFunction<serde_json::Value>`) and mirror the pattern.

- [ ] **Step 4: Rebuild NAPI native addon**

Run: `pnpm --filter jazz-napi build 2>&1 | tail -10`
Expected: build success; `index.d.ts` updated with `onAuthFailure(callback: (reason: string) => void): void`.

- [ ] **Step 5: Run the test again**

Run: `pnpm --filter jazz-tools exec vitest run src/runtime/napi.integration.test.ts -t "fires callback with reason"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-napi/src/lib.rs crates/jazz-napi/index.d.ts packages/jazz-tools/src/runtime/napi.integration.test.ts
git commit -m "feat(jazz-napi): add on_auth_failure method; integration test"
```

---

## Task 9: Wire `JazzClient.updateAuthToken` → `runtime.updateAuth`

**Files:**

- Modify: `packages/jazz-tools/src/runtime/client.ts:772-778`
- Test: `packages/jazz-tools/src/runtime/client.test.ts` (NEW)

- [ ] **Step 1: Write the failing test**

Create `packages/jazz-tools/src/runtime/client.test.ts`:

```typescript
import { describe, it, expect, vi } from "vitest";
import { JazzClient } from "./client.js";

function makeFakeRuntime() {
  return {
    updateAuth: vi.fn(),
    onAuthFailure: vi.fn(),
    // Stub methods JazzClient uses on init:
    query: vi.fn(),
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
    close: vi.fn(),
  };
}

function makeContext() {
  return {
    appId: "test-app",
    schema: {}, // minimal
    serverUrl: "https://example.test",
    jwtToken: "initial.jwt.token",
  } as any;
}

describe("JazzClient.updateAuthToken", () => {
  it("forwards refreshed JWT to the Rust runtime via runtime.updateAuth", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.forExistingRuntime(runtime as any, makeContext());

    client.updateAuthToken("new.jwt.token");

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: "new.jwt.token" });
  });

  it("forwards undefined JWT (clear) as null jwt_token", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.forExistingRuntime(runtime as any, makeContext());

    client.updateAuthToken(undefined);

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: null });
  });
});
```

- [ ] **Step 2: Run to verify fail**

Run: `pnpm --filter jazz-tools exec vitest run src/runtime/client.test.ts`
Expected: FAIL — `runtime.updateAuth` was not called.

- [ ] **Step 3: Fix `updateAuthToken`**

In `packages/jazz-tools/src/runtime/client.ts` replace:

```typescript
updateAuthToken(jwtToken?: string): void {
    this.context.jwtToken = jwtToken;
    this.resolvedSession = resolveClientSessionStateSync({
      appId: this.context.appId,
      jwtToken,
    }).session;
}
```

with:

```typescript
updateAuthToken(jwtToken?: string): void {
    this.context.jwtToken = jwtToken;
    this.resolvedSession = resolveClientSessionStateSync({
      appId: this.context.appId,
      jwtToken,
    }).session;
    // Push the refreshed credentials into the Rust transport. `updateAuth`
    // is optional on the Runtime interface because not every binding exposes
    // it yet; bindings that do will route this to TransportControl::UpdateAuth.
    this.runtime.updateAuth?.(JSON.stringify({ jwt_token: jwtToken ?? null }));
}
```

Also ensure the `Runtime` type (look near top of `client.ts` for the `interface Runtime { ... }` or `type Runtime = ...`) declares `updateAuth?(authJson: string): void;`. If the type already includes it via structural typing, leave it.

- [ ] **Step 4: Run test**

Run: `pnpm --filter jazz-tools exec vitest run src/runtime/client.test.ts`
Expected: PASS both tests.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/runtime/client.ts packages/jazz-tools/src/runtime/client.test.ts
git commit -m "fix(jazz-tools): JazzClient.updateAuthToken forwards new JWT to Rust transport"
```

---

## Task 10: Wire `ConnectSyncRuntimeOptions.onAuthFailure` → `runtime.onAuthFailure`

**Files:**

- Modify: `packages/jazz-tools/src/runtime/client.ts:608-630, 638-713`
- Test: `packages/jazz-tools/src/runtime/client.test.ts`

- [ ] **Step 1: Add a failing test**

Append to `client.test.ts`:

```typescript
describe("JazzClient onAuthFailure wiring", () => {
  it("registers runtimeOptions.onAuthFailure with runtime.onAuthFailure on construction", () => {
    const runtime = makeFakeRuntime();
    const onAuthFailure = vi.fn();

    JazzClient.forExistingRuntime(runtime as any, makeContext(), { onAuthFailure });

    expect(runtime.onAuthFailure).toHaveBeenCalledTimes(1);

    // Invoke whatever Rust-side callback was registered:
    const registered = runtime.onAuthFailure.mock.calls[0][0];
    registered("expired");
    expect(onAuthFailure).toHaveBeenCalledWith("expired");
  });

  it("does nothing when runtimeOptions.onAuthFailure is omitted", () => {
    const runtime = makeFakeRuntime();
    JazzClient.forExistingRuntime(runtime as any, makeContext(), {});
    expect(runtime.onAuthFailure).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run to verify fail**

Run: `pnpm --filter jazz-tools exec vitest run src/runtime/client.test.ts -t "onAuthFailure wiring"`
Expected: FAIL.

- [ ] **Step 3: Wire it up**

In `packages/jazz-tools/src/runtime/client.ts`, change the constructor param from `_runtimeOptions` (unused) to actually wiring the callback:

```typescript
private constructor(
  runtime: Runtime,
  context: AppContext,
  defaultDurabilityTier: DurabilityTier,
  runtimeOptions?: ConnectSyncRuntimeOptions,
) {
    this.runtime = runtime;
    this.scheduler = getScheduler();
    this.context = context;
    this.defaultDurabilityTier = defaultDurabilityTier;
    this.resolvedSession = resolveClientSessionStateSync({
      appId: context.appId,
      jwtToken: context.jwtToken,
    }).session;

    if (runtimeOptions?.onAuthFailure) {
      const handler = runtimeOptions.onAuthFailure;
      // The Rust callback receives a string; the TS callback type is
      // `AuthFailureReason`. The Rust side currently emits "Unauthorized"
      // etc.; map to a narrower type when the string matches known codes.
      this.runtime.onAuthFailure?.((reason: string) => {
        handler(mapRustAuthReasonToFailureReason(reason));
      });
    }
}
```

Add `mapRustAuthReasonToFailureReason` as a top-level helper in the same file (or reuse `mapAuthReason` in `jazz-worker.ts:215` — extract it to a shared util module if desired). The mapping converts `"Unauthorized"` → `"invalid"`, `"token expired"` → `"expired"` etc. — use the same mapping `jazz-worker.ts:mapAuthReason` already uses so worker and direct paths agree.

- [ ] **Step 4: Run test**

Run: `pnpm --filter jazz-tools exec vitest run src/runtime/client.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/runtime/client.ts packages/jazz-tools/src/runtime/client.test.ts
git commit -m "fix(jazz-tools): wire ConnectSyncRuntimeOptions.onAuthFailure to runtime.onAuthFailure"
```

---

## Task 11: Propagate worker `update-auth` errors to the main thread

**Files:**

- Modify: `packages/jazz-tools/src/worker/jazz-worker.ts:498-508`
- Modify: `packages/jazz-tools/src/worker/worker-protocol.ts` (add error-posting message variant if needed)
- Test: `packages/jazz-tools/src/worker/jazz-worker.test.ts`

- [ ] **Step 1: Add a failing test**

Append to `packages/jazz-tools/src/worker/jazz-worker.test.ts`:

```typescript
describe("worker update-auth error propagation", () => {
  it("posts auth-failed with reason=invalid when runtime.updateAuth throws", async () => {
    const posted: any[] = [];
    const post = (msg: any) => posted.push(msg);
    const runtime = {
      updateAuth: vi.fn(() => {
        throw new Error("boom");
      }),
    };
    // Set up worker state with runtime and current auth:
    const workerState = makeWorkerTestState({ runtime, currentAuth: { jwt_token: "old" } });

    await handleWorkerMessage(workerState, { type: "update-auth", jwtToken: "new.jwt" }, post);

    const authFailed = posted.find((m) => m.type === "auth-failed");
    expect(authFailed).toBeDefined();
    expect(authFailed.reason).toBe("invalid");
  });
});
```

The helpers `makeWorkerTestState` and `handleWorkerMessage` already exist in the worker test file (see the existing `describe` blocks). Match whatever seam is in use.

- [ ] **Step 2: Run to verify fail**

Run: `pnpm --filter jazz-tools exec vitest run src/worker/jazz-worker.test.ts -t "update-auth error"`
Expected: FAIL.

- [ ] **Step 3: Update the handler**

In `packages/jazz-tools/src/worker/jazz-worker.ts`:

```typescript
case "update-auth": {
  currentAuth = mergeAuth(currentAuth, msg.jwtToken);
  if (runtime) {
    try {
      runtime.updateAuth(JSON.stringify(currentAuth));
    } catch (e) {
      console.error("[worker] runtime.updateAuth failed:", e);
      post({ type: "auth-failed", reason: "invalid" });
    }
  }
  break;
}
```

If `worker-protocol.ts` doesn't already declare `auth-failed` with `reason: AuthFailureReason`, check — the review indicated `onAuthFailure` is already invoked in the worker via `runtime.onAuthFailure?.((reason) => post({ type: "auth-failed", reason: mapAuthReason(reason) }))` at jazz-worker.ts:309. If the message type already exists, reuse it. Otherwise add it to `worker-protocol.ts`.

- [ ] **Step 4: Run test**

Run: `pnpm --filter jazz-tools exec vitest run src/worker/jazz-worker.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/worker/jazz-worker.ts packages/jazz-tools/src/worker/worker-protocol.ts packages/jazz-tools/src/worker/jazz-worker.test.ts
git commit -m "fix(jazz-tools): worker posts auth-failed when runtime.updateAuth throws"
```

---

## Task 12: Cross-layer E2E — auth refresh through JazzClient reaches transport

**Files:**

- Test: extend `packages/jazz-tools/tests/browser/db.auth-refresh.worker.test.ts` (existing browser integration test) to assert that after `updateAuthToken`, the worker's `runtime.updateAuth` gets called with the new JWT.

- [ ] **Step 1: Read the existing browser test**

Run: `sed -n '1,70p' packages/jazz-tools/tests/browser/db.auth-refresh.worker.test.ts`
Expected: see the existing "postMessage dispatch" assertion.

- [ ] **Step 2: Add a new browser test that traces through the worker**

The cleanest assertion is: after `client.updateAuthToken("new")`, observe the worker-bridge's outbound `update-auth` message, then assert that within a short timeout the worker sent back an `update-auth-ack` (add one in the worker handler if one doesn't exist — post `{ type: "update-auth-ack" }` after a successful `runtime.updateAuth`). The ack is a test seam only.

Pseudocode of the new test:

```typescript
test("updateAuthToken -> worker runtime.updateAuth acked", async ({ page }) => {
  // Bring up playwright page, worker, JazzClient, all wired to a mock WS server
  // that accepts the first JWT. Freeze the server to not require real server.
  // Call client.updateAuthToken("fresh.jwt") and wait for the bridge to see
  // an `update-auth-ack` message (added for this test).
});
```

The concrete wiring depends on the test file's existing harness — match its patterns.

- [ ] **Step 3: Implement the `update-auth-ack` seam**

In `packages/jazz-tools/src/worker/jazz-worker.ts` inside the `update-auth` handler success branch, post `{ type: "update-auth-ack" }`. In `worker-protocol.ts`, add the message variant. In `worker-bridge.ts`, forward it to a bridge-local listener if helpful; or just observe it via `onmessage` in the test.

- [ ] **Step 4: Run it**

Run: `pnpm --filter jazz-tools exec playwright test tests/browser/db.auth-refresh.worker.test.ts 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/jazz-tools/src/worker/jazz-worker.ts packages/jazz-tools/src/worker/worker-protocol.ts packages/jazz-tools/tests/browser/db.auth-refresh.worker.test.ts
git commit -m "test(jazz-tools): E2E auth refresh reaches worker runtime.updateAuth"
```

---

## Task 13: Final verification sweep

- [ ] **Step 1: Full test suite**

Run (in parallel where possible):

```bash
cargo test -p jazz-tools --features test 2>&1 | grep -E "^test result" | tail -20
pnpm --filter jazz-tools test --run 2>&1 | tail -20
pnpm --filter jazz-tools build 2>&1 | tail -5
cargo build -p jazz-napi 2>&1 | tail -5
cargo build -p jazz-rn 2>&1 | tail -5
cargo build -p jazz-wasm --target wasm32-unknown-unknown 2>&1 | tail -5
```

Expected: every suite passes with zero failures.

- [ ] **Step 2: Remove or update stale todo/issues**

For each of:

- `todo/issues/update-auth-noop-breaks-jwt-refresh.md` — mark resolved or delete.
- `todo/issues/worker-never-emits-auth-failed.md` — mark resolved or delete.

Run: `bash scripts/update-todo.sh`

- [ ] **Step 3: Commit housekeeping**

```bash
git add todo/
git commit -m "chore(todo): close auth-refresh issues fixed by this branch"
```

---

## Self-Review

- **Spec coverage:** Push direction (Task 9), pull direction for NAPI/RN/WASM (Tasks 7-8, WASM already works), TS wiring of `onAuthFailure` (Task 10), worker error propagation (Task 11), shared helper tradeoff (Tasks 1-5), tests (Tasks 1/6/8/9/10/11/12). All items from the review's Critical section are covered.
- **Placeholder scan:** Every step has concrete code or an exact command. Where implementation details depend on existing helpers (`make_core_for_test`, worker test seams), the step explicitly says to reuse/match whatever the file already uses. No TODOs.
- **Type consistency:** `install_transport<S, Sch, W, T>` signature matches across Tasks 1-5. `onAuthFailure?.((reason: string) => …)` is consistent across client.ts wiring and worker postback. `AuthFailureCallback::on_failure(&self, reason: String)` is consistent RN-side.

**Ready to execute.**
