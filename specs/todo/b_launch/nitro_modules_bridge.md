# Nitro Modules Bridge for Jazz2: Exploration & Spec Design

## Context

Jazz2 has a Rust core (`groove`) with two existing binding crates:
- **groove-wasm** — browser via wasm-bindgen (`Rc<RefCell<RuntimeCore<OpfsBTreeStorage, WasmScheduler, JsSyncSender>>>`)
- **groove-napi** — Node.js server via napi-rs (`Arc<Mutex<RuntimeCore<SurrealKvStorage, NapiScheduler, NapiSyncSender>>>`)

React Native is the missing target. The existing specs (`react_native_packaging.md`, `react_native_storage_investigation.md`) have open questions: "NAPI via Hermes vs JSI vs Turbo Modules?" — Nitro Modules would be the answer.

Jazz1 uses `uniffi-bindgen-react-native` for its RN bridge: Rust → UniFFI macros → C FFI → C++ JSI HostObject → TurboModule → TypeScript. This works but has drawbacks (see "Why Nitro over UniFFI" below).

Brad is adding Rust support to Nitro in a fork (`boorad/nitro feat/rust`), which would enable a direct Rust HybridObject implementation without the C++ intermediary layer.

---

## Where Nitro Fits in Jazz2

### What it replaces
Nitro would **not** replace groove-wasm (browser) or groove-napi (Node.js server). It creates a **third binding crate** — `groove-nitro` — specifically for React Native on iOS and Android.

### Architecture diagram

```
                    ┌─────────────────────────────────────────┐
                    │           groove (Rust core)             │
                    │  RuntimeCore<S, Sch, Sy>                 │
                    │  SchemaManager, QueryManager, SyncManager│
                    └────┬──────────┬──────────┬──────────────┘
                         │          │          │
              ┌──────────┘    ┌─────┘    ┌─────┘
              ▼               ▼          ▼
    ┌──────────────┐  ┌────────────┐  ┌──────────────┐
    │ groove-wasm  │  │groove-napi │  │ groove-nitro │  ← NEW
    │ wasm-bindgen │  │ napi-rs    │  │ Nitro Rust   │
    │ browser/OPFS │  │ Node/Surreal│  │ RN/SurrealKV │
    └──────┬───────┘  └─────┬──────┘  └──────┬───────┘
           │                │                 │
           ▼                ▼                 ▼
      Browser JS       Node.js TS       React Native TS
      (Web Workers)    (Server)         (iOS/Android)
```

### What groove-nitro wraps
```
RuntimeCore<SurrealKvStorage, NitroScheduler, NitroSyncSender>
```
- **SurrealKvStorage** — same persistent storage as groove-napi (Documents dir on iOS, internal storage on Android)
- **NitroScheduler** — schedules `batched_tick()` on the RN JS thread via Nitro callback
- **NitroSyncSender** — sends sync messages back to JS via Nitro callback

---

## Nitro HybridObject Spec

This is the `.nitro.ts` spec that Nitrogen would consume. It mirrors the API surface already exposed by groove-wasm and groove-napi, but uses Nitro's native type system instead of JSON strings.

```typescript
// groove-nitro.nitro.ts
import { HybridObject } from 'react-native-nitro-modules'

// --- Struct types (Nitrogen generates C++ structs + Rust #[repr(C)] structs) ---

interface RowData {
  id: string
  index: number
  columns: ArrayBuffer  // binary-encoded row data (zero-copy)
}

interface UpdatedRowData {
  id: string
  index: number
  oldIndex: number
  columns: ArrayBuffer
}

interface SubscriptionDelta {
  added: RowData[]
  removed: string[]         // removed object IDs
  updated: UpdatedRowData[]
}

/**
 * Core Jazz runtime for React Native.
 * Wraps Rust RuntimeCore with SurrealKV storage.
 */
export interface GrooveRuntime
  extends HybridObject<{ ios: 'rust', android: 'rust' }> {

  // --- CRUD (small payloads — JSON strings are fine) ---
  insert(table: string, valuesJson: string): string              // returns object ID
  update(objectId: string, valuesJson: string): void
  delete(objectId: string): void

  // --- Queries (hot path — binary results via ArrayBuffer) ---
  query(queryJson: string, sessionJson?: string): Promise<ArrayBuffer>

  // --- Subscriptions (hot path — typed delta structs with binary row data) ---
  subscribe(queryJson: string, callback: (delta: SubscriptionDelta) => void): number
  unsubscribe(handle: number): void

  // --- Sync (already binary — zero-copy ArrayBuffer) ---
  onSyncMessageReceived(payload: ArrayBuffer): void
  onSyncMessageToSend(callback: (payload: ArrayBuffer) => void): void

  // --- Server/client management ---
  addServer(serverId: string): void
  removeServer(serverId: string): void
  addClient(clientId: string, sessionJson?: string): void
  removeClient(clientId: string): void
  setClientRole(clientId: string, role: string): void

  // --- Storage ---
  flushStorage(): void
  flushWal(): void

  // --- Schema ---
  readonly currentSchemaJson: string
}

/**
 * Standalone utility functions (not on the runtime object).
 */
export interface GrooveUtils
  extends HybridObject<{ ios: 'rust', android: 'rust' }> {
  generateId(): string
  currentTimestamp(): number
  parseSchema(json: string): string
}
```

### Key design decisions

1. **Binary for hot paths, JSON for cold paths.** Query results and subscription deltas use `ArrayBuffer` with binary-encoded row data (zero-copy across both the Rust→C++ and C++→JS boundaries). CRUD params use JSON strings — they're small and infrequent. Sync messages are already binary-encoded, so they pass as `ArrayBuffer` directly.

2. **Typed subscription deltas.** `SubscriptionDelta` is a Nitrogen-generated struct. The `RowData.columns` field is an `ArrayBuffer` — Rust encodes the row columns into bytes, passes them zero-copy, and the TS layer decodes. This avoids per-value JSI conversion overhead for large result sets.

3. **Callbacks for subscriptions and sync.** Nitro HybridObjects support callbacks. `subscribe()` takes a typed `(delta: SubscriptionDelta) => void` callback. `onSyncMessageToSend()` takes a callback that Rust invokes when it has outbound sync messages.

4. **Promise for query().** Queries in RuntimeCore return a `QueryFuture` (oneshot channel). Nitro supports Promise return types, so `query()` returns `Promise<ArrayBuffer>`. **Note:** This requires the fork's Promise stub to become a real implementation.

5. **Constructor via factory.** Nitro HybridObjects are constructed natively. The constructor args would be: `schemaJson: string, appId: string, env: string, userBranch: string, dataPath: string, tier?: string`. The user provides a `#[no_mangle] pub extern "C" fn create_HybridGrooveRuntimeSpec() -> *mut c_void` factory function.

---

## Rust Implementation Sketch (groove-nitro crate)

```
crates/groove-nitro/
├── Cargo.toml
├── src/
│   ├── lib.rs          # HybridObject impl, Nitro registration
│   ├── scheduler.rs    # NitroScheduler (Scheduler trait)
│   ├── sync_sender.rs  # NitroSyncSender (SyncSender trait)
│   └── types.rs        # Value conversion (reuse from groove-napi)
```

The structure mirrors groove-napi almost exactly. Key differences:
- groove-napi uses `napi::ThreadsafeFunction` for callbacks → groove-nitro uses Nitro's callback mechanism
- groove-napi uses `#[napi]` macros → groove-nitro uses whatever the `feat/rust` Nitro integration provides (likely `#[nitro]` or similar proc macros from Brad's fork)
- Both use `Arc<Mutex<RuntimeCore<SurrealKvStorage, ...>>>` since they're multi-threaded native

### NitroScheduler
```rust
// Analogous to NapiScheduler
struct NitroScheduler {
    callback: /* Nitro's threadsafe callback type */,
    scheduled: Arc<AtomicBool>,
}

impl Scheduler for NitroScheduler {
    fn schedule_batched_tick(&self) {
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            // Invoke callback on JS thread → JS calls back into batched_tick()
            self.callback.call(());
        }
    }
}
```

### NitroSyncSender
```rust
// Analogous to NapiSyncSender
struct NitroSyncSender {
    callback: /* Nitro's threadsafe callback type */,
}

impl SyncSender for NitroSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        let json = serde_json::to_string(&message).unwrap();
        self.callback.call(json);
    }
}
```

---

## Value/Type Bridging

Jazz's Value enum doesn't map cleanly to a single Nitro variant (discriminated unions with string tags are problematic in C++). Instead, we use a **binary encoding strategy**:

### Hot paths: Binary-encoded via ArrayBuffer (zero-copy)

Row data crosses as `ArrayBuffer` using the existing `groove::query_manager::encoding` module. Rust encodes rows into a compact binary format, passes the buffer zero-copy to JS, and a thin TS decoder on the other side interprets the bytes. This is the same pattern groove-wasm uses for subscription deltas (see `build_wasm_delta_json` in `groove-wasm/src/runtime.rs`), but with binary instead of JSON.

### Cold paths: JSON strings

CRUD inputs (`insert` values, `update` values) and schema/query definitions use JSON strings. These are small, infrequent, and already JSON-serialized in the existing bindings.

### Why this is better than per-value Nitro types

- A 100-row query result with 10 columns = 1000 Value conversions across FFI. With binary encoding it's 1 ArrayBuffer (zero-copy).
- Subscription deltas fire frequently. Typed struct + binary columns avoids per-cell overhead.
- The encoding/decoding logic already exists in Rust (`decode_row`) and would need a matching TS decoder.

---

## Why Nitro over UniFFI

| Concern | UniFFI (Jazz1) | Nitro (proposed) |
|---|---|---|
| **Performance** | TurboModule / `jsi::HostObject` | `jsi::NativeState` — 16x faster method calls |
| **Codegen direction** | Rust-first (Rust macros → generate TS) | TS-first (TS spec → generate native interfaces) |
| **Type safety** | Runtime type checking at FFI boundary | Compile-time — Nitrogen fails build if spec ≠ impl |
| **Generated code** | ~3000 LOC TS + ~500 LOC C++ per crate | Minimal generated glue; HybridObject is the API |
| **Maintenance** | uniffi-bindgen-react-native has small community | Margelo actively maintains; growing RN ecosystem adoption |
| **Rust support** | Native via `#[uniffi::export]` | Brad's fork (`feat/rust`) adds native Rust HybridObjects |
| **Callback model** | Limited (no callbacks with return values) | First-class callbacks, Promises, async |
| **Ecosystem** | Mozilla-backed, cross-platform (Swift/Kotlin/Python) | RN-specific, optimized for that use case |

The biggest wins: **performance** (NativeState vs HostObject) and **TS-first spec** (the spec IS the TypeScript interface, not a separate UDL or Rust annotation).

---

## Relationship to Existing Specs

This Nitro approach answers the open questions in:

- **`react_native_packaging.md`**: "NAPI via Hermes vs JSI vs Turbo Modules?" → **Nitro** (JSI-based, faster than all three options listed)
- **`react_native_storage_investigation.md`**: The spike work (SurrealKV on iOS/Android) is a prerequisite. groove-nitro would use the same SurrealKvStorage that the spike validates.
- **`swift_bindings.md`** / **`kotlin_bindings.md`**: Nitro is specifically for React Native. Pure Swift/Kotlin native apps would still use their own FFI approach (UniFFI, C bridge, JNI). But if the primary mobile target is RN, Nitro covers it.

---

## What Needs to Happen (Sequencing)

1. **Complete Nitro Rust support** (`boorad/nitro feat/rust`) — the fork needs to support Rust HybridObjects so groove-nitro can be implemented purely in Rust without a C++ intermediary
2. **Complete RN storage spike** (`react_native_storage_investigation.md`) — validate SurrealKV compiles and runs on iOS/Android
3. **Create `groove-nitro` crate** — implement HybridObject wrapping RuntimeCore, following groove-napi's patterns
4. **Create Nitro spec file** (`groove-nitro.nitro.ts`) — the TypeScript interface above
5. **Wire into `jazz-tools`** — add a React Native runtime target alongside WASM and NAPI in the `Runtime` interface / `JazzClient`
6. **React Native example app** — validate end-to-end: insert → query → subscribe → sync

---

---

## Q1: JSON Strings vs Typed Data at the Boundary

You're right to push back on JSON. The whole point of Nitro is that the C++ JSIConverter layer converts native types directly to/from `jsi::Value` — no serialization step. The current fork already supports this well. Here's how it would work for Jazz:

### What Nitro gives you natively (no JSON needed)

The C++ JSIConverter layer handles these conversions directly to/from JSI values:

| Nitro TS type | C++ type | Rust type (your fork) | Zero-copy? |
|---|---|---|---|
| `number` | `double` | `f64` | yes (value) |
| `bigint` | `int64_t` | `i64` | yes (value) |
| `boolean` | `bool` | `bool` | yes (value) |
| `string` | `std::string` | `String` (via CStr) | no (copy) |
| `ArrayBuffer` | `shared_ptr<ArrayBuffer>` | `Vec<u8>` | **yes** (zero-copy from JS→native via NativeState, native→JS wraps pointer) |
| `T[]` | `std::vector<T>` | `Vec<T>` | no (per-element conversion) |
| `interface Foo` | struct | struct (Nitrogen-generated `#[repr(C)]`) | no (field-by-field conversion) |
| `enum` | `int32_t` | `i32` discriminant | yes (value) |
| `A \| B` (variant) | `std::variant<A,B>` | Rust enum | no (tag + conversion) |
| `(callback) => void` | `std::function` | `Box<dyn Fn>` (via fn ptr + userdata) | n/a |
| `Promise<T>` | `shared_ptr<Promise<T>>` | `Promise<T>` (stub in fork) | n/a |

### Recommended approach for Jazz's Value type

**Don't use JSON.** Instead, define Jazz's Value as a Nitro variant (union type) in the spec:

```typescript
// In the .nitro.ts spec
interface JazzValue {
  type: 'integer' | 'bigint' | 'boolean' | 'text' | 'timestamp' | 'uuid' | 'null'
  // Can't cleanly do tagged union as Nitro variant...
}
```

Actually, Nitro's variant support has a limitation: discriminated unions with string literal tags don't map cleanly to C++. But we can work around this two ways:

**Option A: Flattened API (recommended for hot paths)**

Instead of passing `Value[]` as a generic array, make the Nitro spec method-specific with typed params:

```typescript
// Subscription delta as a Nitro struct
interface RowDelta {
  added: RowData[]
  removed: string[]   // object IDs
  updated: UpdatedRowData[]
}

interface RowData {
  id: string
  index: number
  columns: ArrayBuffer  // binary-encoded row, decoded on TS side
}
```

For query results and subscription deltas (the hot path), use `ArrayBuffer` to pass binary-encoded row data. The Rust side already has `groove::query_manager::encoding::decode_row` — encode on Rust side, pass the bytes zero-copy, decode on TS side. This is faster than JSON *and* faster than per-value JSI conversion for large result sets.

**Option B: Hybrid approach**

- **CRUD params** (`insert`, `update`): These are small and infrequent. JSON strings are fine here — simplicity wins.
- **Query results / subscription deltas** (the actual hot path): Use `ArrayBuffer` for zero-copy binary transfer.
- **Sync messages**: Already binary-encoded in the sync protocol. Pass as `ArrayBuffer` directly — no serialization at all.

This matches what groove-wasm/groove-napi already do for sync messages (binary frames), but extends it to query results too.

### The key insight

The Nitro boundary has *two* hops for Rust: Rust → (extern "C") → C++ → (JSIConverter) → JS. The first hop (Rust↔C++) is where your fork operates. The second hop (C++↔JS) is Nitro's existing JSIConverter layer. For `ArrayBuffer`, *both* hops are zero-copy: Rust passes a pointer to C++, C++ wraps it in `jsi::ArrayBuffer` with `NativeState`, JS reads the memory directly. That's as good as it gets.

---

## Q2: What Your Fork Needs

After reading the generated code thoroughly, here's my assessment:

### What's solid
- **Type codegen** — comprehensive. All Nitro types map to Rust correctly.
- **FFI shim generation** — `#[no_mangle] pub unsafe extern "C" fn` pattern is correct.
- **C++ bridge header** — `HybridTSpecRust.hpp` with inline method overrides calling extern "C" functions works.
- **Callback wrapping** — `Func_*` structs with fn_ptr + userdata is the right pattern.
- **Struct/Enum/Variant** generation — all look correct.
- **Test coverage** in Nitrogen — rust-bridged-type.test.ts, rust-hybrid-object.test.ts, etc.

### What's missing or needs work

1. **Promise is a phantom stub**
   ```rust
   pub mod Promise { pub struct Promise<T>(pub std::marker::PhantomData<T>); }
   ```
   This doesn't work. The C++ `shared_ptr<Promise<T>>` crosses as `void*` into Rust, where it becomes `Promise<T>` — but there's no way for Rust to resolve or await it. Need:
   - A real `RustPromise<T>` type that wraps the C++ Promise pointer
   - An FFI function to resolve/reject from Rust
   - An FFI function to `.then()` / await from Rust
   - For Jazz: `query()` returns `Promise<string>` or `Promise<ArrayBuffer>`, so this is needed.

2. **Callback invocation across threads**
   The generated `Func_*` structs wrap a C fn pointer + userdata, and mark them `Send + Sync`. But Nitro callbacks (JS functions) can **only be called on the JS thread**. The C++ side handles thread-switching via `CallInvoker`, but the Rust side doesn't know about this. For Jazz:
   - `NitroScheduler` needs to call a JS callback from any thread → needs `CallInvoker` integration
   - `NitroSyncSender` needs to invoke a callback when sync messages are ready
   - The fork should either: (a) document that Rust must dispatch to JS thread before calling callbacks, or (b) wrap callbacks in a thread-safe invoker on the C++ side before passing to Rust.

3. **Build system integration (the biggest gap)**
   - No `cargo` invocation in the iOS or Android build pipeline
   - The generated `Cargo.toml` creates a `staticlib`, but nothing links it
   - iOS needs: `cargo build --target aarch64-apple-ios` → `.a` → linked in Xcode/CocoaPods
   - Android needs: `cargo ndk --target aarch64-linux-android` → `.so` or `.a` → linked in CMakeLists.txt
   - This is non-trivial. Jazz1's uniffi-bindgen-react-native handles this; your fork doesn't yet.

4. **Factory function pattern is undocumented**
   The registration code expects:
   ```rust
   #[no_mangle]
   pub extern "C" fn create_HybridGrooveRuntimeSpec() -> *mut c_void
   ```
   This is the user's responsibility, but there's no documentation, example, or even a comment in the generated trait file pointing to this requirement.

5. **Memory management for complex types**
   Every non-primitive crosses as `void*` via `Box::into_raw` / `Box::from_raw`. This means:
   - Each complex parameter allocates on both sides (C++ `new` + Rust `Box`)
   - Ownership transfer is implicit and easy to get wrong
   - For ArrayBuffer specifically: the fork converts to `Vec<u8>` which **copies** the data. It should pass the raw pointer + length instead, to enable zero-copy. The C++ `ArrayBuffer::wrap()` API supports this.

6. **No `memorySize` reporting**
   Nitro HybridObjects report `getExternalMemorySize()` to help the JS GC understand how much native memory is held. The generated Rust trait doesn't include this. For Jazz, the RuntimeCore holds significant native memory (storage, caches, subscriptions), so GC pressure hints matter on mobile.

7. **Error propagation is string-only**
   Rust errors become `*const c_char` → C++ `std::runtime_error`. Works, but loses error types. For Jazz this is probably fine.

### Priority order for Jazz specifically

For groove-nitro to work, the fork needs (in order):
1. **Build system** — cargo invocation for iOS/Android targets, linking into CMake/Xcode
2. **Promise** — real implementation, not a phantom. Jazz needs `query()` → `Promise<ArrayBuffer>`
3. **Thread-safe callbacks** — CallInvoker integration for scheduler/sync sender
4. **ArrayBuffer zero-copy** — pass pointer+len not `Vec<u8>` copy
5. **Factory function docs/example** — so the groove-nitro implementer knows what to write
6. **memorySize** — nice to have, not blocking

---

## Files to Create/Modify

| Action | Path | Purpose |
|---|---|---|
| Create | `crates/groove-nitro/` | New Nitro binding crate |
| Create | `crates/groove-nitro/src/lib.rs` | HybridObject impl |
| Create | `crates/groove-nitro/src/scheduler.rs` | NitroScheduler |
| Create | `crates/groove-nitro/src/sync_sender.rs` | NitroSyncSender |
| Create | `crates/groove-nitro/src/types.rs` | Value conversion (likely copy from groove-napi) |
| Create | `crates/groove-nitro/Cargo.toml` | Crate config |
| Create | `crates/groove-nitro/groove-nitro.nitro.ts` | Nitro TypeScript spec |
| Modify | `packages/jazz-tools/src/runtime/client.ts` | Add NitroRuntime as a third Runtime backend |
| Modify | `packages/jazz-tools/src/drivers/types.ts` | Potentially extend Runtime interface |
| Update | `specs/todo/b_launch/react_native_packaging.md` | Record Nitro as the chosen approach |
