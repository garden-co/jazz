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
import { HybridObject } from "react-native-nitro-modules";

// --- Struct types (Nitrogen generates C++ structs + Rust #[repr(C)] structs) ---

interface RowData {
  id: string;
  index: number;
  columns: ArrayBuffer; // binary-encoded row data (zero-copy)
}

interface UpdatedRowData {
  id: string;
  index: number;
  oldIndex: number;
  columns: ArrayBuffer;
}

interface SubscriptionDelta {
  added: RowData[];
  removed: string[]; // removed object IDs
  updated: UpdatedRowData[];
}

/**
 * Core Jazz runtime for React Native.
 * Wraps Rust RuntimeCore with SurrealKV storage.
 */
export interface GrooveRuntime extends HybridObject<{ ios: "rust"; android: "rust" }> {
  // --- CRUD (small payloads — JSON strings are fine) ---
  insert(table: string, valuesJson: string): string; // returns object ID
  update(objectId: string, valuesJson: string): void;
  delete(objectId: string): void;

  // --- Queries (hot path — binary results via ArrayBuffer) ---
  query(queryJson: string, sessionJson?: string): Promise<ArrayBuffer>;

  // --- Subscriptions (hot path — typed delta structs with binary row data) ---
  subscribe(queryJson: string, callback: (delta: SubscriptionDelta) => void): number;
  unsubscribe(handle: number): void;

  // --- Sync (already binary — zero-copy ArrayBuffer) ---
  onSyncMessageReceived(payload: ArrayBuffer): void;
  onSyncMessageToSend(callback: (payload: ArrayBuffer) => void): void;

  // --- Server/client management ---
  addServer(serverId: string): void;
  removeServer(serverId: string): void;
  addClient(clientId: string, sessionJson?: string): void;
  removeClient(clientId: string): void;
  setClientRole(clientId: string, role: string): void;

  // --- Storage ---
  flushStorage(): void;
  flushWal(): void;

  // --- Schema ---
  readonly currentSchemaJson: string;
}

/**
 * Standalone utility functions (not on the runtime object).
 */
export interface GrooveUtils extends HybridObject<{ ios: "rust"; android: "rust" }> {
  generateId(): string;
  currentTimestamp(): number;
  parseSchema(json: string): string;
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

| Concern               | UniFFI (Jazz1)                                       | Nitro (proposed)                                          |
| --------------------- | ---------------------------------------------------- | --------------------------------------------------------- |
| **Performance**       | TurboModule / `jsi::HostObject`                      | `jsi::NativeState` — 16x faster method calls              |
| **Codegen direction** | Rust-first (Rust macros → generate TS)               | TS-first (TS spec → generate native interfaces)           |
| **Type safety**       | Runtime type checking at FFI boundary                | Compile-time — Nitrogen fails build if spec ≠ impl        |
| **Generated code**    | ~3000 LOC TS + ~500 LOC C++ per crate                | Minimal generated glue; HybridObject is the API           |
| **Maintenance**       | uniffi-bindgen-react-native has small community      | Margelo actively maintains; growing RN ecosystem adoption |
| **Rust support**      | Native via `#[uniffi::export]`                       | Brad's fork (`feat/rust`) adds native Rust HybridObjects  |
| **Callback model**    | Limited (no callbacks with return values)            | First-class callbacks, Promises, async                    |
| **Ecosystem**         | Mozilla-backed, cross-platform (Swift/Kotlin/Python) | RN-specific, optimized for that use case                  |

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

| Nitro TS type        | C++ type                  | Rust type (your fork)                    | Zero-copy?                                                                  |
| -------------------- | ------------------------- | ---------------------------------------- | --------------------------------------------------------------------------- |
| `number`             | `double`                  | `f64`                                    | yes (value)                                                                 |
| `bigint`             | `int64_t`                 | `i64`                                    | yes (value)                                                                 |
| `boolean`            | `bool`                    | `bool`                                   | yes (value)                                                                 |
| `string`             | `std::string`             | `String` (via CStr)                      | no (copy)                                                                   |
| `ArrayBuffer`        | `shared_ptr<ArrayBuffer>` | `Vec<u8>`                                | **yes** (zero-copy from JS→native via NativeState, native→JS wraps pointer) |
| `T[]`                | `std::vector<T>`          | `Vec<T>`                                 | no (per-element conversion)                                                 |
| `interface Foo`      | struct                    | struct (Nitrogen-generated `#[repr(C)]`) | no (field-by-field conversion)                                              |
| `enum`               | `int32_t`                 | `i32` discriminant                       | yes (value)                                                                 |
| `A \| B` (variant)   | `std::variant<A,B>`       | Rust enum                                | no (tag + conversion)                                                       |
| `(callback) => void` | `std::function`           | `Box<dyn Fn>` (via fn ptr + userdata)    | n/a                                                                         |
| `Promise<T>`         | `shared_ptr<Promise<T>>`  | `Promise<T>` (stub in fork)              | n/a                                                                         |

### Recommended approach for Jazz's Value type

**Don't use JSON.** Instead, define Jazz's Value as a Nitro variant (union type) in the spec:

```typescript
// In the .nitro.ts spec
interface JazzValue {
  type: "integer" | "bigint" | "boolean" | "text" | "timestamp" | "uuid" | "null";
  // Can't cleanly do tagged union as Nitro variant...
}
```

Actually, Nitro's variant support has a limitation: discriminated unions with string literal tags don't map cleanly to C++. But we can work around this two ways:

**Option A: Flattened API (recommended for hot paths)**

Instead of passing `Value[]` as a generic array, make the Nitro spec method-specific with typed params:

```typescript
// Subscription delta as a Nitro struct
interface RowDelta {
  added: RowData[];
  removed: string[]; // object IDs
  updated: UpdatedRowData[];
}

interface RowData {
  id: string;
  index: number;
  columns: ArrayBuffer; // binary-encoded row, decoded on TS side
}
```

For query results and subscription deltas (the hot path), use `ArrayBuffer` to pass binary-encoded row data. The Rust side already has `groove::query_manager::encoding::decode_row` — encode on Rust side, pass the bytes zero-copy, decode on TS side. This is faster than JSON _and_ faster than per-value JSI conversion for large result sets.

**Option B: Hybrid approach**

- **CRUD params** (`insert`, `update`): These are small and infrequent. JSON strings are fine here — simplicity wins.
- **Query results / subscription deltas** (the actual hot path): Use `ArrayBuffer` for zero-copy binary transfer.
- **Sync messages**: Already binary-encoded in the sync protocol. Pass as `ArrayBuffer` directly — no serialization at all.

This matches what groove-wasm/groove-napi already do for sync messages (binary frames), but extends it to query results too.

### The key insight

The Nitro boundary has _two_ hops for Rust: Rust → (extern "C") → C++ → (JSIConverter) → JS. The first hop (Rust↔C++) is where your fork operates. The second hop (C++↔JS) is Nitro's existing JSIConverter layer. For `ArrayBuffer`, _both_ hops are zero-copy: Rust passes a pointer to C++, C++ wraps it in `jsi::ArrayBuffer` with `NativeState`, JS reads the memory directly. That's as good as it gets.

---

## Q2: What Your Fork Needs — Status After `6e222071`

All 6 originally-identified gaps have been addressed in commit `6e222071` on `feat/rust`. Here's the updated assessment:

### What was already solid (unchanged)

- **Type codegen** — comprehensive. All Nitro types map to Rust correctly.
- **FFI shim generation** — `#[no_mangle] pub unsafe extern "C" fn` pattern is correct.
- **C++ bridge header** — `HybridTSpecRust.hpp` with inline method overrides calling extern "C" functions works.
- **Struct/Enum/Variant** generation — all correct.
- **Test coverage** in Nitrogen — rust-bridged-type.test.ts, rust-hybrid-object.test.ts, etc.

### What was fixed in `6e222071`

1. **Build system** — DONE
   - **Android**: CMake extension (`createCMakeExtension.ts`) now maps `ANDROID_ABI` to Rust target triple, invokes `cargo build --release --target`, copies the `.a`, and links it via `add_library(IMPORTED)`.
   - **iOS**: Podspec extension (`createPodspecRubyExtension.ts`) adds a `script_phase` that detects `PLATFORM_NAME`/`ARCHS`, maps to Rust target (including `aarch64-apple-ios-sim` for simulator), runs `cargo build`, and vendors the resulting `.a`.

2. **Promise** — DONE (elegant approach)
   Rust trait methods that return `Promise<T>` in the TS spec just return `T` synchronously in the Rust trait. The C++ bridge wraps the call in `Promise<T>::async([=]() { return rust_fn(); })`. Example:
   - TS spec: `calculateFibonacciAsync(value: number): Promise<bigint>`
   - Rust trait: `fn calculate_fibonacci_async(&mut self, value: f64) -> i64`
   - C++ bridge: `Promise<int64_t>::async([=]() { return ..._calculate_fibonacci_async(_rustPtr, value); })`

   No Promise type needed on the Rust side at all. For Jazz: `query()` returns `Promise<ArrayBuffer>` → Rust just returns a `NitroBuffer`.

3. **Callback lifecycle** — DONE
   `Func_*` structs now have 3 fields: `fn_ptr`, `userdata`, `destroy_fn`. The C++ side creates a trampoline that casts userdata back to `std::function`. The `destroy_fn` is called on `Drop`, preventing leaks. C++ owns the `std::function` and handles thread dispatch via CallInvoker as usual — Rust doesn't need to know about thread safety.

4. **ArrayBuffer zero-copy** — DONE
   New `NitroBuffer` type (`#[repr(C)]` struct: `data: *mut u8, len: usize, handle: *mut c_void, release_fn`):
   - **C++ → Rust**: C++ extracts `data()` and `size()` from `shared_ptr<ArrayBuffer>`, boxes the shared_ptr as the handle. Rust gets a `NitroBuffer` and reads `as_slice()` — zero-copy.
   - **Rust → C++**: Rust boxes the `NitroBuffer`, C++ reads data/len and creates `ArrayBuffer::wrap()` with a destructor that calls `release_fn` — zero-copy.
   - `NitroBuffer::from_vec()` lets Rust create buffers from owned data.

5. **Factory function docs** — DONE
   Generated trait now includes a doc comment with the exact factory function signature:

   ```rust
   /// #[no_mangle]
   /// pub extern "C" fn create_HybridFooSpec() -> *mut std::ffi::c_void {
   ///     let obj: Box<dyn HybridFooSpec> = Box::new(MyFoo::new());
   ///     Box::into_raw(Box::new(obj)) as *mut std::ffi::c_void
   /// }
   ```

6. **memorySize** — DONE
   Trait includes `fn memory_size(&self) -> usize { 0 }` (default impl). C++ bridge calls it from `getExternalMemorySize()`. Jazz can override to report RuntimeCore's storage/cache size.

### Remaining concerns (minor)

- **Error propagation** is still string-only (`*const c_char` → `std::runtime_error`). Fine for Jazz.
- **Promise as a parameter** (not return type) still uses the phantom stub. If a Rust method receives a `Promise<T>` from JS (e.g., `awaitAndGetPromise`), the Rust side gets the unwrapped `T` value. This works for the common case but doesn't support awaiting arbitrary JS promises from Rust. Not needed for Jazz.
- **Thread safety of callbacks**: The C++ trampoline and `std::function` capture handle thread dispatch, but Rust calling the callback still invokes the trampoline synchronously. For Jazz's `NitroScheduler`, the callback should be safe to call from any thread since the C++ `std::function` would use CallInvoker internally — but this is untested.
- **Build system is untested** on actual iOS/Android builds. The CMake and Podspec logic looks correct but hasn't been validated end-to-end with a real React Native app.

---

## Files to Create/Modify

| Action | Path                                            | Purpose                                         |
| ------ | ----------------------------------------------- | ----------------------------------------------- |
| Create | `crates/groove-nitro/`                          | New Nitro binding crate                         |
| Create | `crates/groove-nitro/src/lib.rs`                | HybridObject impl                               |
| Create | `crates/groove-nitro/src/scheduler.rs`          | NitroScheduler                                  |
| Create | `crates/groove-nitro/src/sync_sender.rs`        | NitroSyncSender                                 |
| Create | `crates/groove-nitro/src/types.rs`              | Value conversion (likely copy from groove-napi) |
| Create | `crates/groove-nitro/Cargo.toml`                | Crate config                                    |
| Create | `crates/groove-nitro/groove-nitro.nitro.ts`     | Nitro TypeScript spec                           |
| Modify | `packages/jazz-tools/src/runtime/client.ts`     | Add NitroRuntime as a third Runtime backend     |
| Modify | `packages/jazz-tools/src/drivers/types.ts`      | Potentially extend Runtime interface            |
| Update | `specs/todo/b_launch/react_native_packaging.md` | Record Nitro as the chosen approach             |
