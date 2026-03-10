# Nitro Modules Bridge for Jazz2

## Spike Status (2026-02-18)

The `spike/rn-fjall` branch validated the full Nitro-Rust-Fjall stack on iOS. This was a narrowly-scoped storage spike — not the full GrooveRuntime, just enough to prove the toolchain end-to-end.

### What was built

- **`crates/jazz-nitro`** — Rust crate with `StorageSpikeImpl`: open/write/read/flush/close against Fjall
- **`.nitro.ts` spec** — `StorageSpike` HybridObject with 5 sync methods (no Promises, callbacks, or ArrayBuffer yet)
- **Nitrogen codegen** — ran from the `boorad/nitro#feat/rust` fork, producing:
  - Generated trait (`HybridStorageSpikeSpec`) + FFI shims + C++ bridge headers
  - Wrapper-based `factory.rs` — the factory creates a `StorageSpikeImplWrapper` that delegates to `StorageSpikeImpl`, keeping impl code independent of the generated trait
  - iOS autolinking (`.rb`), Android autolinking (`.cmake`, `.gradle`)
- **`JazzNitro.podspec`** — CocoaPods spec with a `script_phase` that detects `PLATFORM_NAME`/`ARCHS`, maps to Rust target triple, and runs `cargo build --release`
- **`examples/rn-storage-spike`** — React Native 0.78 app exercising the HybridObject from TypeScript

### Key lessons from the spike

1. **Xcode CC env must be unset for Rust builds.** Xcode sets `CC`/`CXX`/`LD` to iOS SDK clang, which breaks host-targeted build scripts (proc-macros, libc crate). The podspec unsets them and instead sets `CARGO_TARGET_*_LINKER` for cross-compilation.

2. **Commit via `runtime.spawn` + `mpsc::sync_channel`, not `block_on`.** Fjall's `txn.commit()` is async. `runtime.block_on()` panics if the calling thread is already a tokio worker. The spawn+channel pattern (same as `tree.close()`) avoids this.

3. **Nitrogen wrapper pattern.** The generated `factory.rs` wraps the impl struct rather than having the impl struct directly implement the generated trait. This keeps `jazz-nitro/src/lib.rs` free of codegen dependencies — the impl crate only needs Fjall, not the generated trait.

4. **Podspec lives in the crate, not in `ios/`.** `JazzNitro.podspec` is at `crates/jazz-nitro/`, and the RN app's Podfile references it via a relative path. The generated Rust crate (`nitrogen/generated/shared/rust/`) is compiled by the podspec's script phase.

5. **Metro resolver needs workspace root.** The RN app's `metro.config.js` must include the workspace root in `watchFolders` and `nodeModulesPaths` for pnpm-hoisted dependencies to resolve.

### iOS benchmark results (Simulator, aarch64-apple-ios-sim)

| Op                    | Latency           |
| --------------------- | ----------------- |
| open (cold)           | 37.73ms           |
| write (single)        | 589µs             |
| read (single)         | 81µs              |
| flush                 | 8.30ms            |
| 10K sequential writes | 60ms (166K ops/s) |
| 10K sequential reads  | 11ms (924K ops/s) |

### What's not yet validated

- Android (emulator or device)
- iOS device (only simulator so far)
- Any advanced Nitro features: Promises, callbacks, ArrayBuffer/NitroBuffer, variants

---

## Context

Jazz2 has a Rust core (now in `crates/jazz-tools`) with two existing binding crates:

- **jazz-wasm** — browser via wasm-bindgen
- **jazz-napi** — Node.js server via napi-rs

React Native is the missing target. The existing specs (`react_native_packaging.md`, `react_native_storage_investigation.md`) had open questions: "NAPI via Hermes vs JSI vs Turbo Modules?" — **Nitro Modules is the answer.**

Jazz1 uses `uniffi-bindgen-react-native` for its RN bridge: Rust → UniFFI macros → C FFI → C++ JSI HostObject → TurboModule → TypeScript. This works but has drawbacks (see "Why Nitro over UniFFI" below).

Brad is adding Rust support to Nitro in a fork (`boorad/nitro#feat/rust`), which enables a direct Rust HybridObject implementation without a C++ intermediary layer.

---

## Where Nitro Fits in Jazz2

### What it replaces

Nitro does **not** replace jazz-wasm (browser) or jazz-napi (Node.js server). It creates a **third binding crate** — `jazz-nitro` — specifically for React Native on iOS and Android.

### Architecture diagram

```
                    ┌─────────────────────────────────────────┐
                    │         jazz-tools (Rust core)           │
                    │  RuntimeCore<S, Sch, Sy>                 │
                    │  SchemaManager, QueryManager, SyncManager│
                    └────┬──────────┬──────────┬──────────────┘
                         │          │          │
              ┌──────────┘    ┌─────┘    ┌─────┘
              ▼               ▼          ▼
    ┌──────────────┐  ┌────────────┐  ┌──────────────┐
    │  jazz-wasm   │  │ jazz-napi  │  │  jazz-nitro  │  ← NEW
    │ wasm-bindgen │  │ napi-rs    │  │ Nitro Rust   │
    │ browser/OPFS │  │ Node/Surreal│  │ RN/Fjall │
    └──────┬───────┘  └─────┬──────┘  └──────┬───────┘
           │                │                 │
           ▼                ▼                 ▼
      Browser JS       Node.js TS       React Native TS
      (Web Workers)    (Server)         (iOS/Android)
```

### What jazz-nitro wraps today vs. the target

**Today (spike):** `StorageSpikeImpl` — a standalone Fjall key-value store. No RuntimeCore, no scheduler, no sync. Just open/write/read/flush/close.

**Target:** `RuntimeCore<FjallStorage, NitroScheduler, NitroSyncSender>`

- **FjallStorage** — same persistent storage as jazz-napi (Documents dir on iOS, internal storage on Android)
- **NitroScheduler** — schedules `batched_tick()` on the RN JS thread via Nitro callback
- **NitroSyncSender** — sends sync messages back to JS via Nitro callback

---

## Nitro HybridObject Spec

This is the `.nitro.ts` spec that Nitrogen would consume. It mirrors the API surface already exposed by jazz-wasm and jazz-napi, but uses Nitro's native type system instead of JSON strings.

```typescript
// jazz-nitro.nitro.ts (target — not yet implemented)
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
 * Wraps Rust RuntimeCore with Fjall storage.
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

5. **Constructor via factory.** Nitro HybridObjects are constructed natively. The constructor args would be: `schemaJson: string, appId: string, env: string, userBranch: string, dataPath: string, tier?: string`. Nitrogen now auto-generates `factory.rs` with the `create_HybridTSpec()` function and a wrapper struct that delegates to the impl crate (see fork commit `ab6e81b4`).

---

## Rust Implementation — Current and Target

### Current layout (spike)

```
crates/jazz-nitro/
├── Cargo.toml
├── JazzNitro.podspec           # iOS build (script_phase runs cargo)
├── jazz-nitro.nitro.ts         # Nitro TypeScript spec
├── nitro.json                  # Nitrogen config (implCrate, autolinking)
├── package.json                # workspace package for Nitrogen resolution
├── tsconfig.json
├── src/
│   └── lib.rs                  # StorageSpikeImpl (Fjall ops only)
└── nitrogen/generated/         # All Nitrogen codegen output
    ├── shared/c++/             # C++ bridge headers
    ├── shared/rust/            # Generated trait, FFI shims, factory.rs
    ├── ios/                    # Autolinking, Swift bridge
    └── android/                # Autolinking, cmake, gradle
```

### Target layout (full RuntimeCore bridge)

```
crates/jazz-nitro/src/
├── lib.rs              # HybridObject impl wrapping RuntimeCore
├── scheduler.rs        # NitroScheduler (Scheduler trait)
├── sync_sender.rs      # NitroSyncSender (SyncSender trait)
└── types.rs            # Value conversion (reuse from jazz-napi)
```

The structure will mirror jazz-napi. Key differences:

- jazz-napi uses `napi::ThreadsafeFunction` for callbacks → jazz-nitro will use Nitro's `Func_*` callback mechanism (`fn_ptr` + `userdata` + `destroy_fn`)
- jazz-napi uses `#[napi]` macros → jazz-nitro implements a trait generated by Nitrogen from the `.nitro.ts` spec
- Both use `Arc<Mutex<RuntimeCore<FjallStorage, ...>>>` since they're multi-threaded native

### NitroScheduler

```rust
// Analogous to jazz-napi's NapiScheduler
struct NitroScheduler {
    callback: /* Nitro Func_* callback */,
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
// Analogous to jazz-napi's NapiSyncSender
struct NitroSyncSender {
    callback: /* Nitro Func_* callback */,
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

Row data crosses as `ArrayBuffer` using the existing `jazz_tools::query_manager::encoding` module. Rust encodes rows into a compact binary format, passes the buffer zero-copy to JS, and a thin TS decoder on the other side interprets the bytes. This is the same pattern jazz-wasm uses for subscription deltas (see `build_wasm_delta_json` in `jazz-wasm/src/runtime.rs`), but with binary instead of JSON.

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
- **`react_native_storage_investigation.md`**: The spike work (Fjall on iOS/Android) is **validated on iOS**. jazz-nitro uses the same Fjall patterns as jazz-napi.
- **`swift_bindings.md`** / **`kotlin_bindings.md`**: Nitro is specifically for React Native. Pure Swift/Kotlin native apps would still use their own FFI approach (UniFFI, C bridge, JNI). But if the primary mobile target is RN, Nitro covers it.

---

## What Needs to Happen (Sequencing)

1. ~~**Complete Nitro Rust support** (`boorad/nitro feat/rust`)~~ — DONE. Sync methods, factory, FFI shims, iOS/Android build system all work.
2. ~~**Complete RN storage spike**~~ — DONE (iOS Simulator). Fjall opens, writes, reads, flushes, closes via Nitro Rust bridge. See `examples/rn-storage-spike/RESULTS.md`.
3. **Validate on Android** — run the storage spike on Android emulator via cargo-ndk + CMake
4. **Validate advanced Nitro features** — Promises (for `query()`), callbacks (for subscriptions/sync), ArrayBuffer/NitroBuffer (for zero-copy binary transfer). These are implemented in the fork but untested in Jazz.
5. **Evolve `jazz-nitro` to wrap RuntimeCore** — expand from storage-only spike to the full `GrooveRuntime` HybridObject spec (see Nitro HybridObject Spec below), following jazz-napi's patterns
6. **Wire into `jazz-tools` TS** — add a React Native runtime target alongside WASM and NAPI in the `Runtime` interface / `JazzClient`
7. **React Native example app** — validate end-to-end: insert → query → subscribe → sync

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

For query results and subscription deltas (the hot path), use `ArrayBuffer` to pass binary-encoded row data. The Rust side already has `jazz_tools::query_manager::encoding::decode_row` — encode on Rust side, pass the bytes zero-copy, decode on TS side. This is faster than JSON _and_ faster than per-value JSI conversion for large result sets.

**Option B: Hybrid approach**

- **CRUD params** (`insert`, `update`): These are small and infrequent. JSON strings are fine here — simplicity wins.
- **Query results / subscription deltas** (the actual hot path): Use `ArrayBuffer` for zero-copy binary transfer.
- **Sync messages**: Already binary-encoded in the sync protocol. Pass as `ArrayBuffer` directly — no serialization at all.

This matches what jazz-wasm/jazz-napi already do for sync messages (binary frames), but extends it to query results too.

### The key insight

The Nitro boundary has _two_ hops for Rust: Rust → (extern "C") → C++ → (JSIConverter) → JS. The first hop (Rust↔C++) is where your fork operates. The second hop (C++↔JS) is Nitro's existing JSIConverter layer. For `ArrayBuffer`, _both_ hops are zero-copy: Rust passes a pointer to C++, C++ wraps it in `jsi::ArrayBuffer` with `NativeState`, JS reads the memory directly. That's as good as it gets.

---

## Q2: Fork Status — `boorad/nitro#feat/rust` through `ab6e81b4`

All 6 originally-identified gaps were addressed by `6e222071`. Four more commits landed after that (`540157e8`..`ab6e81b4`) adding Edition 2024 compat, auto-generated factory.rs, `rust.implCrate` config, and the wrapper delegation pattern. Here's the full assessment:

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

5. **Factory function** — DONE (and now fully auto-generated, see item 8 below)
   Generated trait includes a doc comment with the factory signature. As of `9281fccb`, `factory.rs` is fully auto-generated by Nitrogen — no hand-written factory needed.

6. **memorySize** — DONE
   Trait includes `fn memory_size(&self) -> usize { 0 }` (default impl). C++ bridge calls it from `getExternalMemorySize()`. Jazz can override to report RuntimeCore's storage/cache size.

### What was added after `6e222071` (commits `540157e8`..`ab6e81b4`)

7. **Rust Edition 2024 compatibility** (`540157e8`) — DONE
   - `#[unsafe(no_mangle)]` instead of `#[no_mangle]`
   - Explicit `unsafe {}` blocks inside `unsafe fn` bodies
   - `extern "C"` declarations moved to file scope in iOS/Android autolinking

8. **Auto-generated `factory.rs`** (`9281fccb`) — DONE
   Nitrogen now generates `factory.rs` with `create_HybridTSpec()` factory functions for each Rust-autolinked HybridObject in `nitro.json`. No more hand-written factory boilerplate.

9. **`rust.implCrate` config** (`c9c91e10`) — DONE
   New optional `rust.implCrate` field in `nitro.json`. When set, the generated `Cargo.toml` includes `[workspace]` (prevents parent workspace absorption) and `[dependencies]` with a path dep to the impl crate. `factory.rs` generates proper `use impl_crate::StructName` imports.

10. **Wrapper delegation pattern** (`ab6e81b4`) — DONE
    `factory.rs` now generates a wrapper struct (e.g. `StorageSpikeImplWrapper`) that implements the generated trait by delegating each method to the user's impl struct. This bridges the cross-crate gap — the user's struct in the impl crate doesn't need to depend on or directly implement the generated trait. Also fixes parameter parsing for types with nested parens (e.g. `Box<dyn Fn() -> f64>`).

### Practical note: `lib/` must be committed in the fork

GitHub tarballs (used by pnpm for git deps) don't include build output. Both `packages/nitrogen/lib/` and `packages/react-native-nitro-modules/lib/` must be force-added (`git add -f`) to the `feat/rust` branch. Commits `a622703a`, `2f821eaa`, and `858402f9` handle this.

### Remaining concerns (minor)

- **Error propagation** is still string-only (`*const c_char` → `std::runtime_error`). Fine for Jazz.
- **Promise as a parameter** (not return type) still uses the phantom stub. If a Rust method receives a `Promise<T>` from JS (e.g., `awaitAndGetPromise`), the Rust side gets the unwrapped `T` value. This works for the common case but doesn't support awaiting arbitrary JS promises from Rust. Not needed for Jazz.
- **Thread safety of callbacks**: The C++ trampoline and `std::function` capture handle thread dispatch, but Rust calling the callback still invokes the trampoline synchronously. For Jazz's `NitroScheduler`, the callback should be safe to call from any thread since the C++ `std::function` would use CallInvoker internally — but this is untested.
- ~~**Build system is untested** on actual iOS/Android builds.~~ **iOS validated** on the spike branch. The podspec script_phase approach works (with the Xcode CC env unset fix). Android build still untested.

---

## Files Created / To Modify

| Status | Path                                        | Purpose                                     |
| ------ | ------------------------------------------- | ------------------------------------------- |
| DONE   | `crates/jazz-nitro/`                        | Nitro binding crate (spike scope)           |
| DONE   | `crates/jazz-nitro/src/lib.rs`              | `StorageSpikeImpl` (Fjall only)             |
| DONE   | `crates/jazz-nitro/Cargo.toml`              | Crate config                                |
| DONE   | `crates/jazz-nitro/jazz-nitro.nitro.ts`     | Nitro TypeScript spec (`StorageSpike`)      |
| DONE   | `crates/jazz-nitro/nitro.json`              | Nitrogen config                             |
| DONE   | `crates/jazz-nitro/JazzNitro.podspec`       | iOS CocoaPods build integration             |
| DONE   | `crates/jazz-nitro/nitrogen/generated/`     | All Nitrogen codegen output                 |
| DONE   | `examples/rn-storage-spike/`                | React Native 0.78 test app                  |
| TODO   | `crates/jazz-nitro/src/scheduler.rs`        | NitroScheduler                              |
| TODO   | `crates/jazz-nitro/src/sync_sender.rs`      | NitroSyncSender                             |
| TODO   | `crates/jazz-nitro/src/types.rs`            | Value conversion (reuse from jazz-napi)     |
| TODO   | `crates/jazz-nitro/jazz-nitro.nitro.ts`     | Expand to full `GrooveRuntime` HybridObject |
| TODO   | `packages/jazz-tools/src/runtime/client.ts` | Add NitroRuntime as a third Runtime backend |
| TODO   | `packages/jazz-tools/src/drivers/types.ts`  | Potentially extend Runtime interface        |
