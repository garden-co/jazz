---
"jazz-tools": patch
---

End-to-end encryption: API changes to the bindings and Rust crates, which callers of these surfaces must adapt to.

- **Node (napi):** `openMemoryKeyless(schema, config, bindings, appId, environment, proof?)` takes `proof: { token, claimedAuthor, appId }`; a session proof must name its app. The `EncryptionRuntime` class and `KeylessProof` are removed (an encrypted open builds its runtime itself), and so are `NapiDb.takeStoreRequests` and `NapiDb.replyStore`.
- **Browser (wasm):** `openBrowserKeyless(…, token?, claimedAuthor?)` is now `openBrowserKeyless(…, proof?)` with `proof: { token, claimedAuthor, appId }`. `WasmDb.takeStoreRequests` and `WasmDb.replyStore` are removed.
- **Kotlin and UniFFI:** `openWithStore` takes a typed `PrivateStore` instead of a `(String) -> StoreReply` callback; `StoreReply` is removed. The UniFFI exports `table_identity`, `encryption_ready`, `encryption_state` and `encryption_reports`, and their C `jazz_call` methods, are removed. Kotlin `devices()` throws when the runtime answers with something other than an array.
- **`jazz-ffi` errors:** `JazzError::Encryption { code, reason, io }` is a new variant, added after every existing one, so earlier variants keep their indexes. A host speaking contract 1 gets it as `Refused`, or as `Io` when `io` is set.
- **Swift:** `deviceId()`, `answer()` and `recoveryCreate()` throw instead of returning `""`.
- **`jazz-core` (Rust):** a write refused because its space's key is pending is `JazzError::KeyRefusal { refusal, original }`, so it no longer matches `Write`/`WriteRejected` directly; `into_original()` gives the original error. `UnsupportedPolicy` and `SchemaConversionError::unsupported_policy()` are removed, and the diagnostic path of a refused `exists` condition is now `…Exists.condition`.
- **`jazz-e2ee` and `jazz-e2ee-client` (Rust):** public items without a caller are removed: `content::repair`, `StreamValueReader`, `StreamResealer`, `EqualityValueBuf`, `EqualityItems`, `encrypt_stream_value`, `recovery_material_check`, `HpkeKeyPair::generate`, `RefusalCode::{DeviceIdentityConflict, ApprovalClosed, SpacePending}`, `RecoveryFailure::{Cancelled, MaterialUnusable}`, `E2eeClient::{read, write, prepare_space, create_space, deliver_space, set_member_change_wait, take_reports}`, `SpaceCreation`, `CELL_TYPE_ENCODING` and `host::runtime::{Options, build}`. `EqualityValue` no longer has a lifetime or `Copy`, and successor fields moved into `.columns`.
- **Generated bindings:** the members table (`__e2ee_members`) has no update, upsert, restore, text edit, splice or update advice in Rust, Swift, Kotlin or Go, since its policy refuses every update.
- **TypeScript internals:** a write waiting for a key retries on the error's structured `reason` and `space` fields, not its message text; the internal `retryAfterKey` helper changed shape and `KEY_WAIT_MS` is no longer exported from its module.
