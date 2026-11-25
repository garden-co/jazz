---
"jazz-tools": patch
---

End-to-end encryption: API changes to the bindings and Rust crates.

**Breaking changes**

- **Node (napi) `openMemoryKeyless`:** takes `proof: { token, claimedAuthor, appId }` (`openMemoryKeyless(schema, config, bindings, appId, environment, proof?)`); a session proof must name its app.
- **Node (napi) `EncryptionRuntime` and `KeylessProof`:** removed; an encrypted open builds its runtime itself.
- **Node (napi) `NapiDb.takeStoreRequests` and `NapiDb.replyStore`:** removed.
- **Browser (wasm) `openBrowserKeyless`:** `openBrowserKeyless(…, token?, claimedAuthor?)` is now `openBrowserKeyless(…, proof?)` with `proof: { token, claimedAuthor, appId }`.
- **Browser (wasm) `WasmDb.takeStoreRequests` and `WasmDb.replyStore`:** removed.
- **Kotlin `openWithStore`:** takes a typed `PrivateStore` instead of a `(String) -> StoreReply` callback.
- **Kotlin `StoreReply`:** removed.
- **Kotlin `devices()`:** throws when the runtime answers with something other than an array.
- **UniFFI exports `table_identity`, `encryption_ready`, `encryption_state` and `encryption_reports`, and their C `jazz_call` methods:** removed.
- **`jazz-ffi` `JazzError::Encryption { code, reason, io }`:** a new variant, so an exhaustive match on `JazzError` (Rust `match`, Kotlin `when`, Swift `switch`) needs a case for it. It is added after every existing variant, so earlier variants keep their UniFFI indexes; a host speaking contract 1 gets it as `Refused`, or as `Io` when `io` is set.
- **Swift `deviceId()`, `answer()` and `recoveryCreate()`:** throw instead of returning `""`.
- **`jazz-core` `JazzError::KeyRefusal { refusal, original }`:** a new variant for a write refused because its space's key is pending, which no longer matches `Write`/`WriteRejected` directly; `into_original()` gives the original error.
- **`jazz-core` `UnsupportedPolicy` and `SchemaConversionError::unsupported_policy()`:** removed.
- **`jazz-core` schema diagnostics:** the path of a refused `exists` condition is now `…Exists.condition`.
- **`jazz-e2ee` `content::repair`, `StreamValueReader`, `StreamResealer`, `EqualityValueBuf`, `EqualityItems`, `encrypt_stream_value`, `recovery_material_check` and `HpkeKeyPair::generate`:** removed.
- **`jazz-e2ee` `RefusalCode::{DeviceIdentityConflict, ApprovalClosed, SpacePending}` and `RecoveryFailure::{Cancelled, MaterialUnusable}`:** removed.
- **`jazz-e2ee` `EqualityValue`:** no longer has a lifetime or `Copy`.
- **`jazz-e2ee` successor records:** their fields moved into `.columns`.
- **`jazz-e2ee-client` `E2eeClient::{read, write, prepare_space, create_space, deliver_space, set_member_change_wait}`:** removed.
- **`jazz-e2ee-client` `E2eeClient::take_reports`:** only with the `testing` feature, as a test observation; hosts use `on_report` and `flush_reports`.
- **`jazz-e2ee-client` `SpaceCreation`, `CELL_TYPE_ENCODING` and `host::runtime::{Options, build}`:** removed.

**Other changes**

- A write waiting for a key retries on the error's structured `reason` and `space` fields, not its message text. The internal `retryAfterKey` helper changed shape, and its module no longer exports `KEY_WAIT_MS`.
- A retyped encrypted column is refused naming both types as `jazz.app.json` writes them, the sealed one first.
