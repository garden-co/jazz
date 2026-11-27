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
- **`jazz-app-support` `with_key_wait`:** takes the client first (`with_key_wait(&client, || …)`), whose closing ends its wait.
- **`jazz-core` `EncryptionRuntime::begin_streaming_upload`:** takes a second argument, `ended`, the future of the client's closing; a wait for the space's key ends with it (`JazzError::ChannelClosed`).
- **Rust `jazz::db::DbConfig` and `jazz::db::InsertOptions` (released in 2.0.0-beta.5):** each gains a public field, `add_creator_as_member` (`bool` on `DbConfig`, `false` from `DbConfig::new`; `Option<bool>` on `InsertOptions`). Neither struct is `#[non_exhaustive]`, so code that builds one with a struct literal naming every field no longer compiles. Add the field, or use `DbConfig::new` / `..InsertOptions::default()`.
- **Rust `JazzClient` reads inside a transaction (released in 2.0.0-beta.5):** they now read at the tier they ask for. In beta.5 every transaction read was local-first, whatever its tier. A failed `ReadTier::Remote` read in a transaction now returns `JazzError::QueryFailed`, which carries the database error and its code, instead of `JazzError::Query` with the message only. Other tiers still return `JazzError::Query`.

**Other changes**

- Removing a space member from Rust, Swift, Kotlin or Go (`remove_member`, `removeMember`, `RemoveMember`) commits a serializable transaction even when it finds no membership to delete, so that the authority confirms the read. That sends a serializable commit with reads and no writes. Compatibility with released servers: the shape isn't new on the wire. The commit's types (`WireKind`, `WireRead`) and the protocol crate are unchanged since 2.0.0-beta.5, and an empty list of ops encodes like any other. Servers have validated read-only serializable commits since 2.0.0-alpha.60: an unchanged read commits, and a stale one fails `transaction_conflict` (`a_read_only_serializable_commit_validates_its_reads` in `crates/jazz/tests/serializable_sync.rs`, in every release since). Member removal is the only API that sends one, and it needs an encryption-capable server, which no release before this one is. Servers from alpha.59 and earlier speak wire v3 and refuse the connection at the handshake.
- Closing an app ends the waits for encryption: a write waiting for its space's key and `encryption().ready(…)` in Rust fail `Stopped` (`closed` through `jazz-ffi`: `waitUntilReady` in Swift, Kotlin and Go), and TypeScript's `db.encryption.ready()` rejects with code `closed`, instead of holding the close until their deadline.
- Closing an app also ends an encrypted streaming upload waiting for its space's key (Rust fails `ChannelClosed`, `closed` through `jazz-ffi`) and a TypeScript write waiting for its space's key (it rejects with an `EncryptionError` of code `closed`).
- A C host's private store callback may call `"close"` on its own handle: it closes without waiting, as the callback runs on the client's thread, and the client stops once the callback returns (see `jazz_ffi.h`). It used to deadlock.
- That close from inside the store callback can't wait for writes, so it ignores a `timeoutMs`: when it then loses writes (memory storage, unsent), it fails `unsynced` with `pending` (`Unsynced { pending }` through UniFFI; `io` without contract 2) instead of answering null, as a close that waited would at its timeout.
- An encryption call cut short by its handle's close (`jazz-ffi`'s `encryptionCall`, and Swift's, Kotlin's and Go's encryption operations) reports `closed`, not `io` (the private store's "the app is closed").
- A write waiting for a key retries on the error's structured `reason` and `space` fields, not its message text. The internal `retryAfterKey` helper changed shape, and its module no longer exports `KEY_WAIT_MS`.
- A retyped encrypted column is refused naming both types as `jazz.app.json` writes them, the sealed one first.
