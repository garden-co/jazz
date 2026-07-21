# SessionClaims chatter investigation

## Spec finding

`crates/jazz/SPEC/8_sync_protocol.md` does not specify a normative
`SessionClaims` cadence. The sync chapter states that the semantic transport is
FIFO and binding/server byte transports sit below it:

> `Db` and `PeerConnection` keep the semantic `Transport` surface over
> `SyncMessage`.

It also says:

> The only ordering assumption is **per-link FIFO**.

The message table in the same section lists `CommitUnit`, `FateUpdate`,
`RegisterShape`, `Subscribe`, `SubscribeRejected`, `Unsubscribe`, `ViewUpdate`,
`FetchContentExtent` / `ContentExtents`, and catalogue messages, but does not
list `SessionClaims`.

`crates/jazz/SPEC/2_data_model_identity.md` is scoped to identity and shape; it
does not cover session-claim sync cadence.

First finding: SessionClaims cadence is currently unspecified by the spec. The
implementation grew a "send whenever claims are applied" behavior.

## Client-side emitters

- `crates/jazz/src/protocol.rs:26`: defines
  `SyncMessage::SessionClaims` as a "Trusted backend assertion of process-local
  auth claims for a write subject."
- `crates/jazz/src/db.rs:1875`: public `Db::set_identity_claims`.
  Before this change, every call updated local node state, pushed
  `PendingUpstreamCommand::SessionClaims`, and scheduled a tick.
- `crates/jazz/src/db.rs:4459`: `PeerConnection::tick` drains
  `PendingUpstreamCommand::SessionClaims` and sends
  `SyncMessage::SessionClaims` before later pending subscriptions and uploads.
- `crates/jazz-wasm/src/lib.rs:1126`: exported JS `setIdentityClaims`, which
  calls through to `Db::set_identity_claims`.
- `crates/jazz-wasm/src/lib.rs:527`: internal wasm adapter forwarding method.
- `crates/jazz-wasm/src/lib.rs:700-732` and same-pattern methods below:
  some identity-scoped write helpers synthesize default local-first claims
  before invoking identity writes.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:1399`:
  `applySessionClaims` calls `db.setIdentityClaims(session.identity,
  session.claims)`.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:451`,
  `483`, `511`, `543`, `583`: mutation paths call `applySessionClaims`.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:604`,
  `619`, `636`, `648`: dry-run policy checks call `applySessionClaims`.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:740`:
  one-shot query calls `applySessionClaims`.
- `packages/jazz-tools/src/runtime/native-runtime/native-runtime-adapter.ts:812`:
  subscription creation calls `applySessionClaims`.

Why the wire trace showed 2x: two independent TS operations commonly apply the
same session to the same native DB before the first core tick, for example the
front-door query/subscription path and local/background edge coverage attach.
Each call previously enqueued an identical pending `SessionClaims`. On the tick,
the core drained both pending commands before `RegisterShape`, `Subscribe`, or
`CommitUnit`, producing:

`SessionClaims`, `SessionClaims`, operation message.

This is not two wire layers both encoding claims. It is one core emitter fed by
repeat `setIdentityClaims` calls.

## Server-side behavior and cost

- `crates/jazz-tools/src/server/routes/websocket.rs:229-233`: websocket
  admission validates the session identity and builds `WebSocketAdmission`
  containing identity, converted claims, and `CommitUnitTrust::Session`.
- `crates/jazz-tools/src/server/routes/websocket.rs:554-555`: the route opens
  the core server shell with that identity, claims, and trust.
- `crates/jazz-server/src/loopback_websocket.rs:429-435`: loopback websocket
  admission similarly accepts the subscriber session with admitted claims.
- `crates/jazz-server/src/lib.rs:623-646`: server sessions call
  `accept_subscriber_session_with_claims_and_trust`.
- `crates/jazz/src/db.rs:3376`: accepting a subscriber with claims installs
  them in the server node before serving the connection.
- `crates/jazz/src/node/ingest.rs:139-145`: inbound
  `SyncMessage::SessionClaims` is applied only when the ingest context exists
  and `context.trust == CommitUnitTrust::TrustedBackend`.
- `crates/jazz/src/node/ingest.rs:1926-1934`: ordinary session commit uploads
  use the authenticated connection identity as permission subject; trusted
  backends may use the transaction permission subject.

For ordinary browser websocket sessions, the server does not require
`SessionClaims` per message. It already cached admitted claims on connection
open, and later `SessionClaims` messages from `CommitUnitTrust::Session` links
are ignored. The per-message cost is decode/dispatch plus one branch; no JWT
verification is performed for these semantic messages. The bigger cost is wire
volume, websocket batching pressure, log/trace noise, and extra core ticks.

For trusted backend links, `SessionClaims` is load-bearing because the backend
may assert claims for identities whose writes it forwards.

## Contract and fix

Recommended contract:

- Session/admission claims for ordinary websocket clients are connection state.
- `SessionClaims` semantic messages are only needed when process-local claims
  change after connection setup, and are load-bearing for trusted backend
  attribution.
- Multisink/multiple-identity use remains supported: dedupe is per identity and
  exact claim map, not per connection globally.

Implemented minimal mechanical fix:

- `crates/jazz/src/node/mod.rs:683` now returns whether
  `set_session_claims` changed the stored claims and skips cache invalidation
  when identical.
- `crates/jazz/src/db.rs:1875` only enqueues upstream
  `PendingUpstreamCommand::SessionClaims` when that return value is true.
- `crates/jazz/src/db/tests.rs:7763` adds
  `repeated_identical_session_claims_emit_once_before_subscribe`, asserting that
  two identical calls produce exactly one `SessionClaims` before
  `RegisterShape` and `Subscribe`.

This does not prevent later changed claims from being sent, and it does not
collapse different identities.

## Gates

- `cargo fmt`: exit code 0.
- `cargo fmt --check`: exit code 0.
- `cargo test -p jazz db::tests::repeated_identical_session_claims_emit_once_before_subscribe -j 2 -- --exact`: exit code 0.
- `cargo test -p jazz -j 2`: exit code 0.
- `cargo test -p jazz-server -j 2`: exit code 0.
- `cargo test -p jazz-tools --features test -j 2`: exit code 0.
- `cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -j 2 -- --exact`: exit code 0.
- `JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle -j 2`: exit code 0.
