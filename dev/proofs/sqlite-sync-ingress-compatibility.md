# SQLite semantic ingress exactness

SQLite host and server ingress now accept one complete canonical postcard
`SyncMessage` per frame. The shared decoder checks the existing logical-message
size limit, complete input consumption and canonical byte spelling. It reuses
`jazz_types::postcard_exact`; it introduces no new serializer or wire fields.

Previously, NAPI and the server used `postcard::from_bytes`, which silently
ignored an unread suffix. WASM queued arbitrary bytes for a later database tick,
where malformed input could reach a panicking decoder. The native relay rejected
suffixes but still accepted overlong varints. All now reject these inputs at their
public ingress boundaries. NAPI and WASM validate every frame in a batch before
queueing its valid prefix. Server and native carrier failures retain their
existing terminal connection behavior.

The existing `jazz.sqlite.native-relay-carrier.v1` sample for
`Unsubscribe { id: 128 }` remains `01 80 01`. Appending `00`, spelling its tag as
`81 00`, or spelling its ID as `80 81 00` is refused. The normal encoder already
emits the shortest spelling, so existing correctly encoded traffic is unchanged.
No stored bytes, enum tags, authority decisions or catalogue identities change.

The public NAPI/WASM regression exercises single and batch rejection through
sealed generated artifacts. A server regression checks that a malformed message
closes its session before a following catalogue request and leaves another
session usable. The native carrier regression checks that an overlong varint
cannot be followed by valid traffic on the same failed carrier.

The retired Groove semantic corpus still needs its separate port. These tests
do not replace its full message, compression and feature-negotiation coverage.
