# SQLite native relay compatibility

The native relay uses the SQLite database API for its durable owner and its
in-memory foregrounds. A foreground write reaches Local durability only after
the owner stores it. Global settlement still requires the authority's receipt.
Initialization reserves an identity at that owner before publication or
cancellation. The native command ABI remains unchanged.

This is a source port under validation. The complete native relay unit suite and
installed Android and iOS artifacts are not qualified by the integration tests
listed below.

The Linux React Native harness can now build a sealed NAPI artifact with the
optional byte bridge and run the public TypeScript API through this relay. It
does not prove JSI or TurboModule installation on a device. A serial run passes
all 103 existing tests across 15 files, with fixtures and assertions unchanged.

Foreground subscriptions use the same ordinary local view as binding reads;
explicit policy reads would hide offline previews. The owner still admits the
exact subject, and remote delivery remains authority-checked. Shared binding
write and cleanup hooks schedule the native owner, including streamed writes.
A standalone no-op retains its existing receipt without uploading different
operation bytes under the preceding transaction identity. This changes no
storage or wire encoding and grants no authority acceptance.

Native typed transaction mutations use the shared binding refusal path. If an
insert, update, upsert, delete or restore is refused because of the row's state,
the transaction retains its first refusal and commit returns a rejected receipt.
Catching a staging error cannot publish the unit's other writes. Rollback clears
that refusal through the same binding lifecycle. New public C ABI regressions
cover all five mutation kinds in mergeable and serializable transactions, plus
rollback followed by a successful replacement transaction.

Streamed attachments use that same refusal path in the shared binding, NAPI and
native C ABI. An upload rejected for its row's state cannot leave its companion
writes eligible for commit. Its capability is consumed on attachment; rollback
clears the refusal. Invalid owner, schema, identity and transaction kind still
fail their original core checks. New binding and C ABI regressions failed before
this change; the native case also checks successful commit and rollback with a
large text value through an admitted private owner and a real authority.

A clean foreground node handoff reserves a clock position strictly beyond the
previous holder's high-water mark before exposing the replacement foreground.
Even a holder that closes without writing advances the next handoff's floor.
Exhaustion fails the attach operation; it cannot wrap to an earlier identity.
This uses the existing sequence floor and changes no storage or wire encoding.

Socket-driven pumps notify foreground consumers through the same path as owner
turns. An unwaited authority rejection therefore reaches the application without
an unrelated read or explicit settlement waiter. Explicit waiters retain their
rejection; observe-only waiters leave the event available. The native subscribe
command also forwards the existing `observe_accepted_evidence` option. Only an
opted-in observer receives an empty delta when acceptance changes the evidence
without changing row contents.
Foreground notification checks whether the stream has a deliverable event,
including its authority and content gates. A drained local stream or a Remote
stream waiting offline does not request another platform turn merely because
the socket or owner was pumped. New row events and unwaited rejections still
notify the foreground through the same path.

Strict Remote subscriptions retain their authority gate after the opening.
During an outage they withhold local previews. After reconnect they reconcile
against the last delivered result, preserving removals from revocation and
ordered delta indexes. This also covers consumers that did not poll during the
outage. Each strict stream retains its delivered result in memory; reconciliation
uses that result and its existing subscription group, with no new database scan.
Local-first subscriptions keep their existing fallback behaviour. No storage,
wire, permission or acceptance encoding changes.

## Durable owner file

`jazz.sqlite.native-relay-file.v1` uses the table `_jazz_native_relay_v1`:

| Column    | Encoding                                                                  |
| --------- | ------------------------------------------------------------------------- |
| `slot`    | SQLite INTEGER, always 1                                                  |
| `version` | SQLite INTEGER, always 1                                                  |
| `scope`   | SQLite TEXT, the existing `jazz-native-relay-owner-v1:` encoding          |
| `owner`   | The same TEXT for an admitted relay; NULL for an unadmitted registry open |
| `node`    | BLOB containing exactly 16 node UUID bytes                                |
| `author`  | TEXT containing the canonical author subject                              |

The scope concatenates application, storage and authentication scope. Each value
has its UTF-8 byte length, a colon, then the value. The frozen fixture records
all TEXT and BLOB fields as hex bytes. SQLite owns page and journal formatting;
those physical pages are not a second application codec.

Opening takes an exclusive lock on a stable `.owner.lock` inode. It validates the
marker before opening the core database. An occupied SQLite file without this
marker is refused with `unsupported_storage_codecs`. Scope, node and author
mismatches are refused.
An admitted owner cannot reopen in generic mode. A
closed generic root can be promoted by trusted admission for its exact scope,
node and author, using the protocol below. A private account session recovers the stored
node before opening, since its initial candidate node is fresh on every session.
Account-host routing may recognise a linked login in the base file to select a
separate login store. The selected file's preliminary read checks its exact scope
and author. Opening checks all the fields again under the lock. An explicit host identity does not get silently
replaced.

SQLite uses FULL synchronous writes, and the parent directory is synced after
file creation. The core database owns its WAL and durability receipts. The lock
inode is retained after close so another process cannot split ownership by
replacing it.

### Private account handoff

The native account-session route retains a separate store for each linked login
under the same account lock. The first login keeps the existing account file;
other logins use pinned sibling filenames. Each pending journal remains bound to
its original login and resumes only under that login. Generic private-scope
admission cannot select this account-host route.

The [native login storage proof](sqlite-native-account-login-storage.md) names
the filename encoding, pins its bytes and explains selection, locking and
compatibility. The direct core and Node account-owner APIs retain their separate
[drained handoff contract](sqlite-private-owner-handoff-compatibility.md).

### Closed generic linked-identity handoff

`jazz.sqlite.native-generic-handoff.v1` is a transition over the existing v1
marker, pinned by `sqlite_native_generic_handoff_v1.json`. It changes only the
scope and author TEXT values; version, node and the NULL owner are unchanged.
Two authenticated host identities must name the same non-system account, the
application and storage namespaces must match, and the node must be unchanged.
The old owner must have closed and invalidated its aliases before the new open
can acquire the exclusive file lock.

Both pending journals must be empty. The check uses the existing transaction
keys and pending-upload index; it does not scan settled history. An existing
private core scope refuses the transition, including an interrupted promotion
whose native marker still says generic. Neither shared account IDs nor the
handoff grants private pending previews, delegated claims or Global acceptance.
Refusal preserves the old marker and upload bytes, and the original identity
can reopen normally.

The lock spans validation, core opening and the FULL synchronous marker update.
The update happens only after core open and cached catalogue validation succeed;
a crash before it leaves the old admission marker. Once committed, an existing
v1 reader can open using the new exact identity. Earlier builds cannot perform
the transition themselves. There is no new file layout or wire encoding.
Private account switching and retaining a previous login's pending uploads are
not implemented by this generic transition.

### Generic owner promotion

`jazz.sqlite.native-owner-promotion.v1` is an ordered transition over the existing
`jazz.sqlite.native-relay-file.v1` and `jazz.sqlite.owner-scope.v1` records. It adds
no columns or wire fields. The promotion fixture pins the nullable owner and
canonical subject bytes in all three states:

| State      | Native `owner` | Core `subject`   | Permitted open                      |
| ---------- | -------------- | ---------------- | ----------------------------------- |
| Generic    | NULL           | Absent           | Generic, or exact trusted admission |
| Core bound | NULL           | Admitted subject | Exact trusted admission only        |
| Admitted   | Encoded scope  | Admitted subject | Exact trusted admission only        |

The exclusive file lock spans validation, core admission and the marker update.
Core admission checks pending writes and forwarded uploads against the admitted
subject before creating a private projection. Other subjects' pending work is
refused; the native marker stays generic. Successful promotion preserves original
transaction identities and attribution, and does not accept their writes: only
the authority can do that.

The core subject is bound first. If the process stops before the native marker is
updated, a later generic open refuses the root, and the same trusted admission
can finish the marker update. The marker uses a FULL synchronous SQLite commit.
The tests exercise this intermediate state, both reopen modes, pending-write
delivery, wrong scope/node/author refusal, and the exact pre/post marker bytes.

The completed bytes remain valid for the existing v1 reader. Older unpublished
native candidates lack the intermediate-state recovery check; those candidates
must not be mixed with a host that enables this promotion protocol. Deploy the
core and native host together. Linked-provider author changes are not admitted
by this protocol.

## Catalogue cache

`jazz.sqlite.native-catalogue-cache.v2` replaces the retired key-value cache.
Its table is `_jazz_native_catalogue_v2`, with INTEGER primary key `slot = 1` and
BLOB `capture`. The BLOB is:

1. The four bytes `4a 4e 43 02` (`JNC` and version 2).
2. Application namespace: a big-endian u32 byte length and UTF-8 bytes.
3. Storage namespace in the same form.
4. Capture: a big-endian u32 byte length and exactly that many bytes.

The capture retains `jazz.sqlite.catalogue-capture.v1`. Account and session
identity remain outside this shared metadata cache. The existing cache filename
hash and its `jazz-native-catalogue-root-v1` domain remain unchanged.

A stable lock covers cache creation, loading and replacement across account
owners. Replacement must pass the core's monotonic catalogue validation. A
failed write retains the exact pending capture for retry. Readiness cannot report
success before the capture is stored. Old `JNC` version 1 envelopes, foreign
SQLite layouts, wrong scopes and malformed lengths are refused. They are never
interpreted as an empty cache or overwritten to make an open succeed.

This port provides no migration from the previous native storage layout. A
release must deploy the SQLite native artifacts and matching SDK together.
Refusal preserves an old root for an explicit migration or reset decision.

## Native socket carrier

`jazz.sqlite.native-relay-carrier.v1` names the carrier used after the existing
native connector has authenticated and negotiated Hello. Each logical frame
contains exactly one postcard `jazz::db::SyncMessage`, matching the SQLite
server. It adds no enum tags, session claims or new authority privileges.
Semantic frames are bounded by the existing logical-message limit. Trailing or
malformed bytes fail the connection. A full outbound queue returns the original
message for retry, preserving order and ownership.

The existing `WireFrame::Error` control frame can terminate the stream; its retry
classification is preserved. Only the existing backpressure/not-ready classes
permit reconnect. This carrier does not support a mixed legacy Groove peer and
SQLite peer. The Hello envelope alone does not establish such compatibility.

A terminal handshake failure reports the error and stops that socket worker.
It does not repeatedly submit unchanged credentials or install a semantic link.
An explicit host reconnect replaces the stopped worker and can clear the error
after successful admission. Retryable connection failures and loss of an already
established link retain their reconnect path. The typed connector result chooses
this behaviour; diagnostic strings do not decide whether to retry.

## Evidence

`crates/jazz-native-relay/fixtures/sqlite_native_relay_v1.json` pins owner marker
fields, a complete cache envelope containing the existing frozen catalogue
capture, and a semantic wire frame.

The new `sqlite_relay` integration target exercises the public C ABI with real
SQLite owners and authorities. It covers local/global settlement, restart with
an offline upload, rejected receipts, empty transactions, initialization
reservation/cancellation/publication, permission advice, storage refusal and
scope isolation. Additional cases cover rejection event ownership and accepted-
evidence notifications. A streamed text test checks authority acceptance and hydrated
subscription delivery. A real JWT/account-registry/WebSocket test reopens the account
owner, reads its accepted write and obtains authority advice.

The `sqlite_wire` target checks frozen bytes, lossless backpressure, malformed
and oversized input, and retry-error propagation. These tests do not replace
the full relay test suite, power-loss testing or installed mobile qualification.
The `sqlite_socket` target separately checks terminal versus retryable handshake
outcomes through the public socket-worker seam and verifies that a failed remote
admission leaves the local owner usable.
