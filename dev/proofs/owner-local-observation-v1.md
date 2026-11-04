# Owner-local accepted observations

This Rust capability returns partial, possibly stale candidates from one SQLite
owner. Each returned body has exact accepted state and content origins. It does
not promise fresh or complete history, membership, authority SELECT permission,
or an authority transaction predicate. It makes no network request.

`local_observation_cut` freezes the actual owner instance, authority namespace,
schema and effective scope, a locally known authority coordinate, and a durable
evidence-admission watermark. Several `observe_local` calls can share that cut.
They use the existing query preparation, governing `ReadSpec` and `Cut` machinery,
with current applicable policies and no pending prefix. No transaction or staged
write is opened, so success, error and dropping the cut leave no batch to commit
or roll back. Ordinary transaction pending-prefix behaviour is unchanged.
Capture returns `NotObserved` while a SQLite transaction or savepoint remains
active: its evidence admission numbers may be reused after rollback. Capturing
outside that transaction freezes only committed admission state.

Closing or stopping mutation admission retires local capabilities in every view
of that owner. Explicit retirement is terminal for these capabilities; a reopened
owner is a different instance. The stored authority namespace is reused rather
than copied into another durable owner table.

## Proof kinds

- `LocalSubmission` (0): a genuine Global accepted receipt, normal body
  reconciliation and captured original local submission provenance. A receipt
  proves the accepted unit/content, not SELECT under the submitted claims.
- `AuthorityRead` (1): an actual read performed by this authority handle under
  its current-owner or exact explicitly selected governing context, paired with an
  exact body already held by the store.

The remote accepted-read envelope does not identify its authenticated BundleId.
It cannot enter the authority-proof path. A replica's upstream pin remains
UNKNOWN; a local selected bundle is not evidence of that upstream pin. Missing,
legacy, malformed or ambiguous optional provenance cannot manufacture evidence.
Ordinary valid commits retain their existing admission and settlement contract.

Current-main public `pinned_view` selects a resolved policy bundle; it does not
authenticate a session. The privileged local companion still requires the actual
live admitted receiver slot and both owner handles. Its route cannot be created
from a caller-selected bundle, schema or account/node strings alone. This core
uses a private effective-scope value from ordinary read-session operands and each
validated staged operation's identity; it needs no accepted-read transport or
bound-transaction host API. Main normalizes `Session(SYSTEM)` to the database's
local identity, and optional submission capture follows that same normalization.

Only narrow unbranched stored-row queries are supported. Joins, aggregates,
relations and history-table queries retain the accepted-shape refusal. Receipt
admission currently requires an exact content-bearing accepted image. A
content-free receipt cannot guess an earlier content origin. Exact authority
pairing can retain distinct state and content origins without reverse-position
inference. No projected row cache or second body cache is introduced.

## Trusted local Rust companion

The privileged host explicitly calls
`receiver.trusted_local_submission_route(&producer, admitted_subscriber)` using
two actual `Db` handles and a live admitted session. This is a new local capability
attachment, not a claim that `SubscriberId`, account equality or node bytes prove
locality. The caller must control the in-process producer/receiver connection.

The producer calls `export_local_submission(&route, receipt)`. Core reads the
real pending header/items and captured per-operation records, binding them to the
exact source node, transaction, operation order, complete Commit fingerprint,
namespace, owner instances and live session generation. Route and export fields
are private; neither has a byte constructor or `Deserialize` implementation.

The receiver attaches that value before serving the matching ordinary frame.
One optional companion occupies the session slot; replacing it discards only
optional provenance. The existing transport/backlog, authentication, schema
admission, policy checks, serialization validation and receipt path remain the
only Commit executor. A held frame retains its companion until it is served.
Stale owners/sessions, wrong owners/namespaces or a mismatched Commit supply no
provenance; they do not turn an ordinary valid Commit into a provenance refusal.
Actual SQLite failures remain storage failures with transactional rollback.

No browser RPC, NAPI/WASM method or upstream wire/header is added here. A future
worker adapter must establish a trusted local registration and transport
core-generated capture without accepting arbitrary decoded peer metadata. Its
local flush fence must include pending content and provenance. Owner retirement,
including a direct storage-owner retirement path, must invalidate these
capabilities before replacement. Native two-owner/file-reopen evidence does not
qualify browser/IndexedDB/OPFS persistence.

## Additive SQLite contracts

`jazz.sqlite.observation-submission.v1` is
`_jazz_observation_pending_v1(tx, op, schema, scope)`, keyed by the existing pending
transaction and operation index. Capture occurs before stored-name lowering;
each operation retains its own original schema and local governing scope.
An open batch captures that optional provenance at each successful staging call,
before another stage or commit can change the registered claims or owner context.
Commit persists those exact captures; it does not reconstruct them from current
claims. Unsupported capture does not alter ordinary commit permission checks.
Callback captures that have already lost this mapping remain unsupported for
observation rather than guessing a schema or refusing their ordinary commit.
The named `_jazz_observation_pending_forget_v1` AFTER DELETE trigger follows the
existing pending header's lifetime, including when an older writer deletes it.
No Commit or pending-header encoding changes.

`jazz.sqlite.accepted-observation.v1` is an evidence-only table containing:

- `admission INTEGER PRIMARY KEY AUTOINCREMENT`;
- original `schema`, stored `tbl`, row `id`, exact `state` coordinate and `scope`;
- `proof`, exact `state_node/state_tx` and `content_node/content_tx` forward
  references, and the expected `content` coordinate.

Schema/row ids have enforced 16-byte BLOB lengths; scope hashes have enforced
32-byte lengths. SQL and decoding enforce integer types, proof tags and
`0 < content <= state <= (2^52 - 1)`. Transaction ids retain all u64 bits through
the existing u64-as-i64 SQLite representation. Each lookup joins the exact
`_jazz_nodes` and `_jazz_transactions` keys and checks the expected coordinate.
No greatest-sparse-origin or replica `tx_ids` fallback is permitted.

The unique `(schema,tbl,id,state,scope,proof)` key bounds conflict checks across
the two proof kinds. Identical evidence retains its original admission;
conflicting pairs refuse admission. A later proof never upgrades an earlier
cut. An index on `state` supports the local coordinate watermark without scanning
all evidence. The admission watermark comes from SQLite's durable sequence, not
an in-memory counter or a pending-provenance allocation.

Pending content, pending metadata, accepted evidence allocation, receipt cleanup
and normal reconciliation use the existing savepoints. A validated accepted
image can temporarily be in the existing baseline while a pending prefix still
overlays SQLite. Admission checks only that exact accepted baseline or an exact
retained image; normal rebase persists it before the outer `sync_tick` commits.
No additional replay or cache is introduced. Any persistence/rebase failure rolls
back evidence and admission; evidence cannot be read before its accepted body is
stored. Delayed evidence receives a fresh admission even for an old coordinate.

## Scope bytes and compatibility

`jazz.sqlite.observation-scope.v1` encodes the domain bytes with a NUL, canonical
author UTF-8 (u32 little-endian byte length), u32 claim count, and byte-sorted
claim keys/values. Values use explicit tags 0–16, little-endian scalar bits,
length-prefixed UTF-8/bytes and counted nested containers. Integer widths,
negative zero, null and Some are distinct. Unsupported engine-only carriers,
depth beyond 64 and encodings beyond 1 MiB leave provenance unavailable.

The scope digest uses main's `jazz_types::sha256::derive_key` with that named
context: SHA-256 over u64 little-endian context length, context bytes and input.
`jazz.sqlite.observation-submission-scope.v1` derives from that 32-byte digest,
then local context tag 0 plus actual owner schema (16 bytes), or tag 1 plus the
actual selected BundleId (32 bytes), then 0 for UNKNOWN upstream or 1 for an
actual authority context. These are public provenance fingerprints, not MACs,
credentials or persisted authorization verdicts. E2EE keyed BLAKE2b and UUIDv5
formats are unchanged.

`observation_scope_v1.json` independently pins scope/context preimages, SHA-256
answers and full-u64 forward-reference values. `local_observation_v1.sql` pins
both tables, their constraints, index and legacy deletion trigger independently.
The old unpublished BLAKE3 experiment is not a supported stored format and is
preserved only as development evidence; no published writer emitted it.

Older SQLite writers ignore these additive tables. Their ordinary pending-header
deletion also fires the additive cleanup trigger. They do not create new
observation evidence. A later capable reader therefore fails observation closed
when original provenance is missing, while retaining the ordinary accepted
versus local/global verdict and body. New writers create metadata lazily, without
rewriting existing rows, claims, policy bundles, Commit bytes or storage epochs.

For content-free delete/restore states, the state image carries its own normal
`$updatedAt`/`$updatedBy` stamp. Pair validation compares every application and
creation-provenance cell to the exact named content image, while allowing those
two state stamps to change. Equal state/content coordinates compare all cells;
a content-bearing state cannot substitute an earlier content image. Ordinary
reads continue to project the state metadata through the existing read machinery.
