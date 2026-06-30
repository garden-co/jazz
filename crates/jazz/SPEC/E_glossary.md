# jazz — Specification · Appendix E. Glossary

_Non-normative (guidance)._ This appendix is a dependency-ordered terminology
index. Each entry gives a compact gloss and points to the chapter that _owns_
the term; this appendix is never the source of truth for behavior. Code spelling
is authoritative (`DurabilityTier::Global`, not `global`).

**jazz** — the distributed, local-first database specified here. It is built by
lowering onto **groove**, the storage and incremental-view-maintenance engine
beneath it, which has its own specification. jazz is not a second query engine
(ch. 1, ch. 14).

## Identity (ch. 2)

- **`NodeUuid` / `RowUuid` / `AuthorId` / `SchemaVersionId` / `MigrationLensId` /
  `BranchId`** — wire-stable UUID identities.
- **`NodeAlias` / `SchemaVersionAlias`** — node-local `u64` interned identities;
  never on the wire (ch. 14).
- **`AuthorId::SYSTEM`** — the internal author that bypasses all policy (ch. 7).

## Time & order (ch. 2–4)

- **`TxTime`** — packed HLC time: 48-bit ms + 16-bit counter.
- **`TxId`** — `TxTime` + creating `NodeUuid`; the transaction's identity.
- **`GlobalSeq`** — the core-assigned serialization position ("seq").

## Schema (ch. 2, ch. 10)

- **`JazzSchema` / `TableSchema` / `ColumnSchema`** — the logical schema.
- **`MergeStrategy::{Lww, Counter}`** · **`LargeValueKind::{Text, Blob}`**.
- **schema version** — a content-addressed `SchemaVersionId`; **migration lens**
  — bidirectional translation between versions; **catalogue** — the published
  schema, lens, and pointer store; **current write schema** — the moving write
  pointer; **schema-version storage partition** — the physical per-version
  table.

## Transactions (ch. 3)

- **mergeable transaction** (`TxKind::Mergeable`) — an eventually-consistent
  column-LWW write; the high-level facade spelling is _batch_ (ch. 13).
- **exclusive transaction** (`TxKind::Exclusive`) — serializable compare-and-set;
  the facade spelling is _transaction_. **open exclusive transaction** — its
  pre-commit local state.
- **commit unit** — the atomic `CommitUnit { tx, versions }` shipped at commit.
- **fate** (`Fate::{Pending, Accepted, Rejected}`) — an authority's verdict.
- **durability tier** (`DurabilityTier::{None, Local, Edge, Global}`) — how far a
  write has settled. _Fate and durability are separate axes._
- **snapshot** (`Snapshot`) · **read sets** (`RowRead`/`AbsentRead`/`PredicateRead`).

## History & merge (ch. 4)

- **version / parents** — a row version and its DAG edges; **frontier / heads** —
  the undominated versions; **argmax history** — current is the
  argmax-by-`TxId` version.
- **current row** — visible content winner gated by the deletion register; **local
  current** vs **global current** (`HistoryEntry::is_locally/globally_current`).
- **deletion register** (`MergeAspect::Deletion`, `DeletionEvent::{Deleted,
Restored}`) · **global-current overwrite table** — node-local derived current
  state · **merge version** — an upstream-created merge of concurrent heads.

## Reads & queries (ch. 5–6)

- **settled read** vs **local read**; **historical / settled-history read** at a
  `GlobalSeq` (ch. 11).
- **shape** (`ShapeId`) — a validated, schema-stamped query; **binding**
  (`BindingId`) — its parameter assignment; **claim** (`claim(name)`) —
  server-injected identity data (ch. 7).
- **result set** — typed `ResultMemberEntry` membership plus `ProgramFactEntry`
  facts for matched include paths, relation/path edges, and join witnesses;
  real-row members expose `(table, row_uuid, tx_id)` only as their final/public
  row projection; **settled subscription result set** — the subscriber-side
  complete member/fact state and matched include material for one binding.
  Server-side
  `maintained_subscription_views` and the subscriber-side settled result set
  share the entry shape but play different roles.

## Sync & topology (ch. 8–9)

- **`SyncMessage`** — the one wire vocabulary (`CommitUnit`, `FateUpdate`,
  `RegisterShape`, `Subscribe`, `Unsubscribe`, `ViewUpdate`, catalogue + content
  messages).
- **`PeerState` / `PeerRole::{Relay, EdgeClient}`** — link-local sync state and
  role; **relay** (uses `AuthorId::SYSTEM`, no fate), **edge** (terminates a
  client identity; mergeable fate authority), **core** (exclusive authority,
  history-complete), **client**. The sync participant type is `Node`: a local
  `NodeState` engine plus connections and serving. Relay, edge, and core are
  node-level roles, **not** `Db` roles.
- **payload coverage / peer payload inventory** — the sync vocabulary for what
  payload bytes a peer can safely reference instead of resending. Inventory facts
  are deliberately narrow today: **complete-tx payload dedup / complete tx
  payload refs** means transactions whose full version payload has already been
  shipped and may be referenced by `peer_payload_inventory.complete_tx_payloads`.
  Partial mergeable and view-complete exclusive payloads are not represented by
  today's complete-tx payload tier and must not be described as broad "known
  versions". Add row-version or maintained-view-complete coverage facts only if
  partial payload dedup needs them · **deferred edge fate**.

## API & branches (ch. 13, ch. 11)

- **`Db` / `DbIdentity`** — the client-side application facade: no role, always
  a synced client over a `NodeState`. **`NodeState`** (local engine) / **`Node`**
  (sync participant) are the node-level types beneath it.
- **`read` / `one` / `all` / `subscribe`** · **`ReadOpts` / `LocalUpdates` /
  `Propagation`** · **`WriteHandle` / Rust `WatchHandle` / binding
  subscription stream** · **`RowIdSource`**
  (`Production` / `Seeded`).
- **snapshot-base branch** (`BranchRecord`, `BranchState`) · **branch overlay** ·
  **root branch**.

## Open questions

- 🔶 **Flat index.** Keep this dependency-ordered grouping, or add a flat
  alphabetical index for lookup as well?
- 🔶 **Facade spelling.** The high-level facade spells mergeable transactions as
  _batch_ and exclusive transactions as _transaction_; the implementation has not
  yet landed those facade spellings.
- 🔶 **Edge fate authority.** An edge is the mergeable fate authority; the
  implementation path currently uses the core for mergeable fate authority.
