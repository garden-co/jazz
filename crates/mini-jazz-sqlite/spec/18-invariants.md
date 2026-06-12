# Invariants To Test

## Appendix D: Invariants To Test

Ongoing work should turn as many of these as practical into integration tests. A
few may remain assertion-level checks or design review items until the relevant
feature exists.

### D.1 Identity Invariants

- Public row ids are stable across replicas.
- Public transaction ids are stable across local-to-global acceptance.
- Public transaction ids are opaque. Protocol peers and application code must
  not derive writer node, local epoch, ordering, or authority state from the id
  string.
- Locally generated public transaction ids are UUIDv7 strings in the Rust
  prototype.
- Physical ids never cross API or sync boundaries.
- Rehydrating the same public id on one replica returns the same physical id.
- Different replicas may assign different physical ids to the same public id.
- Logical row ids are globally unique.
- Public row ids cannot be owned by two different tables. An unresolved ref may
  mention a public row id before target ownership exists, but it does not claim
  ownership.
- Node ids are writer identities, not authorization users.
- One user may write from multiple nodes.
- Public-id/physical-id and branch-id/ordinal mappings are crash-atomic; a
  public identity cannot hydrate to two local physical identities after restart.

### D.2 Transaction Invariants

- One simple write creates one sealed transaction.
- One explicit transaction may contain multiple row mutations and still seals as
  one transaction.
- One upload message contains exactly one sealed transaction; that transaction
  may contain multiple row mutations across multiple tables.
- Ordinary committed local transactions enter the durable upload registry
  atomically with the transaction commit.
- Failure to insert ordinary local transaction upload registry metadata fails
  the local commit.
- An explicit transaction with no staged mutations is a no-op and creates no
  transaction record.
- Multiple staged mutations to the same row in one explicit transaction
  normalize to a single final semantic row mutation before sealing.
- `insert` is create-only; `upsert` is the explicit create-or-update operation.
- A sealed transaction is immutable except for outcome/receipt enrichment.
- Authority acceptance enriches an existing transaction instead of replacing its
  public id.
- Rejection preserves the transaction record and history rows.
- Rejection details live outside the hot transaction row.
- Mergeable transactions may publish optimistically at local tier.
- Pending exclusive transactions are not visible until globally accepted.
- Waiting on an exclusive transaction at local or edge tier is a runtime error.
- Waiting on an exclusive transaction at global tier resolves on acceptance and
  rejects on rejection.
- Local exclusive previews, if implemented, are opt-in unsettled UI state rather
  than ordinary visible current rows.
- Edge-accepted mergeable transactions produce replayable receipt state.
- Edge-accepted mergeable transactions are accepted and visible without a
  global epoch.
- Awaiting-dependency state is durable, not visible in ordinary current reads,
  and clears on successful revalidation of the same public transaction id.
- Later global acceptance enriches the same public transaction id.
- Rejected outcome is terminal for ordinary visibility.
- Stale incoming fate cannot downgrade accepted/global or rejected state.
- Stale incoming fate cannot lower a transaction's global epoch, drop durable
  receipt tiers, erase rejection detail, or resurrect rejected current rows.
- Multiple transactions may share one global epoch.
- Transaction info APIs expose outcome, rejection, receipts, and global epoch
  consistently after sync.
- Duplicate incoming transaction records are idempotent.

### D.3 History And Projection Invariants

- History rows are append-only for application state.
- Ordinary deletes are history versions, not physical history removal.
- Hard delete/truncate are explicit product-visible destructive retention
  operations with deterministic sync semantics.
- Main current projection is rebuildable from history plus transaction
  outcome/receipts.
- Rebuilding a projection twice from the same inputs is byte-for-byte
  deterministic where the physical format is deterministic.
- If current projection and history disagree, rebuild from history wins.
- Rejected history rows do not appear in ordinary reads.
- Projection repair after rejection removes rejected visible state.
- Projection repair after late acceptance can make previously hidden state
  visible.
- Local current projection may include local optimistic mergeable writes.
- Remote pending history cannot displace a durable accepted/global current row.
- Remote pending history may materialize only when no durable row version exists
  for that row and branch.
- Durable/global ordering uses `(global_epoch, tie_breaker)`, not global epoch
  alone.
- Cross-node concurrent same-row pending writes are conflicts unless merge
  strategy resolves them.
- Incidental SQLite row order never decides visible conflict winners.
- Query results have deterministic order even when the user did not provide an
  explicit ordering. The default order must be stable and documented.
- Observable tie-breakers must be semantic, not incidental physical SQLite row
  order.

### D.4 Visibility And Snapshot Invariants

- Current projection reads and historical snapshot reads have distinct semantics.
- Global epoch snapshots include only accepted transactions at or below the
  requested global epoch.
- Rejected and pending transactions are excluded from global epoch snapshots.
- Global epoch snapshot export can produce complete authority state, while
  global epoch snapshot queries are policy-filtered through the query session.
- `as-of time` query/export APIs are expected product surface, but their
  timestamp-to-epoch mapping is not yet specified.
- Full vector snapshots include global base, explicit local bases, and explicit
  dots.
- Full vector snapshots have no excludes in v0.
- Remote local bases are valid only when explicitly named in the snapshot.
- Remote pending history does not imply remote local-base visibility.
- Vector canonicalization does not change visible rows.
- Learning `tx_id -> global_epoch` never changes public transaction identity.
- Global epoch order is authority order, not full causality.
- Causality-sensitive validation uses observed/read facts and write facts.

### D.5 Branch Invariants

- Branches are visibility views over shared history, not copied databases.
- Branch creation creates both backing row and engine branch metadata.
- A branch handle cannot be used when the backing row is not visible under
  policy.
- Branch access checks both backing-row permission and row/version permission
  through the branch view.
- A branch-local transaction may be globally accepted while invisible to main.
- Main visibility does not automatically include branch-local history.
- Branch reads use source precedence, not incidental storage order.
- Branch source reachability is transitive and acyclic.
- Branch source depth is precedence: nearer sources shadow deeper sources,
  while same-depth candidates remain conflicts.
- Ordinary branch writes over unresolved same-depth candidates fail as
  ambiguous; explicit conflict resolution creates a branch-local base row.
- Branch-local writes use the same logical row ids as main by default.
- Branch source/provenance changes are ordinary authorized metadata
  transactions.
- Branch sync includes branch metadata as well as visible row history.
- Branch metadata includes base global epoch and source branch ids; row
  `branch_id` alone is insufficient for branch reproduction.
- Base-only rows needed for branch query results are included in branch sync.
- Branch-local tombstones over pinned-base rows prevent base rows from
  reappearing in the branch view.
- Rejected branch overlays fall back to the pinned base when a base candidate
  exists.
- Pinned branch reads use branch overlay plus base snapshot, not latest main.
- Pinned branch write-policy validation uses branch overlay plus base snapshot
  for referenced policy rows.
- Pinned branch policy read-set recording records base-snapshot dependencies
  when no branch overlay exists.
- Edge validation of untrusted branch writes reproduces the same pinned-base
  policy decision from synced branch/base history.
- Branch query-scope repair is scoped by branch id.
- Stale branch-source metadata replay cannot override newer source-list state.
- A branch delete of a pinned-base row exports a branch tombstone sufficient to
  repair peer recursive reads.

### D.6 Query And Observed-Fact Invariants

- One-shot queries and subscriptions share query semantics.
- Query results are deterministic even without explicit user ordering.
- Query results and pagination are independent of storage-local identifiers
  such as row numbers, transaction row numbers, insertion order, or SQLite
  allocation order.
- Ordering by a ref field uses the public referenced row id, not the local
  numeric row id used to store the ref.
- Queries return semantic rows and observed facts.
- Required includes filter out the parent when missing or unauthorized.
- Optional includes preserve the parent and return null/absent when missing or
  unauthorized.
- Required includes do not filter a parent solely because a nullable scalar ref
  is null or a reverse relation result is empty.
- Required includes do filter a parent when a non-null scalar target or required
  forward-array member is missing or unauthorized.
- Required-include filtering happens before pagination.
- Optional missing includes produce absence facts.
- Selection always preserves public row `id`; selected root fields may coexist
  with includes and selected semantic system fields.
- Hop/gather relation traversals are first-class query IR and subscription
  dependencies.
- Policy dependencies are observed facts distinct from result dependencies.
- Rows needed only for policy do not automatically appear in semantic results.
- Predicate, range, absence, page-boundary, branch/source, and catalogue facts
  are represented when needed for correctness.
- Observed facts can carry multiple reasons for the same concrete row.
- Bundle locators dedupe concrete rows/transactions even when facts repeat.
- Normalized predicates/ranges compare deterministically for supported planner
  forms.
- Query-scope refresh repairs rows that leave a predicate through an update.
- Query-scope refresh repairs rows that leave a predicate through a delete by
  sending tombstone history.
- Stale query refreshes cannot regress a row after a newer refresh for the same
  public row id has already applied.
- Refreshing one active descriptor does not eagerly evict unrelated cached rows
  learned through other descriptors.
- Query-scope export includes predicate observed facts with table, field, value,
  and branch context for supported predicates.
- Query-scope repair rows may be included even when they are no longer semantic
  result rows.
- Query-scope export dedupes concrete history records included for several
  reasons.
- Recursive query-scope export includes deleted descendant subtrees, not only
  direct deleted children.

### D.7 Sync Invariants

- Sync is query-scoped, not table replication.
- Bundles use public ids.
- Applying the same bundle twice is idempotent.
- Bundle application hydrates public ids before touching hot tables.
- Bundles are not authoritative result snapshots.
- Receivers apply history/outcomes/receipts/facts and rerun queries locally.
- A receiver lacking required catalogue state waits or fails closed.
- Out-of-order history and outcome delivery eventually converges after all
  required facts arrive.
- Duplicate, delayed, and reordered bundles do not create duplicate history.
- Duplicate policy-invalid untrusted applies are idempotent and produce one
  rejected transaction record plus one subscription-visible rejection event per
  subscription baseline.
- Reconnect replays desired subscriptions.
- Reconnect uses replay-window recovery before full scope/frontier fallback.
- Scope contraction removes or invalidates stale rows.
- Scope contraction is represented with enough ordinary history/facts for the
  receiver to rerun locally; bundles are not authoritative result snapshots.
- Scope contraction does not imply eager deletion of unrelated retained cache
  facts.
- Rows that leave scope because of update, delete, policy change, branch source
  change, outcome change, or catalogue/lens change eventually disappear from
  local query results after relevant repair data arrives.
- Client upload sends transaction data, not authoritative history. The receiver
  derives authoritative history/system fields from connection trust, auth,
  policy, branch, and storage state.
- Client upload data cannot forge receipts, global epochs, rejection state,
  catalogue state, or semantic system fields.
- Upload uses one `UploadTx` message per transaction. Protocol batching may
  carry many upload messages, but one message is still one transaction.
- Uploaded row data is row-image-shaped: inserts and updates carry effective
  field values, and deletes carry empty values.
- Mergeable upload transactions may omit reads; exclusive upload transactions
  require the read facts needed for validation.
- Uploads are sent from the durable registry ordered by `(created_at,
sync_seq)`, where `sync_seq` is a local-only monotonic tie-breaker.
- The client sends uploads in registry order but does not wait for one
  transaction's ACK before sending the next transaction.
- Upload transport must preserve message order, or provide an ordered stream
  abstraction below the sync protocol.
- `UploadAck` is connection-local flow control only and never completes the
  durable upload registry entry.
- Upload registry completion is derived from local authoritative transaction
  fate, not from the ACK path.
- `TxStatus` is non-cumulative. `GlobalAccepted` satisfies edge-level retry and
  wait needs, `EdgeAccepted` records edge acceptance, and `Rejected` is
  terminal for upload retry.
- Mergeable upload registry entries complete on edge acceptance, global
  acceptance, or rejection.
- Exclusive upload registry entries complete only on global acceptance or
  rejection; edge-only or downgrade-like statuses are ignored for exclusive
  retry completion.
- Unknown upload transaction statuses are ignored by the client.
- Reconnect clears connection-local in-flight upload state and replays active
  durable upload registry entries, even when no subscription is active.
- Duplicate upload of an already known transaction is idempotent and does not
  rewrite accepted history for the same public transaction id.
- A server that lacks authoritative state needed to validate an uploaded update
  or delete waits/fetches trusted upstream state when possible; missing local
  state is not automatically a rejection.
- Authenticated untrusted uploads use the authenticated connection/session user
  for validation and provenance, not client-supplied author fields.
- Trusted peer uploads may preserve peer-supplied author/provenance only within
  the connection's trusted role.

### D.8 Subscription Invariants

- Subscription first delivery equals the corresponding one-shot query at the
  same tier.
- Subscription updates are semantic row diffs.
- Ordered subscriptions emit deterministic moved diffs for order-only changes.
- Subscription diff ordering is deterministic and follows the same effective
  ordering as the corresponding query result.
- Dependency-only changes can update parent semantic rows.
- Every subscription update is tier-gated.
- Rows may arrive before query settlement without being published.
- Missing sync/catalogue state leaves a query unsettled until timeout or
  irrecoverable failure.
- Rejections that change visible results produce semantic diffs.
- Rejected unawaited writes surface through the global rejection/error callback.
- Ordered-page invalidation considers old and new order keys, not only row ids.
- Targeted unsubscribe removes exactly the named subscription and pending
  download/cursor state for that subscription.
- Targeted unsubscribe does not touch upload registry state and does not rely
  on replaying the remaining subscription set.

### D.9 Policy Invariants

- Policy sees the same session context across local, worker, edge, and global
  evaluation.
- Local-first auth can derive a stable user from a durable local secret.
- Auth refresh may update session state only when user identity is
  preserved.
- Auth mode is available as policy input.
- Non-admin sessions fail closed when policy metadata is missing.
- Trusted-peer policy authority and write attribution are distinct: running
  as a user enforces that user's policies, admin/system work bypasses policy
  with system attribution, and privileged attribution bypasses policy while
  recording the named user as provenance.
- System actor provenance uses a reserved namespace that cannot collide with
  ordinary app user ids.
- Admin sessions bypass row policy but remain auditable sessions.
- Trusted peers may read applied policy-scoped facts without an end-user
  user when acting as infrastructure.
- Read policy affects query results and sync delivery.
- Insert/update/delete policy affects transaction acceptance.
- Delete may fall back to update semantics where explicit delete rules are not
  yet available.
- Policy failures do not reveal whether a hidden row exists to ordinary clients.
- Trusted peer and authority logs may contain more detail than client errors.
- Edge policy may be stale for mergeable transactions only within the accepted
  product tradeoff.
- Untrusted write validation uses authenticated session context, never
  provenance fields.
- When a trusted peer validates a transaction received directly from an
  untrusted connection, it uses that connection's authenticated session.
- When the global authority validates a transaction forwarded by an
  intermediary, it uses forwarded per-transaction authenticated session context;
  missing forwarded auth context rejects or stalls validation rather than
  falling back to provenance fields.
- Exclusive transactions are validated by global authority against
  authority-visible history and the authority's current trusted policy
  catalogue.
- Exclusive stale-read validation uses the writer user's current
  policy-filtered view; rows hidden from that writer do not invalidate row,
  absence, predicate, or range reads.
- Recursive policies over acyclic ref chains are SQL-lowerable.
- Direct and indirect recursive policy cycles are rejected.
- Write policies record transitive policy read facts, not only direct parent
  rows.
- Policy evaluation and policy read-set recording use the same read context.
- Historical snapshot policy evaluates referenced parents at the same snapshot
  epoch recursively, not through current projection.
- Branch-local parent rows override base parents for branch policy checks.
- `write_if_created_by_user` allows self-authored inserts and preserves
  original `created_by` on updates.
- Updates and deletes record the previously visible row as a read dependency.
- Partial updates preserve omitted fields when constructing the proposed row for
  write-policy validation, including omitted refs used by policy checks.
- Ref-retarget updates validate the proposed row against policy dependencies
  reached through the new ref target, and a denied retarget leaves the previous
  visible ref intact.
- A policy-denied local delete records the rejection and repairs current,
  query, and subscription-visible state back to the previously authorized row.
- Multi-row transactions reject atomically when any row mutation fails local
  write-policy validation, while preserving write-set history for the rejected
  transaction.
- Trusted/admin writes may bypass user row policies while preserving explicit
  author/provenance attribution.

### D.10 Catalogue And Lens Invariants

- `permissions.ts` is required even when empty.
- Missing permission bundles do not imply permissive behavior.
- Catalogue publication is admin/core controlled.
- Catalogue sync is a separate lane from ordinary query-scoped row sync.
- Runtime work references a catalogue revision.
- Explicit indexes and merge strategies are part of the schema hash.
- Index-only and merge-strategy-only schema changes derive automatic lens
  compatibility.
- Lenses live in `migrations/`.
- Lenses used by v0 are SQL-lowerable.
- Migration DSLs support column add/drop/rename, table add/drop, and table
  rename with structural validation.
- Permission-only changes do not require structural storage migrations.
- Writes through an old schema view append current-schema history.
- Product-level restore/undelete reuses insert authorization semantics.
- Conflict resolution may require explicit conflict-resolution permission where
  the schema declares it.
- Branch source/provenance edits are governed by branch backing-row
  permissions.

### D.11 Authority Validation Invariants

- Authority validation uses authority-visible history, not optimistic current
  projections polluted by proposals.
- Authority validation uses current authority policy, not stale locally observed
  policy, for security.
- Stale-read comparison is parameterized by the writer user's
  policy-filtered read context so hidden rows do not cause false conflicts or
  leak existence.
- Row reads still observe the same visible version at validation time.
- Absence reads remain absent at validation time.
- Range reads remain valid at validation time.
- Policy dependencies still authorize the operation at validation time.
- Exclusive write conflict items are logical rows.
- Two exclusive writes to different columns of the same row are not
  automatically safe.
- Column masks are auxiliary metadata for merge, UI, invalidation, explanation,
  and semantic diffs.
- Updates and deletes record the previously visible row version as write base.
- Read/write sets replace explicit parent pointers for v0 causality and
  validation.

### D.12 Conflict Invariants

- Current projection exposes a resolved value plus conflict metadata.
- Empty conflict metadata is represented explicitly enough for rebuild.
- Non-empty conflict metadata identifies candidate transactions.
- Conflict candidate retrieval is policy-filtered for user-facing APIs.
- Conflict resolution is an ordinary transaction.
- Conflict resolution records resolved candidate transaction ids and
  clears/updates conflict metadata.
- Automatic deterministic merge may derive current values without appending a
  resolution transaction.
- Explicit resolution transactions carry semantic acknowledgement/choice and
  are not invisible cache entries.
- Mergeable transactions may use per-column merge metadata.
- Merge strategies are deterministic reducers over normalized semantic values.
- Rich text is a blessed built-in merge strategy target.
- Exclusive transactions remain row-granular for correctness.
- Encrypted conflicting values are represented as opaque conflicting blobs, not
  plaintext candidate values.

### D.13 Error And Diagnostic Invariants

- Write promise rejection and global rejection callback use the same error shape
  for the same transaction outcome.
- Promise rejection, waits, transaction-info APIs, and rejection subscriptions
  are derived from durable transaction outcome/rejection records.
- Rejection subscriptions can emit safe detail enrichment for the same public
  transaction id.
- Errors carry stable machine codes plus human-readable messages.
- Transport/quota/upload capacity failures are transport/API errors.
- Semantic database failures are transaction/query errors or rejections.
- Upload envelope/auth failures may close the protocol session; semantically
  invalid upload transactions reject the transaction without closing a healthy
  session.
- Recoverable catalogue/sync gaps are unsettled state before timeout, not
  immediate errors.
- Developer diagnostics can be richer and less stable than public errors.

### D.14 Storage And Lowering Invariants

- Hot paths use local integer surrogates for repeated public ids.
- Hot enum fields use integer discriminants.
- The upload registry stores retry metadata only; transaction fate, receipts,
  rejection detail, row history, current projection, and upload row data remain
  in their normal storage tables.
- Upload registry cleanup deletes only completed registry rows and never deletes
  transaction records, history, receipts, rejection detail, row identity mappings,
  or current projection.
- Upload registry cleanup never deletes active rows, regardless of age.
- Runtime can install and use schemas that are not the todo fixture; fixture
  helpers do not define core semantics.
- Composite-key hot tables use `WITHOUT ROWID` unless benchmarks show a
  regression.
- Generated indexes come from schema/query intent.
- Generated indexes declare confidentiality leakage.
- Physical application row columns use `j_` engine names.
- Pure system tables do not need `j_` prefixes.
- User columns colliding with physical prefixes are escaped by the codec.
- SQL fragments and bind parameters travel together in implementation plans.
- The identity codec is centralized.
- Higher-level bindings expose idiomatic value types while preserving explicit
  validation and round-trip semantics.
- TypeScript bindings convert bytes, timestamps, JSON, enums, and transformed
  columns according to stable JS boundary rules.
- Generated row/result layout follows declared schema order plus requested
  includes and subscription deltas.

### D.15 File/Blob Invariants

- Blob metadata is ordinary policy-controlled relational data.
- Files are product-visible row-modeled assets with conventional file metadata
  and part/chunk tables.
- Blob bytes do not bypass Jazz session or policy checks.
- Blob durability may gate transaction publication at a tier.
- File content is immutable in v0.
- Replacing a file creates a new content version.
- Immutable chunks may be shared by digest across branches.
- File serving re-checks session and policy.
- Deletes or permission changes on owning rows may cascade to blob access.

### D.16 Privacy Invariants

- Server-readable fields may participate in server-side policy, indexes,
  predicates, ordering, sync scope, and validation.
- Client-decrypted fields cannot be used by untrusted servers/edges for
  plaintext filtering, sorting, indexing, or policy.
- Sync facts can leak information and must be policy-aware.
- File content digests are privacy-sensitive.
- Generated indexes require server-readable or explicitly indexable-encrypted
  columns.

### D.17 Harness Invariants

- Multi-runtime tests can run against SQLite only; memory-only nodes use
  in-memory SQLite rather than a fake store.
- Multi-runtime tests can mix in-memory SQLite nodes and durable SQLite-file
  nodes.
- Multi-runtime tests can model several in-memory browser-tab nodes connected
  to one durable worker/tab-broker node.
- Tests can model local, edge, and global roles.
- Tests can delay, duplicate, drop, and reorder messages.
- Tests can simulate asynchronous scheduling by randomly choosing explicit node
  progress, network progress, or disk progress steps while remaining
  deterministic from a seed.
- Tests can inspect public events without relying on physical ids.
- Tests can rebuild projections and compare semantic state.
- Tests can assert query settled vs row-received distinctions.
- Deterministic clocks/epochs make failures reproducible.
- Durable nodes survive close/reopen with transaction records, history,
  projections, observed facts needed for recovery, catalogue state, and sync
  frontier state intact.
- In-memory nodes lose local non-synced state on restart unless that state has
  been synced to a durable peer.
- Browser-like main-thread in-memory nodes can reconcile from a durable
  worker/tab-broker node after restart.
- Durable worker/tab-broker nodes can reconcile with edge/global after
  disconnect.
- Query descriptors are not durable disk state; after tab, worker, edge, or
  upstream restart, downstream active subscriptions replay desired state and
  that interest trickles upstream.
- Main-thread tab nodes mirror active query scopes by default, not the whole
  durable worker cache.
- Multiple tabs connected to one broker converge through the broker without
  sharing in-memory state directly.
- Edge nodes can reconcile with global after disconnect and preserve replayable
  mergeable transaction receipts.
- Global authority restart preserves global epochs, transaction outcomes,
  catalogue publication state, and validation history needed for correctness.
- After crash/reopen, projections are either intact or rebuildable from history
  and transaction outcomes/receipts.
- After disconnect/reconnect, subscriptions replay desired state and republish
  only settled semantic results.
- Message replay after reconnect is idempotent across durable and in-memory
  receivers.
- Crash at any explicit embedded transaction boundary leaves the SQLite database
  in a valid state.
- Crash after local write before upstream sync preserves durable local writes on
  durable nodes and drops them on purely in-memory nodes.
- Crash after receiving history before receiving outcome/receipt leaves queries
  unsettled or correctly pending, not incorrectly visible.
- Crash after outcome/receipt before projection repair repairs or rebuilds
  projection on reopen.
- Policy and lens state survive durable restart through catalogue state, not
  ambient process memory.
- Storage isolation keeps app/environment/namespace/driver state separated.

### D.18 Developer Tooling And Admin Workflow Invariants

- Schema and permissions are validated together.
- Missing explicit permissions fail closed in tooling and runtime.
- Catalogue publication is admin-controlled.
- Permission-only changes do not require structural storage migrations.
- Migration stubs are reviewed artifacts, not invisible runtime guesses.
- Inspector/devtools use admin/service credentials and respect redaction.
- Tooling surfaces generated storage layout, policies, indexes, sync state,
  transactions, query scopes, and branch/history state without making physical
  ids public API.
