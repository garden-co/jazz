# E2EE core hook interface: design note for review

Status: draft for the jazz-core owners, written before any core code changes. It proposes how core provides what the protocol requires; the protocol governs where they differ. A reviewed design is not an implementation qualification: each hook needs its own tests when built. Protocol reference: `crates/jazz/SPEC/E2EE_PROTOCOL.md` (cited as "spec §n"). Paths are relative to `crates/jazz/layers/db/src/` unless they say otherwise. Line numbers are from this branch's tree and are approximate.

Every claim below about core is marked **exists** (present in this tree today) or **to be built**. Nothing E2EE-specific exists in core yet: there is no encryption declaration in the app-file reader, codegen or deploy, no ledger read with accepted positions, no provisional read and no durable transaction fate. Section 4 lists everything to be built.

## 1. Shape of the integration

- **jazz-core stays crypto-free.** jazz-core (`jazz-db`) defines a small _synchronous_ hook trait, `E2eeHooks`, plus the plain data types that cross it. It calls the trait at the write, read and query seams listed below. A new crate, `jazz-e2ee-client`, implements the trait on top of `crates/jazz-e2ee` (formats and primitives) and the protocol state machine. That crate depends on jazz-core; jazz-core never depends on it or on `jazz-e2ee`.
- **The hooks never await.** Everything asynchronous lives in the client runtime _above_ `Db`: key acquisition and key requests (spec §3.3–§3.4), preparing a space root (spec §3.2), private-store I/O, delivery watching and answering. That runtime uses ordinary `Db` reads, subscriptions and transactions. The sync hooks only use state already in memory (held keys, prepared roots, replayed chains). When they lack something they return a typed "needs key" outcome, the caller awaits the runtime, and then it retries the single staging call.
  - Exists: the bindings run each operation to completion (binding_host.rs:9, "The database runs every operation to completion synchronously"; `Binding::insert` at :1076 calls `block_on(self.db.insert(..))` at :1085).
  - Exists: `Db::transaction` (db.rs:3603) is `async` and takes an `AsyncFnOnce(&mut MergeableTx<'_>)` callback, so a staging call inside it can await the runtime and retry; the callback itself is not replayed (spec §5.6).
- **The server never runs the trait.** The server-side work is system-table registration, derived policies at deploy, one new policy node, `InheritsFrom`, and a system-table write guard (H7). It is ordinary deploy and policy code, not behind the client feature. There is no E2EE-specific authority request and no native validator.

```rust
// jazz-db, behind cfg(feature = "e2ee-client"). Lives on the Db thread: Rc, !Send. To be built.
pub trait E2eeHooks {
    fn declarations(&self) -> &E2eeDeclarations;                        // H1
    fn seal_write(&self, w: &mut WriteCells<'_>) -> Result<(), SealError>; // H2
    fn open_rows(&self, site: ReadSite, rows: &mut Vec<CurrentRow>)
        -> Result<OpenOutcome, Error>;                                   // H3
    fn plan_query(&self, q: &Query) -> Result<QueryPlan, Error>;          // H4
    fn seal_stream(&self, t: &StreamTarget<'_>)
        -> Result<Box<dyn StreamSealer>, SealError>;                     // H5
    fn open_stream(&self, t: &StreamTarget<'_>, header: &[u8])
        -> Result<Box<dyn StreamOpener>, Error>;                         // H5
}
pub enum SealError { NeedsKey { space: SpaceAddress }, SpacePending { space: SpaceAddress }, Refused(Error) }
pub struct OpenOutcome { pub unreadable: Vec<Unreadable>, pub missing_epochs: Vec<(SpaceAddress, EpochId)> }
```

The hooks are installed with a new `DbConfig::with_e2ee(Rc<dyn E2eeHooks>)` (to be built), next to the existing `with_id_source` and `with_clock` (options.rs:80, :108). An `Option` that is `None` costs one branch per call site.

## 2. Hooks

### H1. Declarations from app files, at open

- **Today (exists).** Nothing: the app-file reader (`crates/jazz/src/tools/deployment/artifact/app_file.rs`, `read` at :166, `read_tables` at :297) has no `encryption` entry, codegen and `jazz-app` know nothing of encrypted columns, and the loaders `load_client(json) -> Result<Migrations>` and `load_client_current(json) -> Result<(Schema, Migrations)>` (`crates/jazz/src/tools/deployment/artifact.rs:773`, :783) return schema and migrations only. `DeployRequest` (`crates/jazz-server/src/server/deployment.rs:16`) carries `target_schema_hash`, `schemas`, `migrations`, `permissions` and `allow_dropping_history`.
- **To be built.**
  - The reader parses each table's `encryption` entry (spec §1.5; with the conversion fields of [migrations](E2EE_MIGRATIONS.md) §3.2) and the top-level `encryption` settings (`allowAccountReset`, mechanisms). It keeps them outside `Schema`, so outside the schema hash, and records them in every schema version (`history`).
  - It refuses a `scope` that is not a table with an `id`, a `space` that is not `id` (with `scope` the table itself) or a `uuid` column referencing `scope`, an in-place encrypt conversion of a nullable plaintext column (migrations §3.2), and any app table or `jazz.permissions.json` entry with the reserved prefix `__e2ee_` (in `read_tables` and `permissions_file::read`, permissions_file.rs:25). Deploy and codegen share the reader, so both refuse.
  - `load_client_current` (or a sibling) also returns `E2eeDeclarations`: the entries, the settings, the app ID and the environment label. The client runtime builds its hook object from them, and the binding passes it to `DbConfig`.
  - Codegen hides the ciphertext and `__e2ee_eq_*` columns of encrypted tables and exposes the plaintext types.
  - `DeployRequest` gains `encryption` (every version's declarations and the settings).
- **Deploy warnings (spec §1.5).** After compiling permissions, the deploy compares each encrypted table's select policy with its space table's, syntactically, in both directions: it warns when it can see that the space row is readable more broadly than the encrypted table (for example, the table's policy is the space table's policy conjoined with further conditions), when the encrypted table is readable more broadly than its space row (for example, its policy does not involve the space row), when a space table's select policy is `True`, and when a space table's update policy requires an old-to-new transition (which admits no creation or rotation, spec §1.4). It never claims more.
- **Catalogue identities.** Cell contexts need Jazz's stable table and column IDs (spec §4.1). Exists: `Db::table_identity(&str) -> Result<Option<GlobalPhysicalTableId>>` and `column_identity` (db.rs:2209, :2219). `GlobalPhysicalTableId` wraps a `uuid::Uuid` (`crates/jazz/layers/types/src/ids.rs:87`): a version 5 UUID of the authority's namespace (a random UUID minted once by the authority, sync.rs ~6407) and the stored name (names.rs:185). They return "not observed" until the client knows the namespace and the version's names are confirmed, so a first-ever offline open cannot encrypt or decrypt. The runtime should cache confirmed identities in private state; see Risks.
- **Error and fallback.** An invalid declaration is a deploy or load refusal, never a runtime fallback. A client whose declarations do not match the deployed bundle fails at open.
- **Cost.** Paid once at open.

### H2. Write staging: plaintext to ciphertext plus equality tags

- **Call site (exists).** The value must be transformed _before_ `Inner::chunk_large_cells` (db.rs:7213). That function content-hashes and chunks large values, and the same mutated op is what is stored and uploaded. A hook at cell binding would encrypt SQLite but not the wire. Concretely, the hook runs in:
  - `Db::write_op` (db.rs:3454), the private entry for `insert`, `update`, `upsert`, `delete` and `restore`, and for `MergeableTx` (db.rs:4522) and `MergeableTxHandle::commit` (db.rs:9038), which replay through it;
  - staging in `TxRef::insert` (db.rs:10743) and its siblings, for handle transactions (`begin_mergeable` / `begin_serializable`, db.rs:9993/:10001) and `serializable_tx()` (db.rs:11183), at staging time, so that a refusal surfaces on the staging call and not at commit.
- **Data crossing (to be built).**

  ```rust
  pub struct WriteCells<'a> {
      pub table: &'a str, pub row: RowUuid, pub kind: WriteKind,      // Insert|Update|Upsert|Move { target_space }
      pub cells: &'a mut BTreeMap<String, Value>,                      // plaintext in, ciphertext out; includes conversion source cells
      pub stored_space: Option<&'a Value>,                             // current row's space cell (updates, moves)
      pub companion_rows: &'a mut Vec<(String, RowUuid, BTreeMap<String, Value>)>, // internal rows staged in the same transaction
  }
  ```

  - The hook replaces each encrypted column's value with `Value::Bytes(cell record)` and adds `__e2ee_eq_<col>` cells.
  - It refuses a change of the space coordinate (spec §5.1) except for `WriteKind::Move`, which re-encrypts every encrypted column under the target space ([migrations](E2EE_MIGRATIONS.md) §4.4), and refuses partial edits of encrypted columns.
  - During a conversion it writes the target and sets the source to null (plaintext) or the cleared marker (encrypted), in the same `WriteCells` (migrations §3.4); a tag backfill writes only the token.
  - Deletes, restores and updates that touch no encrypted column return `Ok` without a key.
  - For updates and upserts, core supplies `stored_space`, the space cell of the current row, which is plaintext; core already loads that row for updates and upserts.
  - `companion_rows` lets the hook stage internal rows (a space root) into the write's own transaction; core inserts them in the same transaction, as the same session (R2), before the write. This is how creation stays atomic with the write without a separate transaction kind.

- **No key (`NeedsKey`).** Core has not applied anything, so the caller awaits `runtime.acquire(space)` and re-issues the same call. In an open transaction only that staging call is retried; the callback is never replayed (spec §5.6).
- **Space creation (spec §3.2, §5.4).** The client never evaluates write policies (exists: a replica runs no write-policy check, `Inner::checked` at db.rs:8140 returns `None` on a client; the only client-side answer is the asynchronous `JazzClient::permission_advice`, `crates/jazz/src/tools/client.rs:1701`, which asks the authority and is `Unknown` offline). Creation is therefore optimistic and needs no advice:
  - an encrypted write to a space with no known valid root and no prepared root returns `NeedsKey`; the runtime prepares the root (online it first reads the address's roots; it generates `K₀`, builds and signs the root, and persists `K₀` as an `unpublished` held key) and the caller retries;
  - on the retry the hook stages the prepared root as a companion row of the write, and seals the write under `K₀`;
  - the authority's R13 decides; a refusal fails that transaction only. After a refusal the runtime marks the space; the hook then returns `SpacePending` for it, the caller fails the write `space-pending`, and the runtime writes a space request (spec §3.2);
  - inserting a space row stages nothing.
- **Epoch choice.** The deepest held key that is not marked `refused` (spec §5.2). It is in memory, so no I/O happens on the write path.
- **Cost.** A hash-set lookup per write for unencrypted tables. For encrypted columns: one XChaCha20-Poly1305 seal per encrypted cell plus a keyed BLAKE2b per equality column.

### H3. Reads: one-shot, query results, subscription deltas

- **Principle.** Ciphertext stays ciphertext inside core: storage, the SQL compiler, fanout's watch indexes and diffs. Decryption happens once, on each materialised result _before_ it leaves `Inner` toward a reader. A dropped row is then simply absent, and fanout's `diff` (fanout.rs:2411) computes correct indices without remapping.
- **Call sites (exist).**
  - `Inner::run_with_deps` (db.rs:5085), after execution. This covers `Db::all`, `read` and their variants, and `all_serialized_query` (sync.rs:15048), which the bindings use.
  - Fanout's materialisation paths that bypass `run`: `Fanout::group_rows` (fanout.rs:1823), `recheck_rows` (:1495) and `stored_images` (:1422). The hook runs on their output, before `diff` and `deliver` (:2122).
  - `Binding::local_current_row` (binding*host.rs:1410) and transaction reads run in \_strict* mode (spec §4.4).
- **Data crossing.**
  - `ReadSite { strict: bool, table, query_shape }` and the rows.
  - The hook rewrites encrypted cells in place to plaintext and removes `__e2ee_eq_*` cells.
  - In lenient mode it removes unreadable rows and returns them in `OpenOutcome.unreadable` as `{table, id, column, code, space}`. In strict mode the first bad cell is an error.
  - Short values (spec §4.1): an empty cell reads as the declared default and, when the empty value is stored in the row rather than supplied by the lens for a row that predates the column, is also reported `empty-cell` (core marks lens-supplied defaults on the rows it hands over; to be built); the cleared marker is absent for a converting column and `superseded` otherwise. During a conversion it applies "source if present, else target" (migrations §3.3), decrypting an encrypted source with its own context. A cell sealed under another space of the row is decrypted by its epoch's space, returned and reported `stale-space` (migrations §3.9).
- **Missing epochs.** The hook lists a row needing an epoch the device lacks in `missing_epochs` and reports it provisionally. The runtime acquires the key asynchronously (requesting it if needed), at most once per read (spec §4.2), and then settles the report to the protocol's code before it reaches listeners: `key-pending` once per space while the request is open or its outcome unknown, `key-unavailable` per row when the request insert was refused (spec §3.4, §4.3). The hook knows nothing of requests.
  - One-shot reads: `Binding::all` is already async. The binding wrapper awaits the acquisition and re-runs the read once.
  - Subscriptions: after a key arrives, the runtime calls a new `Db::refresh_reads_of(tables)` (to be built), a public wrapper of the private `Fanout::refresh_where` (fanout.rs:1344). Rows that became readable then arrive as ordinary additions.
- **Bounded wait (spec §4.6).** The 10 s wait-for-delivery episode lives in the binding's stream wrapper around the subscription stream. Core keeps diffing against its own baseline while the wrapper withholds, so the wrapper keeps a full copy of the latest result, applying each withheld delta to it, and ends the wait by emitting that copy as one reset event; afterwards it forwards deltas unchanged. On expiry it emits the copy and reports the space `key-pending`. It discards the copy on stop, sign-out and account change. Bindings need a reset event in their subscription stream type if they lack one; core needs nothing new.
- **Limit and offset.** Unreadable rows are dropped after the SQL window (spec §4.3), which is exactly what post-execution filtering gives.
- **Refused here.** The hook refuses relation reads whose nested rows (array subqueries in cells) contain encrypted tables until nested decryption is designed, and splices on encrypted columns.
- **Cost.** This is the main hot-path cost; see Risks. It is mitigated by a per-client decrypted-cell cache. Its key is the hash of the complete authentication input: the cell cipher's full context bytes (`frame[ctx, cellType]`), the identity of the held key used (space root and epoch) and the ciphertext. A hit never bypasses `stale-space` classification or the AAD checks. The cache is cleared on held-key removal (revision change, spec §6.4), on a change of the active schema view, on a change of the mechanism allow-list (spec §0, which closes the owner), and on sign-out or account change.

### H4. Equality-filter rewriting

- **Call site (exists).** `Db::prepare_query_bound` (db.rs:3092), before query validation, and the remote shape a subscription sends upstream (`remote_shape`, sync.rs:8493). The server must receive the _rewritten_ query, because it only ever sees tags.
- **Data crossing.**
  - Input: `&Query`.
  - Output:

    ```rust
    QueryPlan::Plain
    QueryPlan::Rewritten {
        physical: Query, residual: Residual { preds, limit, offset },
        epochs: EpochSetId, incomplete: Option<SpaceAddress>,
    }
    QueryPlan::NeedsKey { space }
    ```

  - `physical` replaces `Eq(col, v)` (or a full enum `match`) with `In(__e2ee_eq_col, [token per held epoch])`, keeps the required plaintext `Eq(space, s)`, and drops `limit`/`offset` (an unwindowed prepared query exists, db.rs ~136).
  - `residual` is applied after H3 decrypts: compare the authenticated plaintext, then window.
  - `epochs` is folded into the query's shape hash, so a new held epoch makes a new prepared query. The runtime re-prepares live subscriptions when its epoch set changes (retry budget of 3, spec §4.5).
  - **Cold queries.** Online, an encrypted predicate on a space for which no epoch is held returns `NeedsKey` instead of an empty `IN`; the binding acquires within the read budget and plans again (spec §4.5).
  - **Incomplete results.** When the known lineage has unheld epochs, the plan sets `incomplete`, and the runtime reports the space once with the protocol's code for its state: `key-pending` while its request is open or the outcome unknown, `key-unavailable` when the request insert was refused (spec §4.5), exactly as for H3.
  - **Conversions (migrations §3.3).** The physical query has one tag predicate per column, `__e2ee_eq_<source> IN sourceTokens OR __e2ee_eq_<target> IN targetTokens`, and the residual applies source-present precedence. Source tokens are computed only for an identity conversion (re-home, re-encrypt without `convert`) or a re-encrypt with a declared `reverse` mapping, and only when the source is tagged; otherwise, and for encrypt and tag backfill, the query is refused with `conversion-pending` while the space has rows with a present source (or an empty token). That count is an ordinary indexed `Db` read, so the hook cannot run it: the runtime runs it before planning (once per query and space, cached until the space's rows change) and passes the result in through the hook's in-memory state.

- **Refusals (never a scan).**
  - Ordering by an encrypted column.
  - Joins, `reachable`, unions, `partial` selects, or aggregates over encrypted columns.
  - Encrypted predicates other than `eq`.
  - `eq` on an encrypted column without an equality index or without a space `eq`.
- **Offline.** Offline no tokens are computed: the physical query is just `Eq(space, s)`, and the residual does the filtering.
- **Cost.** A query walk at prepare time, plus one BLAKE2b per held epoch per predicate.

### H5. Large-value streaming

- **Call sites (exist).**
  - Write side: the byte path before the large-value chunker, reached from `Binding::begin_streaming_mutation` (binding_host.rs:1811) and the streaming upload functions in large_ops.rs.
  - At begin, `seal_stream` validates the target (only non-indexed `bytes` columns, spec §5.5), selects the epoch and returns a `StreamSealer`.
    - Each `push(&[u8])` returns ciphertext bytes, which go into the unchanged chunker.
    - `finish()` emits the final tagged record.
    - The routing record and header (spec §4.1) are written first.
  - Read side: the range read and whole-value materialisation in large_ops.rs / large.rs.
    - `open_stream` decrypts sequentially. Range reads, JSON-pointer reads, deltas, appends and splices on encrypted columns are refused in the first release. This is a scope decision, not a format limit: STREAM records authenticate individually, so a later range read can seek to a record boundary.
    - A whole read decrypts from the start.
- **Content addressing.** Chunks are hashed after encryption, so there is no cross-row deduplication of encrypted values. This is acceptable, and required for confidentiality.
- **No key.** `NeedsKey` is returned at begin, before any source bytes are pulled (spec §5.5).
- **Cost.** Streaming XChaCha20; memory is bounded by the chunk size.

### H6. Sync arrivals of internal-table rows (key triggers)

- **Recommendation: no new core hook.** The runtime opens ordinary local subscriptions on the internal tables, as the client's own identity:
  - `__e2ee_space_deliveries` addressed to its account;
  - `__e2ee_spaces` and `__e2ee_space_successors` of the spaces it holds keys for (the chain, and its own account envelopes), and of spaces it is waiting for (`space-pending`, and its own space requests);
  - **unanswered** `__e2ee_key_requests` of the spaces it holds keys for (requests with no delivery of the same ID; deliveries are readable by readers, spec R12), and unanswered space requests (no `keyEpochId`) of spaces with no valid root, so that an updater's client creates the space (spec §3.2);
  - its own account's requests, to join one instead of writing another (spec §3.4);
  - `__e2ee_account_keys` and the public account tables of requesters, read when a request is answered (spec §2.12).
- "Unanswered" is an anti-join of requests and deliveries on the row ID, indexed by `(scopeId, identifier, keyEpochId)`. If the query layer cannot express it as one indexed query, the runtime subscribes to both tables per space and computes the difference itself; either way its cost is proportional to the space's requests, and the runtime stops watching a request once it is answered.
- A new delivery or version triggers acquisition with backoff (spec §4.2). This needs only that the internal tables are in the client's schema (H7) and readable under their derived policies.
- **Alternative.** A raw arrival callback in `Db::notify` (db.rs:3380). It is cheaper, but it puts protocol awareness into core and bypasses policy-filtered visibility.

### H7. System tables, derived policies and `InheritsFrom`

- **Today (exists).** There is no catalogue table with Jazz-owned policies. Jazz accounts are not Jazz tables: `jazz_types::account_registry` is pure logic, and the server keeps its state in raw host SQLite tables (`crates/jazz-server/src/server/account_store.rs`). `_jazz_*` tables are skipped by `stored_schema` (db.rs:1201). The E2EE tables must sync, be queried by clients and be policy-checked, so they need a new category, **system tables**: catalogue tables whose definitions and policies come from Jazz, not the app (to be built).
- **Registration (to be built).**
  - A fixed, versioned schema fragment `e2ee_system_tables_v1()` (spec §1.2–§1.3 columns, `uuid` for every ID, account and address column) is defined in jazz-core, with the indexes of spec §1.3: `(scopeId, identifier)` on every space table, `(scopeId, identifier, keyEpochId)` on requests and deliveries, `recipientAccountId` on deliveries, and `accountId` / `deviceId` on the account tables, as `composite_indexes` / `indexed_columns` of the table schema (exists: `TableSchema`, `crates/jazz/layers/model/src/model/public_api/types/schema.rs:439/456`). Every table already has a unique `(id, _branch)` index (values.rs:1749), so the space-row lookup by ID is indexed.
  - **The loaders add the fragment, not the server.** `prepare_deployment` (`crates/jazz/src/tools/deployment.rs:222`) recomputes the hash of every submitted schema and refuses a mismatch, so the server cannot append a fragment to a submitted schema. The client loader (`load_client_current`) and the CLI's deploy loader both append the fragment to every schema version whose declarations include `encryption`, before hashing, so both sides compute one schema and hash. The server's `deploy` (`crates/jazz-server/src/server/deployment.rs:125`) checks that such a schema contains exactly the fragment and refuses it otherwise.
  - The fragment enters the app's schema hash (decided, D3). Held keys do not depend on the schema hash (spec §6.2), so an app schema change never invalidates them.
- **Where policies are derived (to be built).** Today policies arrive at the server already compiled from `jazz.permissions.json` (`DeployRequest.permissions`). In `deploy`, the server refuses any client-supplied `__e2ee_*` permissions and calls a pure `derive_e2ee_policies(&schema, &decls) -> BTreeMap<Table, TablePolicies>` (new, in `crates/jazz/src/tools/deployment/artifact/`), merging the result before staging. The CLI may call the same function to preview, but the server's result is authoritative.
- **System-table write guard (spec R1; to be built).** Exists: policies do not bind every writer. A `system` session compiles every policy to true (compile.rs:2146, :2184), trusted backends and authorities map to the system author (`crates/jazz-server/src/runtime/mod.rs:1394`), and `Who::Local` / `Who::Attributed` writes are unchecked (db.rs:8140). Core therefore refuses updates and deletes of `__e2ee_*` rows in the write path for every writer, before policies, and E2EE inserts are written with `WriteIdentity::Session` (options.rs:302) so that `$createdBy` is the session account (R2).

#### The `InheritsFrom` policy node

- **Need.** Every internal space row names its space row by two columns: `scopeId`, the space table's catalogue identity, and `identifier`, the space row's ID (spec §1.3). This is a polymorphic soft reference: not a reference column, and not to one table. Exists: `Inherits { operation, via_column, max_depth }` (compile.rs:2417) follows only a reference column to one table.
- **Node (to be built).** A new variant of `PolicyExpr` (`crates/jazz/layers/model/src/model/public_api/policy.rs:58`):

  ```rust
  /// Inherit `operation` from the row named by a (table identity, row id)
  /// pair: `table_column` holds a catalogue table identity, `id_column` a
  /// row id of that table. Only the tables in `tables` are candidates.
  InheritsFrom { operation: Operation, table_column: String, id_column: String, tables: Vec<String> },
  ```

- **Semantics.** For the row under check, it holds iff for some `T` in `tables`: `table_column` equals `T`'s catalogue identity, and a non-deleted row of `T` whose `id` equals `id_column`, as the checking transaction leaves it, passes `T`'s policy for `operation`:
  - `Select`: `T`'s select policy, exactly as a direct read of that row (spec read(row)).
  - `Update`: **both** clauses of `T`'s update policy, against the same row image as old and new row: `USING` **and** (`WITH CHECK`, or `USING` when there is none), the no-op save of spec update(row). Exists: `OperationPolicy { using, with_check }` (`crates/jazz/layers/model/src/model/public_api/types/policy.rs:14`), and an ordinary update checks the new row with `with_check.or(using)` (db.rs ~6566). The node must **not** use `Table::policy(Update)` (values.rs:1248), which returns `using.or(with_check)`, a single clause, and which the existing `Inherits` path uses (compile.rs ~2437).
  - A row naming any other table, or a missing or deleted row, passes nothing.
- **Compilation (compile.rs, beside `Inherits`).** An `OR` over `tables`, each disjunct `alias.table_column = <identity of T> AND semi_join(T AS p, p.id = alias.id_column, not deleted, clause(s) of T as above)`.
  - **Identities are resolved on the authority, when the policy is compiled for evaluation.** The bundle stores table names (`tables`), which are stored names within the bundle's schema; the compiler resolves each to its `GlobalPhysicalTableId` from the authority's namespace and the stored name (the function behind `table_identity`), which the authority always has. Exists: the client-side permission compiler (`expr::compile`, `permissions_file`) has no namespace, which is why resolution is not done there. A rename lens keeps the stored name, so the identity is stable.
  - **Depth.** `InheritsFrom` is not a hop of the application's inheritance chain: it compiles `T`'s policy with the **full** depth bound a direct read of `T` uses (`DEFAULT_INHERITS_DEPTH = 10`, compile.rs:29), not depth − 1, so read(row) is exactly a direct read. This cannot recurse: `InheritsFrom` appears only in system-table policies, and the deploy refuses it in application policies and refuses a candidate that is a system table.
  - **Post-state.** For inserts, the node is evaluated against the transaction's end state. Exists: `finish_checks` (db.rs:6432) grounds a unit's writes in rounds and `post_state_pass` (db.rs:6591) re-checks writes against the unit's whole end state, so a space row inserted or updated in the same transaction as the root is checked as written.
- **Every place that matches on `PolicyExpr` needs an arm (to be built).** Exhaustive matches (the compiler forces an arm): `walk` (fanout.rs:2666) and `policy_expr` (compile.rs:2221). Matches with a catch-all, which would otherwise silently misclassify the node and must get an explicit arm:
  - unsafe defaults: `policy_pins` (fanout.rs:2603, `_ => true`: must be false, the node reads another row), `policy_reads_other_rows` (db.rs:4474, `_ => false`: must be true), `policy_is_monotone` (db.rs:8715, `_ => true`: must be false, the space row's policy can change);
  - others: `row_local_policy` (db.rs:237), `conjuncts`, `value_pin`, `row_pin` (fanout.rs:2645, :2847, :2862, :2920), `correlation`, `clauses`, `hops` (sync.rs:3814, :3827, :3876), `recursive_base`, `inherits` (compile.rs:4367, :4410), the printer, `outer_row_refusal` and `too_deep` in `crates/jazz/src/tools/deployment/artifact/expr.rs` (:2736, :2765, :3035, :3079), `policy_json` (permissions_file.rs:403), and `crates/jazz-server/src/server/permissions_validation.rs:46`;
  - the core-schema lowering in `crates/jazz/layers/model/src/model/public_schema_convert.rs`: `convert_policy_predicate` (:3095, whose catch-all at :3226 refuses `Exists` today) and `append_policy_clause` (:1191, catch-all at :1331), plus the other catch-alls in that file (:180, :227, :1187, :1363, :1542, :1626, :1759). Both lowerings gain arms for `InheritsFrom` and for positive `Exists`, which the delivery policy and several account-table policies use. No policy needs `Not(Exists)`.
- **Candidates are fixed at deploy.** `derive_e2ee_policies` writes `tables` as the sorted, distinct set of `scope` tables of the declarations. The deploy refuses an empty list, an unknown table, and an `__e2ee_` table. No client chooses the candidates.
- **Derived policies** (spec R12–R16; `IF(t)` is `InheritsFrom { operation: t, table_column: "scopeId", id_column: "identifier", tables }`). Updates and deletes are `False` on every internal table and refused by the write guard (R1); author columns compare to the session (R2). All compared columns are `uuid`, and `session.user` is a `Value::Uuid` (compile.rs:89), so the comparisons match without coercion (exists: a text column compared with `session.user` would never match, since only claims are coerced).

  | Table                     | Select       | Insert                                                                                                                                                                                            |
  | ------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | `__e2ee_spaces`           | `IF(Select)` | `IF(Select) AND IF(Update)`, `creatorAccountId` = session                                                                                                                                         |
  | `__e2ee_space_successors` | `IF(Select)` | `IF(Select) AND IF(Update)`, `authorAccountId` = session                                                                                                                                          |
  | `__e2ee_key_requests`     | `IF(Select)` | `IF(Select)`, `accountId` = session                                                                                                                                                               |
  | `__e2ee_space_deliveries` | `IF(Select)` | `IF(Select)`, `senderAccountId` = session, `keyEpochId` not null, `EXISTS(__e2ee_key_requests r: r.id = this.id, same scopeId, identifier and keyEpochId, r.accountId = this.recipientAccountId)` |

  Exists: `Exists` correlates with the row under check through outer-row references (`this.col`, compile.rs:2204), and does not compose the target table's own policy. There is no uniqueness check on roots (spec R13, §3.2). Nothing else is checked: no signature, chain, marker or envelope (spec §1.4).

- **Account tables.** Fixed policies, R3–R11 of the spec. They check access only; the server no longer checks a reset identity's ID (now random) or a local-first root's signature (clients do, spec §2.4). The founder, root-kind and `allowAccountReset` conditions of R5–R6 read Jazz's account registry and the deployed configuration; they are checks beside the policy, next to the existing local-first registry, not part of `InheritsFrom`.

#### Encoding, fixture and compatibility

- **What is stored (exists).** Deployed permissions are stored as a bundle: the canonical JSON (sorted keys, no whitespace) of the per-table policies, with a bundle ID over the schema hash and definition (catalogue.rs ~139). `PolicyExpr` is serde-tagged by `type`, and its JSON is pinned by `policy_expr_json_is_pinned` (policy.rs ~482).
- **Name and version.** The node is the `PolicyExpr` variant `"InheritsFrom"` with fields `operation`, `table_column`, `id_column`, `tables` (table names), an additive extension of the bundle definition encoding `jazz-bundle-v1`. The encoding's version does not change: no existing variant changes, and a bundle without the node is byte-identical to today's, with the same ID. It is emitted only into bundles of apps that declare `encryption`. The `Update` operation of this node means both clauses (above); that meaning is part of the variant.
- **Byte-level fixture.** For `IF(Select)` with `tables = ["projects"]`, serde writes
  `{"type":"InheritsFrom","operation":"Select","table_column":"scopeId","id_column":"identifier","tables":["projects"]}`,
  and the bundle stores its canonical form, 116 bytes of UTF-8,
  `{"id_column":"identifier","operation":"Select","table_column":"scopeId","tables":["projects"],"type":"InheritsFrom"}`,
  with SHA-256 `c2aead25ca360dafc60aed57ce264c42ec8ae17e8f52ee70e5a01dc17c05bd4b`. Both strings are to be added to `policy_expr_json_is_pinned`, whose expected unknown-variant message gains `` `InheritsFrom` ``.
- **Compatibility.**
  - Sync wire: unchanged. Catalogue sync carries schemas without permissions, so clients never decode a bundle definition.
  - Row storage: unchanged. The node is policy, not data.
  - Stored permissions: a server binary that predates the node fails to decode a bundle containing it (`unknown variant`), refusing it rather than dropping a policy, so a downgrade fails closed. Such a server could not run an encrypted app anyway. Bundles of apps without `encryption` are unaffected in bytes and IDs.
  - Clients: none; a client never evaluates write policies.

### H8. Transactions the protocol needs

| Protocol need                                                                                                                                                                                    | Exists today                                                                                                                                                                                                                                   | To be built                                                                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Ledger reads at one cut, with accepted positions (spec §0 item 3)                                                                                                                                | Nothing: no API returns accepted positions or a common cut.                                                                                                                                                                                    | A ledger read returning each row's accepted position and the cut `through`.                                                                  |
| Space creation atomic with the first encrypted write (spec §3.2)                                                                                                                                 | Every write is one transaction; `Db::transaction` (db.rs:3603), handle transactions and `serializable_tx` group writes. Insert checks see the transaction's end state (`post_state_pass`, db.rs:6591).                                         | `companion_rows` in the write hook (H2). No separate transaction kind.                                                                       |
| Read-set validation, including phantoms, reported as a conflict (spec §0 item 5): one successor per predecessor, answers sealed to the requester's current account epoch, one bootstrap or reset | Serializable transactions (`serializable_tx`, `begin_serializable`, `commit_serializable_handle` at db.rs:10119). Whether the authority validates their read set, including rows accepted after the cut that match a read, is not established. | Confirm or add authority-side read-set validation for serializable transactions, with phantoms, refused as a conflict, not a policy refusal. |
| Global wait for account operations, rotation and `createSpace`                                                                                                                                   | `Db::wait_for_transaction(tx, tier)` (sync.rs:8927) with `DurabilityTier { None, Local, Global }` (`crates/jazz/layers/model/src/tx.rs:595`); `JazzClient::wait_for_transaction` (`crates/jazz/src/tools/client.rs:2002`).                     | None.                                                                                                                                        |
| Offline replay of the author's own account state (spec §3.7, §5.3)                                                                                                                               | Nothing: no provisional read with own pending rows.                                                                                                                                                                                            | A provisional read `{owner, acceptedThrough, own}`; it may refuse joins, recursion and policy traversals, since replay is client-side.       |
| Durable fate of a creation or request by transaction identity, across restarts (spec §5.4, §5.6)                                                                                                 | In memory only: `rejected_transactions` (db.rs:2110), `take_upstream_rejections` (db.rs:2122), and an internal `origin_fate` (sync.rs:6491) for write-as-origin keys.                                                                          | `Db::transaction_fate(tx) -> Pending \| Accepted(pos) \| Refused(err) \| Unknown`, durable.                                                  |

Not needed any more: a mixed ledger transaction kind, a separate content-key transaction kind, an E2EE-specific wire transaction kind, and making one transaction depend on another's outcome (writes under an unpublished key do not depend on the creation, spec §5.4).

### H9. Host-supplied private storage and randomness

- **Traits** (in `jazz-e2ee-client`; futures are `LocalBoxFuture` because they are polled on the Db thread; to be built):

  ```rust
  pub trait PrivateStore {           // per Dₐ(account) scope: a device document and one held-key document per space (spec §6.1)
      fn load(&self, scope: &str, doc: &str) -> LocalBoxFuture<'_, Result<Option<Vec<u8>>, StoreError>>;
      fn save(&self, scope: &str, doc: &str, bytes: &[u8], expected_revision: u64) -> LocalBoxFuture<'_, Result<(), StoreError>>;
      fn list(&self, scope: &str) -> LocalBoxFuture<'_, Result<Vec<String>, StoreError>>;
      fn delete(&self, scope: &str) -> LocalBoxFuture<'_, Result<(), StoreError>>;     // the whole scope
      fn protect(&self, secret: &[u8]) -> LocalBoxFuture<'_, Result<Vec<u8>, StoreError>>;   // at-rest wrap
      fn unprotect(&self, blob: &[u8]) -> LocalBoxFuture<'_, Result<Zeroizing<Vec<u8>>, StoreError>>;
  }
  ```

  - `protect` / `unprotect` back each platform's at-rest protection:
    - browser: non-extractable WebCrypto AES-KW/GCM key in IndexedDB;
    - Apple: Keychain item;
    - Android: Keystore-wrapped key;
    - otherwise: app-supplied.
  - Unwrapped private keys live in Rust memory (zeroized) only while the session holds them, because sealed-box opening runs in Rust.

- **Randomness (exists).** `jazz_e2ee::random::RandomSource` (`crates/jazz-e2ee/src/random.rs:12`), with `OsRandom` (:26) through getrandom. Recommendation: the default everywhere is `OsRandom`; hosts override it only for deterministic tests and simulation. It is not a per-binding obligation.
- **Per binding (to be built).**
  - **napi** (`crates/jazz-napi`): a JS object with promise-returning methods, called through a thread-safe function. A default Node file store is provided.
  - **wasm** (`crates/jazz-wasm`): no awaited JS→Rust callback exists yet. Add one using `js_sys::Function` → `Promise` → `wasm_bindgen_futures::JsFuture`. The store and keys live in the worker that owns the durable store; see Risks.
  - **jazz-native-relay** (C ABI): function-pointer callbacks with completion handles. jazz-rn's C++ implements them over Keychain/Keystore.
  - **jazz-ffi** (Swift/Kotlin/Go): a foreign trait `PrivateKeyStore`, blocking on its own thread and adapted into the async trait.
  - **Rust apps** (`crates/jazz/src/tools/client.rs`): a config field `private_store: Rc<dyn PrivateStore>` (or a `Send` factory constructed on the client thread).
- **Recovery secrets (spec §2.9).** The recovery key derivation lives in `jazz-e2ee` next to the account sealing key derivation (spec §2.12). Bindings pass the secret in, never store it.
- **Error.** A store failure fails the operation loudly (spec §0). It is never reported as `key-unavailable`. An unknown store format is refused and never overwritten (spec §6.1).

### H10. Feature flag and build split

- **Today (exists).** There is no client/server feature in jazz-core. The role is decided at runtime (`Db::set_authority`, db.rs:2140).
- **Proposal (to be built).**
  - Add a feature `e2ee-client` on `jazz-db`, re-exported by `jazz`. It gates the `E2eeHooks` trait, `DbConfig::with_e2ee` and every call site; with it off, each site compiles to nothing.
  - `jazz-e2ee-client` enables it. Client bindings depend on `jazz-e2ee-client`. jazz-server never does.
  - The deploy-side code (system tables, the write guard, `derive_e2ee_policies`, the `InheritsFrom` node) is **not** gated: the server needs it.
- **Feature unification caveat.** A workspace build that compiles a client binding and the server together unifies `e2ee-client` into the server's jazz-core. That is harmless, since there is no crypto dependency and the hooks are only installed by client openers, but it is not a hard exclusion. Add a runtime guard: `Db::set_authority` refuses a `Db` with hooks installed. Add a CI check that `cargo tree -p jazz-server -e features` lacks `e2ee-client` when built alone.

### H11. Key requests, answers, creation and rotation

- **A client-runtime task, with no new authority request.** It runs above `Db`, like H6:
  - On `NeedsKey`, a missing epoch in a read, or a new version of a held space, the runtime acquires (spec §3.3) and, when that finds nothing, joins a visible open request of its own account or inserts a request with a random ID (spec §3.4). A refused insert is reported `key-unavailable` without inferring a cause; an unknown outcome is kept as possibly open (`key-pending`) and reconciled by transaction identity.
  - It answers the unanswered requests of the spaces it holds keys for, after checking that the requested version belongs to that space's authenticated chain and was created strictly before the request's position (spec §3.4). It replays the requester's public account history, seals to the requester's current account sealing key, and inserts the delivery with the request's ID after a random delay, in a serializable transaction that read the requester's account rows, so that read-set validation refuses an answer sealed to a superseded account epoch. A refusal ends its attempt.
  - Space creation: prepared on `NeedsKey` for a rootless space and staged with the write (H2); `db.encryption.createSpace(row)`; on a visible space request of a rootless space; and in conversion sweeps ([migrations](E2EE_MIGRATIONS.md) §3.5). It learns that it may not create from refusals. Refused and lost creations are repaired as spec §3.2 states.
  - `db.encryption.rotate(space)` acquires the current version if needed and inserts one successor in a serializable transaction (H8). `db.encryption.recoverRotation(space)` is the explicit recovery rotation of spec §3.5.
  - After a device removal of its account, the runtime rotates each space it holds keys for and emits `spaces-need-rotation` for those whose rotation was refused or not possible (spec §2.8).
- **Cost.** Proportional to the unanswered requests of the spaces the session holds keys for, to the space requests it can read, and to the rotations it makes. Every reader requests each new version it needs once; one delivery answers each request.
- **What would need core beyond this.** Nothing for correctness. Proactive delivery (spec §3.4) would need the server to decide whether another account may read a row; it is out of the first release.

## 3. Summary of hooks

| #   | Hook                                                                     | Call site                                                                                | Sync/async                   | Crosses                                               | On failure                                                                | Hot-path cost                           |
| --- | ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------- | ---------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------- | --------------------------------------- |
| H1  | `declarations`                                                           | `load_client_current` → `DbConfig::with_e2ee`; deploy request                            | Sync, at open                | Encryption entries, settings, app ID, env             | Load or deploy refusal                                                    | Once                                    |
| H2  | `seal_write`                                                             | `Db::write_op` before `chunk_large_cells`; `TxRef` staging                               | Sync                         | Table, row, kind, cells, stored space, companion rows | `NeedsKey` → caller awaits runtime and retries; `SpacePending`; `Refused` | Set lookup; µs per encrypted cell       |
| H3  | `open_rows`                                                              | `run_with_deps`; fanout `group_rows` / `recheck_rows` / `stored_images`; strict tx reads | Sync                         | Rows in/out, unreadable reports, missing epochs       | Lenient: drop and report; strict: error                                   | Decrypt per row per refresh (cached)    |
| H4  | `plan_query`                                                             | `prepare_query_bound`, `remote_shape`                                                    | Sync                         | Query → physical query plus residual                  | Refuse unsupported shapes; `conversion-pending`                           | Prepare-time only                       |
| H5  | `seal_stream` / `open_stream`                                            | streaming begin/push/finish; whole-value read                                            | Sync per chunk               | Byte chunks                                           | `NeedsKey` at begin; ranges refused                                       | Streaming AEAD                          |
| H6  | none (internal subscriptions)                                            | runtime above `Db`                                                                       | Async (runtime)              | Delivery, chain and unanswered request rows           | Retry with backoff                                                        | Normal subscription                     |
| H7  | system tables, write guard, `derive_e2ee_policies`, `InheritsFrom`       | loaders, server `deploy`; write path; policy compiler                                    | Deploy-time and policy check | Declarations → `TablePolicies`                        | Deploy refusal; policy refusal                                            | One space-row lookup per internal check |
| H8  | ledger reads, read-set validation, provisional reads, `transaction_fate` | runtime                                                                                  | Async                        | Tx ids, positions, fates                              | Refusal via fate                                                          | Creation, rotation and answers only     |
| H9  | `PrivateStore`, `RandomSource`                                           | bindings → runtime                                                                       | Async (store)                | Documents, wrapped secrets                            | Loud failure                                                              | Off the read/write path once loaded     |
| H10 | `e2ee-client` feature                                                    | Cargo                                                                                    | n/a                          | n/a                                                   | Guard in `set_authority`                                                  | Zero when off                           |
| H11 | none (requests, answers, creation, rotation)                             | runtime above `Db`                                                                       | Async (runtime)              | Request, delivery, root and successor rows            | Wait for an answering reader; retry                                       | One request and one delivery per need   |

## 4. What jazz-core must build

Exists today and used as is: `Db::transaction` with an async callback, mergeable, handle and serializable transactions, `wait_for_transaction` with durability tiers, post-state insert checks, `table_identity` / `column_identity`, `PolicyExpr` with `Exists` and `Inherits`, `OperationPolicy { using, with_check }`, table indexes, `RandomSource` / `OsRandom`, `JazzClient::permission_advice` (not used by E2EE).

To be built:

1. `DbConfig::with_e2ee` and the `E2eeHooks` trait with its call sites (H2–H5), including `companion_rows` staged in the write's own transaction (H2).
2. The current row's space cell passed to the write hook for updates and upserts.
3. `Db::refresh_reads_of(tables)`: a public wrapper of `Fanout::refresh_where`.
4. The rewritten-query path through `prepare_query_bound` and `remote_shape`, with the epoch set folded into the shape hash.
5. Encryption declarations in the app-file reader, codegen and loaders (none exist today), `DeployRequest.encryption`, and the declaration validation of H1.
6. A system-table category: a reserved prefix, a Jazz-defined schema fragment with its indexes, appended by the loaders on both sides, the server's check of it, Jazz-defined policies that apps cannot override, and a write guard refusing updates and deletes of system rows for every writer (H7).
7. `PolicyExpr::InheritsFrom` with its compilation (both update clauses, full depth, identities resolved on the authority), an explicit arm in every walker listed in H7, `InheritsFrom` and positive `Exists` arms in the core-schema lowering, pinned JSON and the compatibility note (H7).
8. Ledger reads with accepted positions and a common cut, provisional reads, authority-side read-set validation with phantoms for serializable transactions, and a durable `Db::transaction_fate(tx)` (H8).
9. An awaited JS→Rust callback facility in jazz-wasm.
10. Encryption declarations recorded in every schema version (`history` in `jazz.app.json`), returned by the loaders and carried in the deploy request, and a deploy check that refuses a migration changing an encrypted column's plaintext type, scope or space without a conversion ([migrations](E2EE_MIGRATIONS.md) §3.1).
11. A client-visible API for the authority's active schema version, to gate conversions ([migrations](E2EE_MIGRATIONS.md) §3.7).
12. A reset event in each binding's subscription stream, for the bounded-wait wrapper (H3).
13. Marking lens-supplied defaults on rows handed to the read hook, so a stored empty cell can be reported (H3).
14. If the query layer lacks it, an indexed anti-join for "requests without a delivery" (H6).

## 5. Main risks

- **Key versions accumulate with rotations.** Every rotation adds a version. Reads look up the held key per cell version, H4 computes one equality token per held version per predicate, and every reader requests each version it needs, with one delivery each. Measure reads, equality queries and request traffic in a space with many rotations before landing.
- **Hot-path decryption.** Every subscription refresh re-materialises and re-decrypts its rows; fanout re-runs whole groups on invalidation. A 10,000-row live query in a busy project could decrypt 10,000 cells per change.
  - Mitigations: the decrypted-cell cache, and decrypting only the selected columns.
  - Benchmark before landing. The cache holds plaintext in memory; it is cleared on held-key removal, allow-list change and sign-out.
- **Browser thread model.**
  - Durable data lives in the worker, while tabs have in-memory dbs. If decryption happens in the worker, plaintext crosses `postMessage` to tabs; this is acceptable because it stays same-origin, and only one key store is needed.
  - If tabs decrypt, each tab needs the private store, and held-key revisions must be coordinated across tabs (spec §6.4 "last tab out").
  - Decided: keys and decryption in the worker (D5).
- **Blocking versus async key acquisition.**
  - Keeping the hooks sync and returning `NeedsKey` keeps core simple. The costs: every binding entry point must loop on `NeedsKey`; a binding that exposes a synchronous transaction callback refuses a staging call that needs an unheld key or an unprepared space root with `key-pending`, and the application acquires first (spec §5.6). Core's own `Db::transaction` is async and does not have this limit.
  - An acquisition also performs reads through the same `Db`, so it must never run re-entrantly inside a hook. The runtime must never call `Db` from inside a hook.
- **Catalogue identity before confirmation.** A first offline open cannot build cell contexts.
- **Policy cost on the server.** Every read and insert of an internal space row evaluates the space row's own read (and for roots and successors, both update clauses) through `InheritsFrom`, and fanout evaluates it for sync visibility. The cost is that of the space table's policy, plus one indexed lookup; measure it with the app's real space-row policies.
- **Refused creations cost a scan of the space.** Repairing a refused or lost creation reads the space's rows to find cells under the refused epoch (spec §3.2). It is proportional to the space, happens only after a refusal, and is bounded by the rows the device may update.
- **Unreadable rows and pagination.** Short pages are accepted by the spec, but UI code that treats a short page as the end of the results will misbehave.
- **Feature unification.** See H10.

## 6. Decisions

- **D1. Access model (decided).** Entitlement to a space's keys is Jazz read permission on the space row; creating and rotating need read and update permission (update meaning both update clauses on the current row, as a no-op save). Keys move only by request, answered by any reader holding them. Nothing about keys or membership is declared in the app files.
- **D2. Where decryption happens (decided: the recommendation).** Inside core at result materialisation (H3), so pagination, fanout diffs and every binding behave alike. Alternative: above core, in each binding, with the delta indices patched separately.
- **D3. Whether the E2EE system schema enters the app's schema hash and bundle (decided: the recommendation).** Included deterministically, appended by the loaders only when encryption is declared. One bundle identity then covers what clients and the server enforce. Alternative: a separate system namespace outside the hash, which needs a second catalogue track.
- **D4. Reaching the space row from internal rows (decided).** One focused policy node, `InheritsFrom` (table identity plus row ID columns), over candidate tables fixed at deploy (H7), and one set of internal tables for every space table. Not taken: one set of internal tables per space table, and a native server validator.
- **D5. Browser worker model (decided: the recommendation).** Keys and decryption in the worker that owns the durable store, with tabs receiving plaintext. Alternative: per-tab decryption with a shared IndexedDB key store.
- **D6. Key triggers (decided: the recommendation).** Internal subscriptions run by the client runtime (H6). Alternative: a raw arrival callback in `Db::notify`.
- **D7. No-key writes (decided: the recommendation).** `NeedsKey` plus a retry by the caller. Alternative: async hooks inside core, which would break the run-to-completion model.
- **D8. Randomness (decided: the recommendation).** `OsRandom` by default everywhere, with host injection only for tests.
- **D9. Space creation (decided).** Optimistic, in the same transaction as the first encrypted write that needs the space or by explicit `createSpace`; no creation on space-row insert; no local permission evaluation; no server-side root uniqueness.
