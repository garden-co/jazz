# E2EE core hook interface: design note for review

Status: draft for the jazz-core owners, written before any core code changes. It proposes how core provides what the protocol requires; the protocol governs where they differ. A reviewed design is not an implementation qualification: each hook needs its own tests when built. Protocol reference: `crates/jazz/SPEC/E2EE_PROTOCOL.md` (cited as "spec §n"). Paths are relative to `crates/jazz/layers/db/src/` unless they say otherwise. Line numbers are from the current tree and are approximate.

## 1. Shape of the integration

- **jazz-core stays crypto-free.** jazz-core (`jazz-db`) defines a small _synchronous_ hook trait, `E2eeHooks`, plus the plain data types that cross it. It calls the trait at the write, read and query seams listed below. A new crate, `jazz-e2ee-client`, implements the trait on top of `crates/jazz-e2ee` (formats and primitives) and the protocol state machine. That crate depends on jazz-core; jazz-core never depends on it or on `jazz-e2ee`.
- **The core never awaits E2EE work.** Everything asynchronous lives in the client runtime _above_ `Db`: key acquisition and key requests (spec §3.3–§3.4), private-store I/O, delivery watching and answering. That runtime uses ordinary `Db` reads, subscriptions and ledger transactions. The sync hooks only use state already in memory (held keys, replayed chains). When they lack a key they return a typed "needs key" outcome, the binding awaits acquisition, and then it retries the single operation.
  - This matches the core's execution model: binding_host.rs:9 says "The database runs every operation to completion synchronously", and `Binding::insert` calls `block_on(self.db.insert(..))` at :1101.
- **The server never runs the trait.** The server-side work is internal-table registration and derived policies at deploy, through one new policy node, `InheritsFrom` (H7). It is ordinary deploy and policy code, not behind the client feature. There is no E2EE-specific authority request and no native validator.

```rust
// jazz-db, behind cfg(feature = "e2ee-client"). Lives on the Db thread: Rc, !Send.
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
pub enum SealError { NeedsKey { space: SpaceAddress }, Refused(Error) }
pub struct OpenOutcome { pub unreadable: Vec<Unreadable>, pub missing_epochs: Vec<(SpaceAddress, EpochId)> }
```

The hooks are installed with `DbConfig::with_e2ee(Rc<dyn E2eeHooks>)`, next to `with_clock` and `with_id_source` (options.rs:80–108). An `Option` that is `None` costs one branch per call site.

## 2. Hooks

### H1. Declarations from app files, at open

- **Today.** `CodegenTable.encryption: Option<Encryption { space, scope, columns, equality }>` already exists (`crates/jazz/src/tools/deployment/artifact.rs:173–205`). It is parsed and validated by `read_encryption` (`artifact/app_file.rs:633`). It lives in `Codegen`, so it is outside `Schema` and outside the schema hash.
  - It never reaches a client at open: `load_client` and `load_client_current` (artifact.rs:808/818) return only the schema and migrations.
  - It never reaches the server either: `deployment_body` and `deployment_request` (artifact.rs:328/342) send schemas, migrations and permissions only.
  - Its only consumer is codegen. `crates/jazz-codegen/src/model.rs:226` hides the ciphertext and `__e2ee_eq_*` columns and marks the table read-only; `jazz-app` refuses writes through `Table::READ_ONLY` (`crates/jazz-app/src/table.rs:198`).
- **Change.** Add `load_client_e2ee(json) -> E2eeDeclarations`, or extend `load_client_current` to return it. It holds the `encryption` entries (spec §1.5), the top-level `encryption` settings (`allowAccountReset`, mechanisms), the app ID and the environment label. The client runtime builds its hook object from these, and the binding passes it to `DbConfig`. Codegen's read-only marking becomes conditional on the client feature.
- **The declaration is already the right shape.** `{ space, scope, columns, equality }` is all the access model needs: who may decrypt, create and rotate is the space table's own read and update policy. Nothing about keys or membership is declared.
- **Validation gap to close.** `read_encryption` never checks that `scope` names an existing table with an `id`, or that `space` is `id` (with `scope` the table itself) or a `uuid` column referencing `scope`. Deploy and codegen share the reader, so both refuse.
- **Deploy warnings (spec §1.5).** After compiling permissions, the deploy compares each encrypted table's select policy with its space table's: it warns when it can see that the space row is readable more broadly (for example, the table's policy is the space table's policy, or `allowedTo.read` of the space row, conjoined with further conditions), and when a space table's select policy is `True`. The comparison is syntactic; it never claims more.
- **Catalogue identities.** Cell contexts need Jazz's stable table and column IDs (spec §4.1). These come from `Db::table_identity` / `column_identity` (db.rs:2209/2219). Those return "NotObserved" until the authority confirms the catalogue, so a first-ever offline open cannot encrypt or decrypt. The runtime should cache confirmed identities in private state; see Risks.
- **Conversions ([migrations](E2EE_MIGRATIONS.md) §3.2).** `E2eeDeclarations` also carries, per column, `from`, `to`, `convert` and `sealedType` (a `columns` value is a type string or an object with these fields), and per entry `backfill`, `rehome` and `sweep`, for every shipped schema version. The reader refuses an in-place encrypt conversion whose plaintext source column is nullable (migrations §3.2).
- **Error and fallback.** An invalid declaration is a deploy or load refusal, never a runtime fallback. A client whose declarations do not match the deployed bundle fails at open.
- **Cost.** Paid once at open.

### H2. Write staging: plaintext to ciphertext plus equality tags

- **Call site.** The value must be transformed _before_ `Inner::chunk_large_cells` (db.rs:7230). That function content-hashes and chunks large values, and the same mutated `op` is what `record_upload` / `Inner::stored_op` (sync.rs:8237, db.rs:4783) upload. A hook at `bind_cell` (db.rs:7008) would encrypt SQLite but not the wire. Concretely, the hook runs in:
  - `Db::write_op` (db.rs:3471), the entry for `insert`, `update`, `upsert`, `delete` and `restore`, and for `MergeableTx` and `MergeableTxHandle::commit` (db.rs:4558, 9055), which replay through it;
  - serializable staging in `TxRef::insert` and its siblings (db.rs:10776), at staging time, so that a refusal surfaces on the staging call and not at commit.
- **Data crossing.**

  ```rust
  pub struct WriteCells<'a> {
      pub table: &'a str, pub row: RowUuid, pub kind: WriteKind,      // Insert|Update|Upsert|Move { target_space }
      pub cells: &'a mut BTreeMap<String, Value>,                      // plaintext in, ciphertext out; includes conversion source cells
      pub stored_space: Option<&'a Value>,                             // current row's space cell (updates, moves)
  }
  ```

  - The hook replaces each encrypted column's value with `Value::Bytes(cell record)` and adds `__e2ee_eq_<col>` cells.
  - It refuses a change of the space coordinate (spec §5.1) except for `WriteKind::Move`, which re-encrypts every encrypted column under the target space ([migrations](E2EE_MIGRATIONS.md) §4.4), and refuses partial edits of encrypted columns.
  - During a conversion it writes the target and sets the source to null (plaintext) or the cleared marker (encrypted), in the same `WriteCells` (migrations §3.4); a tag backfill writes only the token.
  - Deletes, restores and updates that touch no encrypted column return `Ok` without a key.
  - For updates and upserts, core must supply `stored_space`: the space cell of the current row, which is plaintext. Core already loads that row in `Inner::update` and `upsert` (db.rs:8408/8424).

- **No key.** `SealError::NeedsKey { space }`. Core has not applied anything, so the binding awaits `runtime.acquire(space)` and re-issues the same call.
  - In an open transaction only that staging call is retried. The callback is never replayed (spec §5.6).
  - An encrypted write to a space with no root, by a session that may update the space row, is space creation (spec §5.4). The runtime handles it above the hook: it turns the write into the mixed ledger transaction of H8. Any other session gets `space-pending` (spec §3.2).
  - An insert into a space table (a table some `encryption` entry names as `scope`) gets the space root staged in the same transaction when local permission evaluation says the session may update the new row (spec §3.2). The hook reports the insert; the runtime adds the root through the same mixed-transaction path.
  - Callback transactions (spec §5.6): bindings with asynchronous callbacks await acquisition and retry the staging call; the synchronous `Db::transaction(cb)` cannot await, so the staging call fails with `key-unavailable` and the application acquires first (`db.encryption.acquire(space)`).
- **Epoch choice.** The epoch is the deepest held key (spec §5.2). It is in memory, so no I/O happens on the write path.
- **Cost.** A hash-set lookup per write for unencrypted tables. For encrypted columns: one XChaCha20-Poly1305 seal per encrypted cell plus a keyed BLAKE2b per equality column. Both are microseconds for typical cells.

### H3. Reads: one-shot, query results, subscription deltas

- **Principle.** Ciphertext stays ciphertext inside core: storage, the SQL compiler, fanout's watch indexes and diffs. Decryption happens once, on each materialised result _before_ it leaves `Inner` toward a reader. A dropped row is then simply absent, and fanout's `diff` (fanout.rs:2411) computes correct `index`/`previous_index` without remapping.
- **Call sites.**
  - `Inner::run_with_deps` (db.rs:5102), after `self.execute(..)`. This covers `Db::all`, `read`, `all_for_identity` and `all_result_tree`, and `all_serialized_query` (sync.rs:15110), which the bindings use through `Binding::all` (binding_host.rs:1603).
  - Fanout's materialisation paths that bypass `run`: `Fanout::group_rows` (fanout.rs:1823), `recheck_rows` (:1495) and `stored_images` (:1422). The hook runs on their output, before `diff` and `deliver` (:2122).
  - `Binding::local_current_row` (binding*host.rs:1588) and transaction reads (`all_prepared_in`) run in \_strict* mode (spec §4.4).
- **Data crossing.**
  - `ReadSite { strict: bool, table, query_shape }` and the rows.
  - The hook rewrites encrypted cells in place to plaintext and removes `__e2ee_eq_*` cells.
  - In lenient mode it removes unreadable rows and returns them in `OpenOutcome.unreadable` as `{table, id, column, code, space}`. In strict mode the first bad cell is an error.
  - Short values (spec §4.1): an empty cell reads as the declared default and, when the empty value is stored in the row rather than supplied by the lens for a row that predates the column, is also reported `empty-cell` (core marks lens-supplied defaults on the rows it hands over, so the hook can tell them apart); the cleared marker is absent for a converting column and `superseded` otherwise. During a conversion it applies "source if present, else target" (migrations §3.3), decrypting an encrypted source with its own context. A cell sealed under another space of the row is decrypted by its epoch's space, returned and reported `stale-space` (migrations §3.9).
- **Missing epochs.** A row needing an epoch the device lacks is reported (`key-unavailable`) and listed in `missing_epochs`. The runtime acquires the key asynchronously (requesting it if needed), at most once per read (spec §4.2).
  - One-shot reads: `Binding::all` is already an async `HostFuture`. The binding wrapper awaits the acquisition and re-runs the read once.
  - Subscriptions: after a key arrives, the runtime calls a new core API, `Db::refresh_reads_of(tables)`, which wraps the private `Fanout::refresh_where` (fanout.rs:1344). Rows that became readable then arrive as ordinary `added` entries.
- **Bounded wait (spec §4.6).** The 10 s wait-for-delivery episode lives in the binding's stream wrapper around `SubscriptionStream::next_event`. Core keeps diffing against its own baseline while the wrapper withholds, so the wrapper keeps a full copy of the latest result, applying each withheld delta to it, and ends the wait by emitting that copy as one reset event; afterwards it forwards deltas unchanged. On expiry it emits the copy and reports the space `key-pending` (spec §4.6). It discards the copy on stop, sign-out and account change. Bindings need a reset event in their subscription stream type if they lack one; core needs nothing new.
- **Pending keys (spec §4.2–4.3).** The hook still reports the space's rows as `key-unavailable` with their missing epochs; it knows nothing of requests. When the runtime's acquisition finds no key and its key request is open (spec §3.4), the runtime replaces those row reports with one `key-pending` report for the space before they reach listeners; when the request was refused, the row reports stand. No core change.
- **Limit and offset.** Unreadable rows are dropped after the SQL window (spec §4.3), which is exactly what post-`execute` filtering gives.
- **Refused here.** The hook refuses relation reads whose nested rows (array subqueries in cells) contain encrypted tables until nested decryption is designed (developer documentation should say that a list with its children takes two queries). Splices (`ReadOpts::splices`) on encrypted columns are refused too.
- **Cost.** This is the main hot-path cost; see Risks. It is mitigated by a per-client decrypted-cell cache. Its key is the hash of the complete authentication input: the cell cipher's full context bytes (`frame[ctx, cellType]`, so application, scope, space, table, row, column, epoch and sealed type), the identity of the held key used (space root and epoch) and the ciphertext. A changed space coordinate, schema view or sealed type therefore misses, and a hit never bypasses `stale-space` classification or the AAD checks. The cache is cleared on held-key removal (revision change, spec §6.4), on a change of the active schema view, and on sign-out or account change.

### H4. Equality-filter rewriting

- **Call site.** `Db::prepare_query_bound` (db.rs:3109), before `compile_query` validation. Also the remote shape that `open_subscription` sends upstream (`remote_shape`, sync.rs:8525). The server must receive the _rewritten_ query, because it only ever sees tags.
- **Data crossing.**
  - Input: `&Query`.
  - Output:

    ```rust
    QueryPlan::Plain
    QueryPlan::Rewritten {
        physical: Query, residual: Residual { preds, limit, offset },
        epochs: EpochSetId,
    }
    ```

  - `physical` replaces `Eq(col, v)` (or a full enum `match`) with `In(__e2ee_eq_col, [token per held epoch])`, keeps the required plaintext `Eq(space, s)`, and drops `limit`/`offset`; `PreparedQuery::unwindowed` (db.rs:136) already exists.
  - `residual` is applied after H3 decrypts: compare the authenticated plaintext, then window.
  - `epochs` is folded into the `Shape` hash, so a new held epoch makes a new prepared query. The runtime re-prepares live subscriptions when its epoch set changes (retry budget of 3, spec §4.5).
  - **Cold queries.** Online, an encrypted predicate on a space for which no epoch is held returns `QueryPlan::NeedsKey { space }` instead of an empty `IN`; the binding acquires within the read budget and plans again (spec §4.5). When the known lineage has unheld epochs, the plan carries an incompleteness flag and H3's caller reports the space once `key-unavailable`.
  - **Conversions.** For re-encrypt and re-home the token list covers the source's context and the target's. For encrypt and tag backfill, the `conversion-pending` check is an indexed count (source present, or token empty, in the space). It is an ordinary `Db` read, so the hook cannot run it: the runtime runs the count before planning (once per query and space, cached until the space's rows change) and passes the result in through the hook's in-memory state; the hook then refuses with `conversion-pending`.

- **Refusals (never a scan).**
  - Ordering by an encrypted column.
  - Joins, `reachable`, unions, `partial` selects, or aggregates over encrypted columns.
  - Encrypted predicates other than `eq`.
  - `eq` on an encrypted column without an equality index or without a space `eq`.
- **Offline.** Offline no tokens are computed: the physical query is just `Eq(space, s)`, and the residual does the filtering.
- **Cost.** A query walk at prepare time, plus one BLAKE2b per held epoch per predicate.

### H5. Large-value streaming

- **Call sites.**
  - Write side: the byte path before `lv::PushStreamingPreparation::push`. That is `Db::begin/push/finish_streaming_value_upload` (large_ops.rs:1020/1079/1156), reached from `Binding::begin_streaming_mutation` / `StreamingMutation::push` (binding_host.rs:1996/2308), plus `insert_streaming_value*` (large_ops.rs:930–970).
  - At begin, `seal_stream` validates the target (only non-indexed `bytes` columns, spec §5.5), selects the epoch and returns a `StreamSealer`.
    - Each `push(&[u8])` returns ciphertext bytes, which go into the unchanged lv chunker.
    - `finish()` emits the final tagged record.
    - The routing record and header (spec §4.1) are written first.
  - Read side: `Db::read_value_range` (large_ops.rs:711) and the whole-value materialisation (`large.rs:1173 materialize`).
    - `open_stream` decrypts sequentially. Range reads, `read_text_utf16_range`, `read_json_pointer`, `value_delta`, `append_value` and `splice_value` on encrypted columns are refused in the first release. This is a scope decision, not a format limit: STREAM records authenticate individually (the record index is in the nonce), so a later range read can seek to a record boundary and decrypt only the records it needs.
    - A whole read decrypts from the start.
- **Content addressing.** Chunks are hashed after encryption (large.rs `store_chunk`), so there is no cross-row deduplication of encrypted values. This is acceptable, and required for confidentiality.
- **No key.** `NeedsKey` is returned at begin, before any source bytes are pulled (spec §5.5).
- **Cost.** Streaming XChaCha20; memory is bounded by the chunk size.

### H6. Sync arrivals of internal-table rows (key triggers)

- **Recommendation: no new core hook.** The runtime opens ordinary local subscriptions on the internal tables, as the client's own identity:
  - `__e2ee_space_deliveries` addressed to its account (the only deliveries it can read, spec R12);
  - `__e2ee_spaces` and `__e2ee_space_successors` of the spaces it holds keys for (the chain, and its own account envelopes), and of spaces it is waiting for (`space-pending`);
  - `__e2ee_key_requests` of the spaces it holds keys for, to answer them, and its own requests;
  - `__e2ee_account_keys` and the public account tables of requesters, read when a request is answered (spec §2.12).
- A new delivery or version triggers acquisition with backoff (spec §4.2). This needs only that the internal tables are in the client's schema (H7) and readable under their derived policies.
- **Alternative.** A raw arrival callback in `Db::notify` / `Inner::dispatch` (db.rs:3397; fanout.rs:1098 `dispatch(changes: Vec<(String, RowUuid, Vec<u8>)>)`). It is cheaper (no query per arrival), but it puts protocol awareness into core and bypasses policy-filtered visibility.

### H7. Internal tables, derived policies and `InheritsFrom`

- **Today.** There is no catalogue table with server-owned policies.
  - Jazz accounts are not Jazz tables. `jazz_types::account_registry` is pure logic, and the server keeps its state in raw host SQLite tables `_jazz_host_account_identities` / `_links` (`crates/jazz-server/src/server/account_store.rs:20`). They have no policy, no catalogue entry and no sync.
  - `_jazz_*` tables are skipped by `stored_schema` (db.rs:1211).
  - The E2EE tables differ: they must sync, be queried by clients and be policy-checked, so the account registry pattern does not transfer. They need a new category, **system tables**: catalogue tables whose definitions and policies come from Jazz, not the app.
- **Registration.**
  - A fixed, versioned schema fragment `e2ee_system_tables_v1()` (spec §1.2–1.3 columns) is defined in jazz-core (`tools/deployment`). One set of space tables serves every space table of the app.
  - The reserved prefix `__e2ee_` is refused for app tables in `read_tables` (app_file.rs:300), along with any `jazz.permissions.json` entry for it (`permissions_file::read`, :25).
  - At deploy, `prepare_deployment` (`crates/jazz/src/tools/deployment.rs:222`) appends the fragment whenever any `encryption` entry exists. Clients append the same fragment in `load_client_current`, so both sides compute one bundle.
  - The fragment enters the app's schema hash (decided, D3). Held keys do not depend on the schema hash (spec §6.2), so an app schema change never invalidates them.
- **Where policies are compiled.** Today policies arrive at the server already compiled: `DeployRequest.permissions` (`crates/jazz-server/src/server/deployment.rs:16`), compiled client-side by `permissions_file::read` and `expr::compile`.
  - Change: `DeployRequest` gains `encryption: Vec<E2eeDeclaration>` and the top-level settings.
  - In `deploy` (deployment.rs:125), the server refuses any client-supplied `__e2ee_*` permissions and calls a pure `derive_e2ee_policies(&schema, &decls) -> BTreeMap<Table, TablePolicies>` (new, in `tools/deployment/artifact/`). It merges the result before `stage_deployment` / `apply_catalogue` (db.rs:3803).
  - The CLI may call the same function to preview, but the server's result is authoritative.

#### The `InheritsFrom` policy node

- **Need.** Every internal space row names its space row by two columns: `scopeId`, the space table's catalogue identity, and `identifier`, the space row's ID (spec §1.3). This is a polymorphic soft reference: not a reference column, and not to one table. Today's `Inherits` follows only a reference column to one table (compile.rs:2417).
- **Node.** A new variant of `PolicyExpr` (`crates/jazz/layers/model/src/model/public_api/policy.rs`):

  ```rust
  /// Inherit `operation` from the row named by a (table identity, row id)
  /// pair: `table_column` holds a catalogue table identity, `id_column` a
  /// row id of that table. Only the tables in `tables` are candidates.
  InheritsFrom { operation: Operation, table_column: String, id_column: String, tables: Vec<String> },
  ```

- **Semantics.** For the row under check, it holds iff for some `T` in `tables`: `table_column` equals `T`'s catalogue identity, and a non-deleted row of `T` whose `id` equals `id_column` passes `T`'s policy for `operation`. A row naming any other table, or a missing row, passes nothing.
- **Compilation** (compile.rs, beside `Inherits`). An `OR` over `tables`, each disjunct `alias.table_column = '<identity of T>' AND semi_join(T AS p, p.id = alias.id_column, not deleted, policy_T(operation) at depth − 1)`, exactly the inner part of `Inherits`. Identities are resolved when the bundle is compiled, from the catalogue of the bundle's schema, so they survive renames; a table without a confirmed identity is a deploy refusal. `T`'s own policy may use `Inherits` and counts toward the existing depth bound. Fanout's policy walks (fanout.rs:2580, :2744) and sync's (sync.rs:3868) gain the matching arm.
- **Candidates are fixed at deploy.** `derive_e2ee_policies` writes `tables` as the sorted, distinct set of `scope` tables of the declarations. The deploy refuses an empty list, an unknown table, and an `__e2ee_` table. No client chooses the candidates.
- **Derived policies** (spec R12–R16; `read` is `Select`, `update` is `Update`; `IF(t)` is `InheritsFrom { operation: t, table_column: "scopeId", id_column: "identifier", tables }`). Updates and deletes are `False` on every internal table (R1); author columns compare to the session (R2).

  | Table                     | Select                         | Insert                                                                                                                                 |
  | ------------------------- | ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
  | `__e2ee_spaces`           | `IF(Select)`                   | `IF(Update)` on the post-state (the existing post-state check) `AND NOT EXISTS(__e2ee_spaces with the same scopeId and identifier)`    |
  | `__e2ee_space_successors` | `IF(Select)`                   | `IF(Update)`                                                                                                                           |
  | `__e2ee_key_requests`     | `IF(Select)`                   | `IF(Select)`, `accountId` = session                                                                                                    |
  | `__e2ee_space_deliveries` | `recipientAccountId` = session | `IF(Select)` `AND EXISTS(__e2ee_key_requests r: r.id = id, same scopeId, identifier and keyEpochId, r.accountId = recipientAccountId)` |

  `Exists` already correlates with the row under check (compile.rs:2398); the uniqueness and the request match are ordinary `Exists`/`Not`. Nothing else is checked: no signature, chain, derived ID or envelope (spec §1.4).

- **Account tables.** Fixed policies, R3–R11 of the spec. Two of them check more than policy IR expresses today, as in the earlier design: R5 compares a reset identity's ID with its derivation and reads the deployed `allowAccountReset`, and R6 checks a local-first root's signature against the account registry. They are account-registry checks beside the existing local-first registry, not part of `InheritsFrom`.

#### Encoding, fixture and compatibility

- **What is stored.** Deployed permissions are stored as a bundle: the canonical JSON (sorted keys, no whitespace) of the per-table policies in `_jazz_bundles.def`, with bundle ID `SHA-256("jazz-bundle-v1\0" ‖ schema hash ‖ def)` (catalogue.rs:139). `PolicyExpr` is serde-tagged by `type`, and its JSON is pinned by `policy_expr_json_is_pinned` (policy.rs:482).
- **Name and version.** The node is the `PolicyExpr` variant `"InheritsFrom"` with fields `operation`, `table_column`, `id_column`, `tables`, an additive extension of the bundle definition encoding `jazz-bundle-v1`. The encoding's version does not change: no existing variant changes, and a bundle without the node is byte-identical to today's, with the same ID. It is emitted only into bundles of apps that declare `encryption`.
- **Byte-level fixture.** For `IF(Select)` with `tables = ["projects"]`, serde writes
  `{"type":"InheritsFrom","operation":"Select","table_column":"scopeId","id_column":"identifier","tables":["projects"]}`,
  and the bundle stores its canonical form, 116 bytes of UTF-8,
  `{"id_column":"identifier","operation":"Select","table_column":"scopeId","tables":["projects"],"type":"InheritsFrom"}`,
  with SHA-256 `c2aead25ca360dafc60aed57ce264c42ec8ae17e8f52ee70e5a01dc17c05bd4b`. Both strings are added to `policy_expr_json_is_pinned`, whose expected unknown-variant message gains `` `InheritsFrom` ``.
- **Compatibility.**
  - Sync wire: unchanged. Catalogue sync carries schemas without permissions, so clients never decode a bundle definition.
  - Row storage: unchanged. The node is policy, not data.
  - Stored permissions: a server binary that predates the node fails to decode a bundle containing it (`unknown variant`), refusing it rather than dropping a policy, so a downgrade fails closed. Such a server could not run an encrypted app anyway. Bundles of apps without `encryption` are unaffected in bytes and IDs, so mixed-version servers agree on them.
  - Clients: none; the client feature never evaluates server policies.

### H8. Transactions the protocol needs

| Protocol need                                                                                                                                                             | Existing support                                                                                                                                                                                          | Gap                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ledger reads at one cut, with accepted positions (spec §0 item 3)                                                                                                         | `Db::read_ledger_transaction` → `LedgerRead { through, rows, positions }` (ledger_transactions.rs:9, :705)                                                                                                | None.                                                                                                                                                                                                                                                                                               |
| Atomic space creation with the space row or the first encrypted rows (spec §3.2)                                                                                          | `begin_mixed_ledger_transaction` (:142), committed via `commit_serializable_handle` (db.rs:10142). R13 "as the transaction leaves it" uses the post-state check (`explain_post_state_check_plan` exists). | Mixed mode requires `WriteIdentity::Database` (`prepare_ledger_stage` :246). Confirm that this attributes rows to the session's account for R2 (`$createdBy` = session).                                                                                                                            |
| Read-set validation: refuse a commit whose read evidence changed (spec §0 item 5)                                                                                         | Serializable transactions                                                                                                                                                                                 | Confirm that ledger and mixed transactions validate their read set at the authority, including rows matching a read that were accepted after the cut (phantoms), and that the refusal is a conflict, not a policy refusal.                                                                          |
| Global wait for account operations, creation and rotation                                                                                                                 | `wait_for_transaction(tx, Global)` (sync.rs:8985)                                                                                                                                                         | None.                                                                                                                                                                                                                                                                                               |
| Offline space creation, device-active predicate (spec §3.7, §5.3)                                                                                                         | `read_provisional_transaction` → `{owner, accepted_through, own}` (provisional_transactions.rs:30, :71)                                                                                                   | Ledger transactions refuse a non-authority with no upstream (`begin_admitted_ledger` :146), so offline creation must be an ordinary serializable batch plus a provisional read. Provisional reads refuse joins, recursion and policy traversals, which is acceptable because replay is client-side. |
| Writes under an unpublished key depend on the creation: submitted after it and refused with it (spec §5.4)                                                                | Ordered upload of local transactions                                                                                                                                                                      | Confirm that a later transaction can be made to fail when an earlier one it depends on is refused, or add that dependency.                                                                                                                                                                          |
| Clearing the `unpublished` mark (stored with the creation's transaction identity); reconciling unknown outcomes by transaction identity across restarts (spec §5.4, §5.6) | `take_upstream_rejections`, `rejected_transactions` (db.rs:2120/2132); internal `settled(node, tx)` (sync.rs:7003)                                                                                        | Expose a durable `Db::transaction_fate(tx) -> Pending \| Accepted(pos) \| Refused(err) \| Unknown`. Today only an in-memory waiter or the rejection list tells.                                                                                                                                     |

Can be dropped:

- No separate content-key transaction kind.
- No E2EE-specific wire transaction kind. Mixed transactions are already "local only, no new stored or wire kind".

### H9. Host-supplied private storage and randomness

- **Traits** (in `jazz-e2ee-client`; futures are `LocalBoxFuture` because they are polled on the Db thread):

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

- **Randomness.** `jazz_e2ee::random::RandomSource` already exists (random.rs:12), with `OsRandom` through getrandom, using `wasm_js` on the browser target (`crypto.getRandomValues` works in workers).
  - Recommendation: the default everywhere is `OsRandom`. Hosts override it only for deterministic tests and simulation (`jazz-sim`). It is not a per-binding obligation.
- **Per binding.**
  - **napi** (`crates/jazz-napi`, `open_as` lib.rs:1869): a JS object with promise-returning methods, called through `ThreadsafeFunction` (precedent: `tick_scheduler` :931). A default Node file store is provided.
  - **wasm** (`crates/jazz-wasm`): no awaited JS→Rust callback exists yet. Add one using `js_sys::Function` → `Promise` → `wasm_bindgen_futures::JsFuture`; the precedent for a host trait wrapping a JS function is `set_json_schema_validator` (lib.rs:135). The store and keys live in the worker that owns the durable store (`browser::open`, browser.rs:117); see Risks.
  - **jazz-native-relay** (C ABI): function-pointer callbacks with completion handles, like `jazz_native_relay_host_lease_set_foreground_wake_callback` (lib.rs:2977). jazz-rn's C++ implements them over Keychain/Keystore. `account_crypto.rs` already shows getrandom use there.
  - **jazz-ffi** (Swift/Kotlin/Go): `#[uniffi::export(with_foreign)] trait PrivateKeyStore`, mirroring `TokenProvider` (lib.rs:651). It is blocking on its own thread and adapted into the async trait the same way `core::HostTokens` adapts tokens. Go goes through `c_abi.rs`.
  - **Rust apps** (`jazz-app-support`, `JazzClient::connect*`, `crates/jazz/src/tools/client.rs:829–905`): a config field `private_store: Rc<dyn PrivateStore>` (or a `Send` factory constructed on the client thread).
- **Recovery secrets (spec §2.9).** The recovery key derivation (keyed BLAKE2b from a 32-byte recovery secret) lives in `jazz-e2ee` next to the account sealing key derivation (spec §2.12). Bindings pass the secret in, never store it: for local-first accounts it is the account secret the binding already holds; for external accounts it comes from `BrowserPasskeyBackup` (`jazz-tools/passkey-backup`, browser) or a recovery phrase, and the runtime keeps it only for the registration or restore it is used for.
- **Error.** A store failure fails the operation loudly (spec §0). It is never reported as `key-unavailable`. An unknown store format is refused and never overwritten (spec §6.1).

### H10. Feature flag and build split

- There is no client/server feature in jazz-core today. `runtime` is used by both jazz-server and jazz-app (jazz-server/Cargo.toml:20, jazz-app/Cargo.toml:39), and the role is decided at runtime (`OpenAs`, `trusted_backend`, `set_authority`).
- **Proposal.**
  - Add a feature `e2ee-client` on `jazz-db`, re-exported by `jazz`. It gates the `E2eeHooks` trait, `DbConfig::with_e2ee` and every call site; with it off, each site compiles to nothing.
  - `jazz-e2ee-client` enables it. Client bindings (napi, wasm, native-relay, jazz-app-support, jazz-ffi) depend on `jazz-e2ee-client`.
  - jazz-server never does.
  - The deploy-side code (H7 system tables, `derive_e2ee_policies`, the `InheritsFrom` policy node) is **not** gated: the server needs it.
- **Feature unification caveat.** A workspace build that compiles a client binding and the server together unifies `e2ee-client` into the server's jazz-core. That is harmless, since there is no crypto dependency and the hooks are only installed by client openers, but it is not a hard exclusion. Add a runtime guard: `Db::set_authority` (db.rs:2150) refuses a `Db` with hooks installed. Add a CI check that `cargo tree -p jazz-server -e features` lacks `e2ee-client` when built alone.

### H11. Key requests, answers and rotation

- **A client-runtime task, with no new authority request.** It runs above `Db`, like H6:
  - On `NeedsKey`, a missing epoch in a read, or a new version of a held space, the runtime acquires (spec §3.3) and, when that finds nothing, writes the key request for the current version, choosing the attempt from the request rows it can read (spec §3.4). Requests are ordinary inserts; a policy refusal means the account may not read the space row.
  - It answers requests of the spaces it holds keys for: it checks the derived ID, replays the requester's public account history, seals to the requester's current account sealing key and inserts the delivery with the request's ID after a random delay. A refusal ends its attempt.
  - `db.encryption.rotate(space)` acquires the current version if needed and inserts one successor in a ledger transaction (H8).
  - Space creation: with a space row insert (H2), on the first encrypted write by an updater, and in conversion sweeps ([migrations](E2EE_MIGRATIONS.md) §3.5).
- **Cost.** Proportional to the requests of the spaces the session holds keys for and to the rotations it makes. Every reader requests each new version it needs once; one delivery answers each request.
- **What would need core beyond this.** Nothing for correctness. Proactive delivery (spec §3.4) would need the server to decide whether another account may read a row; it is out of the first release.

## 3. Summary of hooks

| #   | Hook                                    | Call site                                                                                | Sync/async                   | Crosses                                         | On failure                                           | Hot-path cost                           |
| --- | --------------------------------------- | ---------------------------------------------------------------------------------------- | ---------------------------- | ----------------------------------------------- | ---------------------------------------------------- | --------------------------------------- |
| H1  | `declarations`                          | `load_client_current` → `DbConfig::with_e2ee`; deploy request                            | Sync, at open                | Encryption entries, settings, app ID, env       | Load or deploy refusal                               | Once                                    |
| H2  | `seal_write`                            | `Db::write_op` before `chunk_large_cells`; `TxRef` staging                               | Sync                         | Table, row, kind, cells, stored space           | `NeedsKey` → binding acquires and retries; `Refused` | Set lookup; µs per encrypted cell       |
| H3  | `open_rows`                             | `run_with_deps`; fanout `group_rows` / `recheck_rows` / `stored_images`; strict tx reads | Sync                         | Rows in/out, unreadable reports, missing epochs | Lenient: drop and report; strict: error              | Decrypt per row per refresh (cached)    |
| H4  | `plan_query`                            | `prepare_query_bound`, `remote_shape`                                                    | Sync                         | Query → physical query plus residual            | Refuse unsupported shapes                            | Prepare-time only                       |
| H5  | `seal_stream` / `open_stream`           | large_ops streaming begin/push/finish; whole-value read                                  | Sync per chunk               | Byte chunks                                     | `NeedsKey` at begin; ranges refused                  | Streaming AEAD                          |
| H6  | none (internal subscriptions)           | runtime above `Db`                                                                       | Async (runtime)              | Delivery, chain and request rows                | Retry with backoff                                   | Normal subscription                     |
| H7  | `derive_e2ee_policies`, `InheritsFrom`  | server `deploy`, `prepare_deployment`; policy compiler                                   | Deploy-time and policy check | Declarations → `TablePolicies`                  | Deploy refusal; policy refusal                       | One space-row lookup per internal check |
| H8  | (existing tx APIs + `transaction_fate`) | runtime                                                                                  | Async                        | Tx ids, fates                                   | Refusal via fate                                     | Creation and rotation only              |
| H9  | `PrivateStore`, `RandomSource`          | bindings → runtime                                                                       | Async (store)                | Documents, wrapped secrets                      | Loud failure                                         | Off the read/write path once loaded     |
| H10 | `e2ee-client` feature                   | Cargo                                                                                    | n/a                          | n/a                                             | Guard in `set_authority`                             | Zero when off                           |
| H11 | none (requests, answers, rotation)      | runtime above `Db`                                                                       | Async (runtime)              | Request, delivery and successor rows            | Wait for an answering reader; retry                  | One request and one delivery per need   |

## 4. What jazz-core must expose that it does not today

1. `DbConfig::with_e2ee` and the `E2eeHooks` trait with its call sites (H2–H5).
2. The current row's space cell passed to the write hook for updates and upserts.
3. `Db::refresh_reads_of(tables)`: a public wrapper of `Fanout::refresh_where`.
4. The rewritten-query path through `prepare_query_bound` and `remote_shape`, with the epoch set folded into `Shape`.
5. `load_client_current` (or a sibling) returning E2EE declarations; `DeployRequest.encryption`.
6. A system-table category: a reserved prefix, a Jazz-defined schema fragment, and Jazz-defined policies that apps cannot override.
7. `PolicyExpr::InheritsFrom` with its compilation, fanout and sync arms, pinned JSON and compatibility note (H7).
8. A durable `Db::transaction_fate(tx)`, and dependent submission of writes under an unpublished key (H8).
9. Confirmation of how mixed-ledger `WriteIdentity::Database` maps to `$createdBy.account`.
10. An awaited JS→Rust callback facility in jazz-wasm.
11. Encryption declarations recorded in every schema version (`history` in `jazz.app.json`), returned by the loaders and carried in the deploy request, and a deploy check that refuses a migration changing an encrypted column's plaintext type, scope or space without a conversion ([migrations](E2EE_MIGRATIONS.md) §3.1).
12. A client-visible API for the authority's active schema version, to gate conversions ([migrations](E2EE_MIGRATIONS.md) §3.7).
13. Read-set validation for ledger and mixed transactions, including phantoms, reported as a conflict (H8, spec §0 item 5).
14. A reset event in each binding's subscription stream, for the bounded-wait wrapper (H3).
15. Marking lens-supplied defaults on rows handed to the read hook, so a stored empty cell can be reported (H3).

## 5. Main risks

- **Key versions accumulate with rotations.** Every rotation adds a version. Reads look up the held key per cell version, H4 computes one equality token per held version per predicate, and every reader requests each version it needs, with one delivery each. Measure reads, equality queries and request traffic in a space with many rotations before landing.
- **Hot-path decryption.** Every subscription refresh re-materialises and re-decrypts its rows; fanout re-runs whole groups on invalidation. A 10,000-row live query in a busy project could decrypt 10,000 cells per change.
  - Mitigations: the decrypted-cell cache keyed by ciphertext hash, and decrypting only the selected columns.
  - Benchmark before landing. The cache holds plaintext in memory; it is cleared on held-key removal and sign-out.
- **Browser thread model.**
  - Durable data lives in the worker (`browser.rs`), while tabs have in-memory dbs. If decryption happens in the worker, plaintext crosses `postMessage` to tabs; this is acceptable because it stays same-origin, and only one key store is needed.
  - If tabs decrypt, each tab needs the private store. IndexedDB and WebCrypto are available to tabs, but held-key revisions must then be coordinated across tabs (spec §6.4 "last tab out").
  - Decided: keys and decryption in the worker (D5).
- **Blocking versus async key acquisition.**
  - Keeping the hooks sync and returning `NeedsKey` keeps core simple. The costs:
    - every binding entry point must loop on `NeedsKey`;
    - callback-style `Db::transaction(cb)` (db.rs:3620), whose closure cannot await, refuses a staging call that needs an unheld key with `key-unavailable`; the application acquires keys first (spec §5.6).
  - An acquisition also performs ledger reads through the same `Db`, so it must never run re-entrantly inside a hook. The runtime must never call `Db` from inside a hook.
- **Catalogue identity before confirmation.** A first offline open cannot build cell contexts.
- **Policy cost on the server.** Every read and insert of an internal space row evaluates the space row's own read or update policy through `InheritsFrom`, and fanout evaluates it for sync visibility (`fanout.rs:2642 conjuncts`, `policy_pins`). The cost is that of the space table's policy, plus one indexed lookup; measure it with the app's real space-row policies.
- **Unreadable rows and pagination.** Short pages are accepted by the spec, but UI code that treats a short page as the end of the results will misbehave.
- **Feature unification.** See H10.

## 6. Decisions

- **D1. Access model (decided).** Entitlement to a space's keys is Jazz read permission on the space row; creating and rotating need update permission. Keys move only by request, answered by any reader holding them. Nothing about keys or membership is declared in the app files.
- **D2. Where decryption happens (decided: the recommendation).**
  - Recommend inside core at result materialisation (H3), so pagination, fanout diffs and every binding behave alike.
  - Alternative: above core, in `binding_host::Binding` and jazz-app. That means no core read hook, but every binding and the delta indices must be patched separately.
- **D3. Whether the E2EE system schema enters the app's schema hash and bundle (decided: the recommendation).**
  - Recommend including it deterministically: it is versioned, and appended only when encryption is declared. One bundle identity then covers what clients and the server enforce.
  - Alternative: a separate system namespace outside the hash. This is cleaner for app migrations, but needs a second catalogue track.
- **D4. Reaching the space row from internal rows (decided).** One focused policy node, `InheritsFrom`, over candidate tables fixed at deploy (H7), and one set of internal tables for every space table. Not taken: one set of internal tables per space table (the catalogue grows with the app, and each set needs its own derived policies), and a native server validator (a second enforcement path policies cannot review).
- **D5. Browser worker model (decided: the recommendation).**
  - Recommend keys and decryption in the worker that owns the durable store, with tabs receiving plaintext.
  - Alternative: per-tab decryption with a shared IndexedDB key store.
- **D6. Key triggers (decided: the recommendation).**
  - Recommend internal subscriptions run by the client runtime (H6).
  - Alternative: a raw arrival callback in `Db::notify`.
- **D7. No-key writes (decided: the recommendation).**
  - Recommend `NeedsKey` plus a retry in the binding.
  - Alternative: async hooks inside core, which would break the run-to-completion model.
- **D8. Randomness (decided: the recommendation).**
  - Recommend `OsRandom` by default everywhere, with host injection only for tests.
  - Alternative: require each binding to supply a `RandomSource`.
