# E2EE core hook interface: design note for review

Status: draft for the jazz-core owners, written before any core code changes. It proposes how core provides what the protocol requires; the protocol governs where they differ. A reviewed design is not an implementation qualification: each hook needs its own tests when built. Protocol reference: `crates/jazz/SPEC/E2EE_PROTOCOL.md` (cited as "spec §n"). Paths are relative to `crates/jazz/layers/db/src/` unless they say otherwise. Line numbers are from the current tree and are approximate.

## 1. Shape of the integration

- **jazz-core stays crypto-free.** jazz-core (`jazz-db`) defines a small _synchronous_ hook trait, `E2eeHooks`, plus the plain data types that cross it. It calls the trait at the write, read and query seams listed below. A new crate, `jazz-e2ee-client`, implements the trait on top of `crates/jazz-e2ee` (formats and primitives) and the protocol state machine. That crate depends on jazz-core; jazz-core never depends on it or on `jazz-e2ee`.
- **The core never awaits E2EE work.** Everything asynchronous lives in the client runtime _above_ `Db`: key acquisition (spec §3.5), private-store I/O, delivery watching and maintenance. That runtime uses ordinary `Db` reads, subscriptions and ledger transactions. The sync hooks only use state already in memory (held keys, replayed lineage). When they lack a key they return a typed "needs key" outcome, the binding awaits acquisition, and then it retries the single operation.
  - This matches the core's execution model: binding_host.rs:9 says "The database runs every operation to completion synchronously", and `Binding::insert` calls `block_on(self.db.insert(..))` at :1101.
- **The server never runs the trait.** The server-side work is different in kind: internal-table registration and derived policies at deploy (H7), plus the authority requests of H11. It is ordinary deploy code, not behind the client feature.

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
- **Change.** Add `load_client_e2ee(json) -> E2eeDeclarations`, or extend `load_client_current` to return it. It holds the `encryption` entries, the `encryptionGroup` tables (spec §1.5), the app ID and the environment label. The client runtime builds its hook object from these, and the binding passes it to `DbConfig`. Codegen's read-only marking becomes conditional on the client feature.
- **Validation gap to close.** `read_encryption` never checks that `scope` names an existing table. It also does not check that the scope table has an `id`, or that every entry naming the same scope agrees on `manageKeys` and `recipients` (below).
- **Catalogue identities.** Cell contexts need Jazz's stable table and column IDs (spec §4.1). These come from `Db::table_identity` / `column_identity` (db.rs:2219/2229). Those return "NotObserved" until the authority confirms the catalogue, so a first-ever offline open cannot encrypt or decrypt. The runtime should cache confirmed identities in private state; see Risks.
- **`manageKeys` (required, spec §1.4 R13 and §1.5).** Both declarations carry the same key, and both require it:

  ```json
  "messages": { "encryption": {
      "space": "roomId", "scope": "rooms",
      "columns": { "body": "string" }, "equality": ["body"],
      "manageKeys": { "via": "workspace", "allowedTo": "update" },
      "recipients": { "via": "members", "account": "accountId", "group": "teamId" } } }
  "teams": { "encryptionGroup": {
      "manageKeys": { "allowedTo": "delete" },
      "recipients": { "via": "members", "account": "accountId", "group": "subteamId" } } }
  ```

  - `via` names a _forward_ relation (or reference column) of the governing row. It is optional; when it is omitted, the operation applies to the governing row itself.
  - `allowedTo` is one of `read`, `insert`, `update` or `delete`.
  - Semantics: the session manages keys iff `allowedTo.<op>(<via>)` holds on the governing row, exactly as the permissions-file atom would.
  - It compiles to the existing `PolicyExpr::Inherits { operation, via_column, max_depth }` (`expr.rs:2046 compile_allowed`; `public_api/policy.rs:131`). That reuses `allowedTo` inheritance; there is no new policy semantics.
  - There is no default. `read_encryption` (and the group reader) refuse an entry without `manageKeys`, and an empty `encryptionGroup`, with a message that names the scope and suggests the common case: "`messages.encryption` needs `manageKeys`, which says who manages the keys of `rooms` spaces. To let whoever may update the room manage them, add `"manageKeys": { "allowedTo": "update" }`." Deploy and codegen share the reader, so both refuse.
  - Every `encryption` entry with the same `scope` must state the same `manageKeys`. The deploy refuses a disagreement.

  Shapes considered and not taken: a reverse relation plus `insert` (needs a hypothetical-row insert check core lacks: `InheritsReferencing`, `compile.rs:2453`, checks existing referencing rows, and `REVERSE_MAX_DEPTH` refuses depth on it); a restricted permissions-language string; and a key on the space table instead of the `encryption` entry.

- **`recipients` (optional, spec §1.5 and §3.8).** `{ "via": <reverse relation of the scope table>, "account"?: <column>, "group"?: <column> }`, at least one of `account`/`group`. The reader checks that `via` is a reverse relation declared on the scope table, that `account` is a `string` or `uuid` column of the relation's source table, that `group` is a reference column of that table whose target declares `encryptionGroup`, and that every entry with the same `scope` states the same `recipients` or none. `encryptionGroup` takes the same key, checked against the group table, with `group` naming child groups. It reaches the client in `E2eeDeclarations` (for the sync, H11) and the server in `DeployRequest.encryption` (for the R24 policy, H7). Omitting it is manual mode.

- **Conversions ([migrations](E2EE_MIGRATIONS.md) §3.2).** `E2eeDeclarations` also carries, per column, `from`, `to`, `convert` and `sealedType` (a `columns` value is a type string or an object with these fields), and per entry `backfill`, `rehome` and `sweep`, for every shipped schema version. The reader refuses an in-place encrypt conversion whose plaintext source column is nullable (migrations §3.2).
- **`manageKeys` that reads session claims.** The reader walks the compiled rule and refuses one that reads `session.claims` unless the declaration has `"claims": "acknowledged"` (spec §1.5); it warns when `allowedTo` is `read` or the rule admits many accounts.
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
  - An encrypted write to a space with no root, by a session that manages it, is space creation (spec §5.4). The runtime handles it above the hook: it turns the write into the mixed ledger transaction of H8. A session that does not manage it gets `space-pending` (spec §3.3).
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
- **Missing epochs.** A row needing an epoch the device lacks is reported (`key-unavailable`) and listed in `missing_epochs`. The runtime fetches the key asynchronously, at most once per read (spec §4.2).
  - One-shot reads: `Binding::all` is already an async `HostFuture`. The binding wrapper awaits the fetch and re-runs the read once.
  - Subscriptions: after the fetch, the runtime calls a new core API, `Db::refresh_reads_of(tables)`, which wraps the private `Fanout::refresh_where` (fanout.rs:1344). Rows that became readable then arrive as ordinary `added` entries.
- **Bounded wait (spec §4.6).** The 10 s wait-for-delivery episode lives in the binding's stream wrapper around `SubscriptionStream::next_event`. Core keeps diffing against its own baseline while the wrapper withholds, so the wrapper keeps a full copy of the latest result, applying each withheld delta to it, and ends the wait by emitting that copy as one reset event; afterwards it forwards deltas unchanged. It discards the copy on stop, sign-out, account change and (for non-declared members) expiry. Bindings need a reset event in their subscription stream type if they lack one; core needs nothing new.
- **Pending keys for declared members (spec §4.2–4.3).** The hook still reports the space's rows as `key-unavailable` with their missing epochs; it knows nothing of membership. When the runtime's fetch finds a declared member with no grant or delivery yet, the runtime replaces those row reports with one `key-pending` report for the space before they reach listeners, and the stream wrapper emits on episode expiry instead of failing. No core change.
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

### H6. Sync arrivals of internal-table rows (key fetch triggers)

- **Recommendation: no new core hook.** The runtime opens ordinary local subscriptions on the internal tables, as the client's own identity:
  - `__e2ee_space_deliveries`, `__e2ee_space_recovery_deliveries`, `__e2ee_group_deliveries` and `__e2ee_group_recovery_deliveries` filtered by `recipientDeviceId = this device` or by its account's recovery roots;
  - `__e2ee_space_group_deliveries` and `__e2ee_group_child_deliveries` addressed to its groups;
  - `__e2ee_space_grants` and `__e2ee_group_membership` naming its account or its groups (for `key-pending`, spec §4.2);
  - `__e2ee_space_successors` and `__e2ee_group_successors` for held spaces and groups, and `__e2ee_group_public_keys` of group recipients it manages (public key chain, spec §3.1);
  - for managers: `__e2ee_rotation_requests` it can read (R23), space and group repairs, and the space roots of its scope tables (with H11's governing-row discovery);
  - for declared members: `__e2ee_spaces` of spaces it is waiting for (`space-pending`);
  - `__e2ee_space_notices` and `__e2ee_group_notices` (reset approvals) of held spaces and groups, verified and surfaced as `account-reset` events (spec §2.11);
  - `__e2ee_key_requests` of held spaces and groups, answered when the requester is not reset-pending (spec §3.4, R27);
  - for managers: public account roots of their direct recipients and members, to list reset-pending accounts as `reset-approval-needed` events (spec §2.11).
- A new delivery's `added` row triggers acquisition with backoff (spec §4.2). This needs only that the internal tables are in the client's schema (H7) and readable under their derived policies.
- **Alternative.** A raw arrival callback in `Db::notify` / `Inner::dispatch` (db.rs:3397; fanout.rs:1098 `dispatch(changes: Vec<(String, RowUuid, Vec<u8>)>)`). It is cheaper (no query per arrival), but it puts protocol awareness into core and bypasses policy-filtered visibility.

### H7. Internal tables and server-side derived policies

- **Today.** There is no catalogue table with server-owned policies.
  - Jazz accounts are not Jazz tables. `jazz_types::account_registry` is pure logic, and the server keeps its state in raw host SQLite tables `_jazz_host_account_identities` / `_links` (`crates/jazz-server/src/server/account_store.rs:20`). They have no policy, no catalogue entry and no sync.
  - `_jazz_*` tables are skipped by `stored_schema` (db.rs:1211).
  - The E2EE tables differ: they must sync, be queried by clients and be policy-checked, so the account registry pattern does not transfer. They need a new category, **system tables**: catalogue tables whose definitions and policies come from Jazz, not the app.
- **Registration.**
  - A fixed, versioned schema fragment `e2ee_system_tables_v1()` (spec §1.2–1.3 columns) is defined in jazz-core (`tools/deployment`).
  - The reserved prefix `__e2ee_` is refused for app tables in `read_tables` (app_file.rs:300), along with any `jazz.permissions.json` entry for it (`permissions_file::read`, :25).
  - At deploy, `prepare_deployment` (`crates/jazz/src/tools/deployment.rs:222`) appends the fragment whenever any `encryption` or `encryptionGroup` entry exists. Clients append the same fragment in `load_client_current`, so both sides compute one bundle.
  - The fragment enters the app's schema hash (decided, D3). Held keys do not depend on the schema hash (spec §6.2), so an app schema change never invalidates them.
- **Where policies are compiled.** Today policies arrive at the server already compiled: `DeployRequest.permissions` (`crates/jazz-server/src/server/deployment.rs:16`), compiled client-side by `permissions_file::read` and `expr::compile`.
  - Change: `DeployRequest` gains `encryption: Vec<E2eeDeclaration>`.
  - In `deploy` (deployment.rs:125), the server refuses any client-supplied `__e2ee_*` permissions and calls a pure `derive_e2ee_policies(&schema, &decls) -> BTreeMap<Table, TablePolicies>` (new, in `tools/deployment/artifact/`). It merges the result before `stage_deployment` / `apply_catalogue` (db.rs:3803).
  - The CLI may call the same function to preview, but the server's result is authoritative.
- **Referencing the governing row.**
  - The space root's governing row is `(scopeId, identifier)`: a polymorphic soft reference, not a ref column, so today's `Inherits` cannot follow it.
  - Proposed core addition: `PolicyExpr::InheritsAs { operation, via_column, target_table, max_depth }`. It treats a text or UUID column as the row id of a named table and otherwise compiles like `Inherits` (`compile.rs:2417`).
  - Derivation emits one disjunct per space table `T`:

    ```
    scopeId = '<T catalogue id>' AND InheritsAs(<manageKeys op>, identifier, T)
    ```

    With a `manageKeys.via`, the target is `T` followed by the `via` hop. There is no default operation: a declaration without `manageKeys` never reaches derivation, because the reader refuses it.

  - Grants, successors and deliveries reach the root through `spaceId`, as `ExistsRel`/semi-join on `__e2ee_spaces` (`compile.rs:2533 exists_rel`).
  - Groups follow the same pattern through `__e2ee_group_bindings`, with the group declaration's `manageKeys`.
  - **Declared group recipients (spec R24).** For a space or group table with `recipients.group`, `__e2ee_group_bindings` gains a read disjunct: a manager of space (or parent group) `S` may read the binding whose `(scopeId, identifier)` is the `group` column of a row of `S`'s membership relation. It is an `ExistsRel` over the application's membership table joined to the governing row and the manager check above; the server derives it from the declaration, and the application writes nothing.
  - **Last manager (spec R25).** One server check in the first release, refusing with `last-manager`: an insert check on `__e2ee_space_grants` and `__e2ee_group_membership` over the whole transaction. The server applies the transaction's grants (membership records) to the current manager state, in position-then-ID order, and refuses if no account is then a manager recipient. Manager state comes from the current-edge projection's role flag (below): for each account, the latest accepted row of role `manager` naming it decides; the creator counts until a `manager` row names it. Two removals in one transaction are therefore checked against the state after both, and a historical `add` followed by a `remove` does not count. Rows are counted whether or not client replay accepts them.
    - Not enforced in the first release: application writes that take `manageKeys` away from every manager recipient (governing row, `via` target, other rule inputs). The SDK refuses leaving or demoting the last manager where it can recognise it. Full post-state enforcement (dependency extraction from compiled `manageKeys` expressions, reverse mapping over old and new row images, a post-state check in the apply path) is later hardening work.
  - **Account reset (spec §2.11, R5).** The insert policy on `__e2ee_account_identities` refuses a generation above 0 from a `local-first` session or when the deployed encryption configuration (`DeployRequest.encryption`, H1) does not set `allowAccountReset`. `allowAccountReset` comes from the top-level `encryption` object of `jazz.app.json` (spec §1.5). Reset approvals are inserted under the space's (group's) management right, refused when the author is the approved account, and read under R11 (R17). **Pending accounts (spec §2.11).** The server keeps, per (space or group, account), the admitted generation: set at the root for its author (the generation of the root's author epoch), otherwise at the first accepted `add` (from the bound account epoch's generation, via a server map of account epoch IDs to generations built from the public roots and successors), raised only by an accepted approval of a later generation. Every governance insert into a space's or group's internal tables (grants, membership records, successors and recipients rows, deliveries incl. self-deliveries and key-request answers, public-key records, rotation requests, notices, approvals, key requests) is refused when the session account's current generation is above its admitted generation there. An approval is refused when its author is the approved account or is itself pending. When a root's author, or a first `add`, is at generation above 0, the transaction must also carry the admission notice; an `admitted` notice is accepted only in such a transaction. Clients do not use the admitted generation to accept or ignore governance records: they apply the pending-author rule in notice form (spec §3.1). A manager's client computes it only to list reset-pending accounts for `pendingResets()` and the `reset-approval-needed` event (spec §2.11).
  - **Derived IDs (spec §0, §3.8).** An insert check on space roots, grants, group membership records, notices, key-request answers and reset identities refuses a version 8 ID that differs from its derivation over the row's own fields; for grants and membership records `n` is the count of accepted rows with the same space or group, role, operation and recipient, read from an index. Rows with other IDs must be version 4 or 7. "Row exists" refusals reach the writer as ordinary policy refusals. Secret-derived recovery root IDs are not recomputed: the server checks only their version 8 form, R9 and uniqueness (spec §0).
  - **Group public keys (spec R16, §3.1).** Inserts are admitted only with the group's root (row ID = group ID, empty `predecessorEpochId` and `proofEpochId`, once per group) or with a group successor of the same row ID, predecessor, proof epoch, epoch and author, and at most one per `(groupId, epochId)`; the policy checks co-insertion, shared ID and uniqueness, not signatures (clients check those). The public key chain decides a group's current epoch for every reader.
  - **Bindings.** `(scopeId, identifier)` of `__e2ee_group_bindings` is unique (a unique index or native validator), so one group per row holds under concurrent creation.
  - **Rotation requests (spec R23).** Insert only with a `remove` membership record (by a manager, or by the member removing itself), with a successor of the source group, or alone as a `coverage` request naming the source's current epoch, in the same transaction. Read by managers of every group and key managers of every space in which the source is currently nested; requests are retained and always returned (the server never retires or hides one on the strength of coverage or public-key records, which only clients authenticate). Paths are computed at read time by a gather over the current-edge projection from the ancestor down to the source (depth 8); nothing is stored per request. A client may pass its cursor so the server returns only later requests; a cursor is invalidated by any new path, and a cold device reads all retained requests once. Cost grows with the requests readable to an ancestor, not bounded by the depth limit; an authenticated summary to bound it is possible later work.
  - **Key requests and delivery to a manager recipient (spec R27).** A current manager recipient may insert a `__e2ee_key_requests` row for its own device; current recipients (members) of the space or group may read it. A disjunct on the space and group delivery insert policies admits a current recipient inserting the one answer of an accepted key request: row ID derived from the request ID (spec §3.4), addressed to the device it names, of the same scope and epoch, whose account is still a current manager recipient and not pending.
  - **Self-removal (spec R26).** A disjunct on the same insert policies admits a self-authored `remove` naming the session's own account without the `manageKeys` check, with a version 4 or 7 ID. In declared mode a `member`-role self-removal is refused while a row of the membership relation still names the account (an indexed lookup on the relation column).

- **Nested groups (R11, R17, R20–R23).**
  - Transitive membership can be expressed today with `exists(from recursive(… maxDepth: 8))`. It lowers to `RelationExpr::Gather`, compiled at `compile.rs:2635`; `RecursionBound::MaxDepth` is at `policy_and_request_contracts.rs:551`.
  - The gap is "current" membership. A member is current when the _latest_ membership record for `(group, member)` is an `add`, and records are immutable add/remove events ordered by accepted position. Policies have no "latest by accepted position" operator.
  - Recommendation: a server-maintained projection `__e2ee_current_edges(parentKind, parentId, childKind, childId, member, manager)`, where `member` and `manager` say whether the latest accepted row of each role is an `add`. It is updated in the same apply as an accepted membership or grant insert, is never synced, and has no app policy. The recursive gather walks it (an edge is current if either flag is set); R25 reads the `manager` flags.
- **Cycle and depth refusal.**
  - Expressing "this add would create a cycle or a chain longer than 8" as a policy is possible but opaque: a recursive gather that bounds the descendants of the child plus the ancestors of the parent.
  - Recommendation: a native server validator on inserts into `__e2ee_group_membership` (`memberKind = group`) and `__e2ee_space_grants` (`recipientKind = group`). It runs over the projection and is registered with the derived policies.
  - Core has no native-check hook today, so this is new (D4).
- **Error.** A violating insert is an ordinary policy refusal and reaches the client through the normal fate path (H8).

### H8. Transactions the protocol needs

| Protocol need                                                                                                                                                             | Existing support                                                                                                                                                                                          | Gap                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ledger reads at one cut, with accepted positions (spec §0 item 3)                                                                                                         | `Db::read_ledger_transaction` → `LedgerRead { through, rows, positions }` (ledger_transactions.rs:9, :705)                                                                                                | None.                                                                                                                                                                                                                                                                                               |
| Atomic space creation with its first rows: root, initial grant, app rows incl. a new scope row (spec §3.3)                                                                | `begin_mixed_ledger_transaction` (:142), committed via `commit_serializable_handle` (db.rs:10142). R13 "as the transaction leaves it" uses the post-state check (`explain_post_state_check_plan` exists). | Mixed mode requires `WriteIdentity::Database` (`prepare_ledger_stage` :246). Confirm that this attributes rows to the session's account for R2 (`$createdBy` = session).                                                                                                                            |
| Membership change plus app content (spec §3.4)                                                                                                                            | Mixed ledger transaction                                                                                                                                                                                  | None.                                                                                                                                                                                                                                                                                               |
| Read-set validation: refuse a commit whose read evidence changed (spec §0 item 5)                                                                                         | Serializable transactions                                                                                                                                                                                 | Confirm that ledger and mixed transactions validate their read set at the authority, including rows matching a read that were accepted after the cut (phantoms), and that the refusal is a conflict, not a policy refusal.                                                                          |
| Confirmation premises checked at commit (spec §3.8)                                                                                                                       | None                                                                                                                                                                                                      | A publication carries recipient-confirmation premises (subject account, expected answers, asker's right); the authority re-evaluates them in the commit and refuses with `confirmation-stale`.                                                                                                      |
| Global wait for governance                                                                                                                                                | `wait_for_transaction(tx, Global)` (sync.rs:8985)                                                                                                                                                         | None.                                                                                                                                                                                                                                                                                               |
| Offline governed operations, device-active predicate (spec §3.7, §5.3)                                                                                                    | `read_provisional_transaction` → `{owner, accepted_through, own}` (provisional_transactions.rs:30, :71)                                                                                                   | Ledger transactions refuse a non-authority with no upstream (`begin_admitted_ledger` :146), so offline creation must be an ordinary serializable batch plus a provisional read. Provisional reads refuse joins, recursion and policy traversals, which is acceptable because replay is client-side. |
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
- **Recovery secrets (spec §2.9).** The recovery key derivation (keyed BLAKE2b from a 32-byte recovery secret) lives in `jazz-e2ee` next to the group key-pair derivation. Bindings pass the secret in, never store it: for local-first accounts it is the account secret the binding already holds; for external accounts it comes from `BrowserPasskeyBackup` (`jazz-tools/passkey-backup`, browser) or a recovery phrase, and the runtime keeps it only for the registration or restore it is used for.
- **Error.** A store failure fails the operation loudly (spec §0). It is never reported as `key-unavailable`. An unknown store format is refused and never overwritten (spec §6.1).

### H10. Feature flag and build split

- There is no client/server feature in jazz-core today. `runtime` is used by both jazz-server and jazz-app (jazz-server/Cargo.toml:20, jazz-app/Cargo.toml:39), and the role is decided at runtime (`OpenAs`, `trusted_backend`, `set_authority`).
- **Proposal.**
  - Add a feature `e2ee-client` on `jazz-db`, re-exported by `jazz`. It gates the `E2eeHooks` trait, `DbConfig::with_e2ee` and every call site; with it off, each site compiles to nothing.
  - `jazz-e2ee-client` enables it. Client bindings (napi, wasm, native-relay, jazz-app-support, jazz-ffi) depend on `jazz-e2ee-client`.
  - jazz-server never does.
  - The deploy-side code (H7 system tables, `derive_e2ee_policies`, the native validator) is **not** gated: the server needs it.
- **Feature unification caveat.** A workspace build that compiles a client binding and the server together unifies `e2ee-client` into the server's jazz-core. That is harmless, since there is no crypto dependency and the hooks are only installed by client openers, but it is not a hard exclusion. Add a runtime guard: `Db::set_authority` (db.rs:2150) refuses a `Db` with hooks installed. Add a CI check that `cargo tree -p jazz-server -e features` lacks `e2ee-client` when built alone.

### H11. Manager enrolment and recipient sync

- **Mostly a client-runtime task, with one new authority request.** Enrolment and the sync (spec §3.8) run above `Db`, like H6, for every scope and group table; the sync parts apply only where `recipients` is declared:
  - it subscribes, as the client's own identity, to `__e2ee_spaces` filtered by each scope's catalogue ID, and to the group roots it can read; these are the spaces and groups the session receives or manages (R11–R12, R17);
  - it confirms management per space or group with the existing online permission advice (an authority-scoped dry run of the `manageKeys` operation on the governing row); `Unknown` means "not now", never "yes";
  - on a confirmed right with no self-authored `add`, it enrols itself first (a self-authored grant or membership record);
  - for each managed space or group with `recipients`, it subscribes to the membership relation, `<membership table> where <relation column> = identifier`, under the application's ordinary read policy;
  - for adds, it reads the relation and the records in one ledger read (`Db::read_ledger_transaction`), so it acts only on accepted rows, and stages grants and deliveries through the same publication paths as manual changes (H8).
- **New: recipient confirmation.** Removals need the authority to confirm, for a manager of space or group `S` and a recipient `R`, two things, each `yes`, `no` or `unknown`: whether any row of `S`'s membership relation names `R`, and whether `R` passes `S`'s `manageKeys` rule. It is evaluated by the server over all rows, not under the asker's read policy, and answered only to a manager of `S`. This is a new authority request alongside permission advice, not a `Db` hook: an indexed lookup on the relation column, plus the `manageKeys` rule evaluated with `R` as the subject (D9), both gated by the `manageKeys` check on the asker. The client asks on an observed deletion, for every manager recipient on an observed change to the governing row or other visible `manageKeys` inputs, on catch-up for every unnamed recipient and every manager recipient, and before each rotation for every manager recipient. It removes `R` only when both answers are `no`.
- **Premises at commit.** The answers are not a token for later use: the publication that removes `R`, and a rotation relying on its confirmations, carry them as premises that the authority re-checks at commit (H8); `confirmation-stale` makes the sync evaluate again. A rotation proceeds when an answer is `unknown`, and that manager recipient receives the new key (spec §3.8).
- **Former managers.** Removed by the remaining managers' sync once confirmation says they neither manage nor are named by a membership row (both roles), or only the `manager` role when a row still names them. Their own runtime may also remove itself when its permission advice turns to denied (R26). Where confirmation can only answer `unknown` (a claims-reading rule, acknowledged at deploy), managers remove former managers explicitly with a `manager`-role `remove`.
- **Space discovery and creation (spec §3.3).** For every scope table, the runtime watches governing rows readable to the session (a subscription per scope table, under the application's read policy) and, for rows whose derived space ID has no root, confirms management with permission advice (batched) and creates the space. It also creates the space right after the session's own accepted insert of a governing row it manages. Cost is proportional to the governing rows the session reads, never to other rows. Better still, and recommended: one authority query, "governing rows of this scope table that I manage and that have no space root", answered by the server with the `manageKeys` evaluation and an anti-join on `__e2ee_spaces`, paged; it bounds discovery by the result instead of by the readable rows.
- **Cost.** One subscription per managed space or group plus one per declared table; work is proportional to the spaces and groups the session manages and their membership changes, never to other spaces. Catch-up costs one confirmation per recipient not named by a visible row.
- **App-file parsing** is H1's `recipients` key; the server's parts are the R24 and R25 checks in H7 and the confirmation request.
- **Refusals.** With `recipients` declared, the runtime refuses the application's explicit add/remove calls (`recipients-declared`). It refuses its own removal, leaving or demotion when it recognises that it is the last manager recipient (`last-manager`); the server enforces only the grant-level rule (H7).
- **Rotation requests and the floor (spec §3.1).** Maintenance reads, in one ledger read, the requests readable to it after its cursor (all of them when a path changed or on a cold start) with their paths, the public key chains of its direct children and group recipients (and the `covers` lists of their `rotation` and `coverage` requests), and its own deliveries; it delivers only to keys clear of every request reachable through that child (the permanent delivery floor), rotates at once while a request is outstanding (an optional coverage wait is configurable, default none), writes a `coverage` request when it consumes a descendant request without rotating, keeps its own authenticated per-ancestor cursor tied to the path set, and relies on read-set validation for the commit.
- **Reset approvals (spec §2.11).** Managers' runtimes list reset-pending accounts per managed space and group (`db.encryption.pendingResets()`, `reset-approval-needed` events); `db.encryption.approveReset(scope, account)` publishes the approval, the re-binding grants and the deliveries. The sync never approves on its own.
- **What would need core beyond this.** Nothing for correctness. If one subscription per managed space proves too costly for a session that manages many spaces, a single subscription over the membership table joined to managed spaces would need relation-shaped subscriptions the runtime cannot express today; measure first.

## 3. Summary of hooks

| #   | Hook                                                   | Call site                                                                                | Sync/async                    | Crosses                                         | On failure                                           | Hot-path cost                        |
| --- | ------------------------------------------------------ | ---------------------------------------------------------------------------------------- | ----------------------------- | ----------------------------------------------- | ---------------------------------------------------- | ------------------------------------ |
| H1  | `declarations`                                         | `load_client_current` → `DbConfig::with_e2ee`; deploy request                            | Sync, at open                 | Encryption entries, groups, app ID, env         | Load or deploy refusal                               | Once                                 |
| H2  | `seal_write`                                           | `Db::write_op` before `chunk_large_cells`; `TxRef` staging                               | Sync                          | Table, row, kind, cells, stored space           | `NeedsKey` → binding acquires and retries; `Refused` | Set lookup; µs per encrypted cell    |
| H3  | `open_rows`                                            | `run_with_deps`; fanout `group_rows` / `recheck_rows` / `stored_images`; strict tx reads | Sync                          | Rows in/out, unreadable reports, missing epochs | Lenient: drop and report; strict: error              | Decrypt per row per refresh (cached) |
| H4  | `plan_query`                                           | `prepare_query_bound`, `remote_shape`                                                    | Sync                          | Query → physical query plus residual            | Refuse unsupported shapes                            | Prepare-time only                    |
| H5  | `seal_stream` / `open_stream`                          | large_ops streaming begin/push/finish; whole-value read                                  | Sync per chunk                | Byte chunks                                     | `NeedsKey` at begin; ranges refused                  | Streaming AEAD                       |
| H6  | none (internal subscriptions)                          | runtime above `Db`                                                                       | Async (runtime)               | Delivery rows                                   | Retry with backoff                                   | Normal subscription                  |
| H7  | `derive_e2ee_policies`, validator                      | server `deploy`, `prepare_deployment`                                                    | Deploy-time and server insert | Declarations → `TablePolicies`                  | Deploy refusal; policy refusal                       | Recursive policy per internal insert |
| H8  | (existing tx APIs + `transaction_fate`)                | runtime                                                                                  | Async                         | Tx ids, fates                                   | Refusal via fate                                     | Governance only                      |
| H9  | `PrivateStore`, `RandomSource`                         | bindings → runtime                                                                       | Async (store)                 | Documents, wrapped secrets                      | Loud failure                                         | Off the read/write path once loaded  |
| H10 | `e2ee-client` feature                                  | Cargo                                                                                    | n/a                           | n/a                                             | Guard in `set_authority`                             | Zero when off                        |
| H11 | none (enrolment, recipient sync); confirmation request | runtime above `Db`; authority                                                            | Async (runtime)               | Membership rows, space and group records        | Wait for a manager online; idempotent retry          | One subscription per managed space   |

## 4. What jazz-core must expose that it does not today

1. `DbConfig::with_e2ee` and the `E2eeHooks` trait with its call sites (H2–H5).
2. The current row's space cell passed to the write hook for updates and upserts.
3. `Db::refresh_reads_of(tables)`: a public wrapper of `Fanout::refresh_where`.
4. The rewritten-query path through `prepare_query_bound` and `remote_shape`, with the epoch set folded into `Shape`.
5. `load_client_current` (or a sibling) returning E2EE declarations; `DeployRequest.encryption`.
6. A system-table category: a reserved prefix, a Jazz-defined schema fragment, and Jazz-defined policies that apps cannot override.
7. `PolicyExpr::InheritsAs` (a soft reference to a named table).
8. A server-maintained current-edge projection, and a native insert validator (or policy-IR equivalents).
9. A durable `Db::transaction_fate(tx)`.
10. Confirmation of how mixed-ledger `WriteIdentity::Database` maps to `$createdBy.account`.
11. An awaited JS→Rust callback facility in jazz-wasm.
12. The recipient confirmation authority request (H11).
13. Evaluating a space's or group's `manageKeys` rule for a named account other than the requester, for recipient confirmation (D9).
14. The transaction-wide grant-level last-manager check over the projection's role flags (H7).
15. Encryption declarations recorded in every schema version (`history` in `jazz.app.json`), returned by the loaders and carried in the deploy request, and a deploy check that refuses a migration changing an encrypted column's plaintext type, scope or space without a conversion ([migrations](E2EE_MIGRATIONS.md) §3.1).
16. A client-visible API for the authority's active schema version, to gate conversions ([migrations](E2EE_MIGRATIONS.md) §3.7).
17. Read-set validation for ledger and mixed transactions, including phantoms, reported as a conflict (H8, spec §0 item 5).
18. Commit-time re-evaluation of recipient-confirmation premises (H8, H11).
19. Insert checks for derived version 8 IDs, group public-key co-insertion and uniqueness, binding uniqueness, and the declared-mode self-removal refusal (H7).
20. One-edge readability of rotation requests over the current-edge projection (H7), and the R27 delivery disjunct.
21. A reset event in each binding's subscription stream, for the bounded-wait wrapper (H3).
22. Transitive rotation-request readability with read-time paths over the current-edge projection (requests retained; optional client-cursor filtering), and the per-(scope, account) admitted generation with the pending-governance refusal (H7).
23. Marking lens-supplied defaults on rows handed to the read hook, so a stored empty cell can be reported (H3).
24. Optionally, the "managed governing rows without a space root" authority query (H11).

## 5. Main risks

- **Key versions accumulate with churn.** Every removal adds an epoch. Reads look up the held key per cell epoch, H4 computes one equality token per held epoch per predicate, and each rotation costs one delivery per remaining direct recipient device or group recipient (nesting reduces it). Measure reads, equality queries and rotation in a high-churn space before landing.
- **Hot-path decryption.** Every subscription refresh re-materialises and re-decrypts its rows; fanout re-runs whole groups on invalidation. A 10,000-row live query in a busy room could decrypt 10,000 cells per change.
  - Mitigations: the decrypted-cell cache keyed by ciphertext hash, and decrypting only the selected columns.
  - Benchmark before landing. The cache holds plaintext in memory; it is cleared on held-key removal and sign-out.
- **Browser thread model.**
  - Durable data lives in the worker (`browser.rs`), while tabs have in-memory dbs. If decryption happens in the worker, plaintext crosses `postMessage` to tabs; this is acceptable because it stays same-origin, and only one key store is needed.
  - If tabs decrypt, each tab needs the private store. IndexedDB and WebCrypto are available to tabs, but held-key revisions must then be coordinated across tabs (spec §6.4 "last tab out").
  - Decided: keys and decryption in the worker (D5).
- **Blocking versus async key fetch.**
  - Keeping the hooks sync and returning `NeedsKey` keeps core simple. The costs:
    - every binding entry point must loop on `NeedsKey`;
    - callback-style `Db::transaction(cb)` (db.rs:3620), whose closure cannot await, refuses a staging call that needs an unheld key with `key-unavailable`; the application acquires keys first (spec §5.6).
  - A fetch also performs ledger reads through the same `Db`, so it must never run re-entrantly inside a hook. The runtime must never call `Db` from inside a hook.
- **Catalogue identity before confirmation.** A first offline open cannot build cell contexts.
- **Policy cost on the server.** Recursive membership policies run on every read and insert of internal tables, and fanout must evaluate them for sync visibility (`fanout.rs:2642 conjuncts`, `policy_pins`). That path is untested with `Gather` at depth 8.
- **Unreadable rows and pagination.** Short pages are accepted by the spec, but UI code that treats a short page as the end of the results will misbehave.
- **Feature unification.** See H10.

## 6. Decisions

- **D1. Key-management declaration (decided).**
  - `"manageKeys": { "via": <forward relation, optional>, "allowedTo": <op> }` is required in every `encryption` entry and in `encryptionGroup`, with an agreement rule across entries for the same scope. There is no implicit default.
  - Recipients follow an optional declared membership relation, `"recipients": { "via", "account"?, "group"? }`, in `encryption` entries and in `encryptionGroup`, kept in step by the client runtime (H11); omitting it is manual mode. Every manager is a recipient; removals happen only on authority-confirmed deletion; the server refuses removing the last manager recipient's grant (R25), and in the first release nothing more: application writes that take the right away are not checked.
- **D2. Where decryption happens (decided: the recommendation).**
  - Recommend inside core at result materialisation (H3), so pagination, fanout diffs and every binding behave alike.
  - Alternative: above core, in `binding_host::Binding` and jazz-app. That means no core read hook, but every binding and the delta indices must be patched separately.
- **D3. Whether the E2EE system schema enters the app's schema hash and bundle (decided: the recommendation).**
  - Recommend including it deterministically: it is versioned, and appended only when encryption is declared. One bundle identity then covers what clients and the server enforce.
  - Alternative: a separate system namespace outside the hash. This is cleaner for app migrations, but needs a second catalogue track.
- **D4. Nested-group and cycle enforcement on the server (decided: the recommendation).**
  - Recommend a server-maintained current-edge projection plus a native validator for cycle and depth.
  - Alternative: pure policy IR (recursive `Gather` over immutable events). This keeps core uniform, but needs a "latest-by-position" operator and is hard to review.
- **D5. Browser worker model (decided: the recommendation).**
  - Recommend keys and decryption in the worker that owns the durable store, with tabs receiving plaintext.
  - Alternative: per-tab decryption with a shared IndexedDB key store.
- **D6. Key-fetch triggers (decided: the recommendation).**
  - Recommend internal subscriptions run by the client runtime (H6).
  - Alternative: a raw arrival callback in `Db::notify`.
- **D7. No-key writes (decided: the recommendation).**
  - Recommend `NeedsKey` plus a retry in the binding.
  - Alternative: async hooks inside core, which would break the run-to-completion model.
- **D8. Randomness (decided: the recommendation).**
  - Recommend `OsRandom` by default everywhere, with host injection only for tests.
  - Alternative: require each binding to supply a `RandomSource`.
- **D9. Evaluating `manageKeys` for another account (decided, scoped).**
  - The server evaluates a space's or group's `manageKeys` rule with a named account as the subject, so recipient confirmation (H11) can tell remaining managers that a manager recipient lost the right.
  - Scope: only this rule, used inside recipient confirmation (answered only to a current manager of the same space or group) for confirming recipients and removing former managers, and in the commit-time premise check (H8). It is a server-only entry point that binds the named account as the subject; no client request can name a subject otherwise.
  - Limit: the named account has no session, so an expression reading `session.claims` evaluates to `unknown`; confirmation then keeps the manager. The deploy refuses such a rule unless acknowledged (H1).
