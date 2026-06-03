# Operator Hard Delete and Compliance Erasure

## Summary

> The engine can already erase a row; this work makes that erasure reachable by an operator, atomic within a transaction, complete across a row's branches and the server's prefixes, delivered to active subscribers immediately and to other cache-holders the next time they run a covering query — while scoping the capability to the backend handle and documenting precisely what erasure can and cannot guarantee.

## Relationship to the original deletion model

No written specification of the original deletion design could be located. The available evidence suggests deletion was intended as a single user-facing `delete` that soft-deletes, with hard deletion occurring only on the server as a background truncation of aged-out soft-deleted rows — never invoked directly by a user. Supporting evidence:

- [`file_storage_cascade_integration.md`](../../b_launch/file_storage_cascade_integration.md) describes the model as "eager soft delete, authoritative hard delete".
- [`commit_author_principal_created_by_permissions.md`](../a_mvp/commit_author_principal_created_by_permissions.md) refers to hard delete only as a tombstone commit and a history-truncation boundary, never as a user operation.
- In the code, soft `delete` is plumbed through every layer and binding, while `hard_delete` and `truncate` were implemented, tested, and left unexposed.

The design in this spec is consistent with that inferred intent: hard delete is exposed only on the backend handle, and the primary documented use case is a server-side job that truncates aged-out soft-deletes.

## What exists today

The engine already implements the erasure primitive:

- `QueryManager::hard_delete` writes a tombstone with empty content and `delete: hard` metadata, removes the row from every index including `_id_deleted`, and truncates history so only the tombstone remains. It is authoritative: it overrides concurrent and later commits, and `restore` on a hard-deleted row returns `RowHardDeleted`.
- `QueryManager::truncate` performs the same erasure but only on a row that is already soft-deleted.
- Both are covered by `manager_tests/deletes.rs`. The hard-delete tombstone already syncs upstream (`hard_delete_syncs_row_batch_created_to_server`).

Neither primitive is reachable above the engine. There is no `runtime_core`, `schema_manager`, binding, or TypeScript method that calls them. Soft `delete` is the only deletion operation exposed to any caller today.

`restore` was exposed recently along the full path — engine → `schema_manager` → `runtime_core` → WASM/NAPI/RN bindings → TypeScript `db.restore`, with a changeset and docs. It is the reference template for the plumbing in this spec.

## Goals

In priority order. Where goals conflict, the higher-priority one wins.

1. **Compliance erasure (primary).** Erase a row's content, history, and indices from every store the system controls, and propagate that erasure to every honest, cooperating client that holds a copy — reaching active subscribers immediately and other cache-holders when they next run a covering query.
2. **Storage reclamation (secondary).** Allow operators to reclaim space consumed by aged-out soft-deleted rows and their history.
3. **Permanent delete (tertiary).** Deletion that `restore` cannot reverse. Already provided by the engine primitive.

## Non-goals

- **Automatic cascade.** Hard delete acts on a single row. Erasing dependent rows (foreign-key children, files, `file_parts`) is the application's responsibility. Cascade — including refcount-aware erasure of content-addressed file parts — is separate future work ([`file_storage_cascade_integration.md`](../../b_launch/file_storage_cascade_integration.md)).
- **Client-side exposure.** Hard delete is not exposed on the scoped client `Db` or in the client bindings as part of this spec.
- **Exposing `truncate`.** The engine's `truncate` (finalising an already-soft-deleted row to a hard delete) stays internal — not exposed in any binding, API, or UI. It is likely useful later as an internal garbage-collection primitive (a server-side pass over aged-out soft-deletes), but exposing or wiring it is out of scope here.
- **A separate erase permission.** Not required, because hard delete is reachable only from the admin-authenticated backend handle. See [Authorisation and exposure](#authorisation-and-exposure). This was considered; if the hard-delete operation is ever exposed client-side, it would likely be required.
- **A queryable erasure audit trail in the engine.** Hard-deleted rows leave all indices; the engine retains no enumerable record. Audit is the application's responsibility.
- **Defending against offline-only, modified/malicious clients, or out-of-band copies.** Any device that has ever seen a row could retain a copy, in local storage or by other means (manual copying, screenshotting, etc.). This spec defines a best-effort distributed deletion process for honest clients, not a guarantee that all persisted copies are deleted.
- **Cross-environment erasure.** `dev` and `prod` are separate databases.

## Authorisation and exposure

Hard delete is exposed only on the backend handle, as `db.hardDelete(table, id)` and the transaction form `DbTransaction.hardDelete`. It is not added to the scoped client `Db` or to the client bindings, which continue to expose only soft `delete`.

Because the only caller is the admin-authenticated backend handle, no dedicated erase permission is required. The server classifies the hard-delete tombstone by shape as an ordinary `Operation::Delete`, so the existing delete permission gates it; this is somewhat redundant, as the backend handle bypasses the evaluation anyway, but is more resilient to future changes. An application that must guarantee retention of specific data (for example, a legal hold) must model that constraint in its own application layer rather than relying on the engine to refuse erasure. This is to be included in the documentation.

## Erasure scope

### Single row

`hardDelete` erases one row and nothing it references. A row typically has many references; cascading the delete through them is, for now, an application-orchestrated operation that calls `hardDelete` on each row in the subject's data graph. See [Erasing across rows](#erasing-across-rows).

### Across schema-version branches

The current `hard_delete` operates on `current_branch()` only. A row that has materialised on more than one schema-version branch (via migration copy-on-write) would remain readable on sibling branches. This is a correctness gap for erasure.

Required behaviour: a `hardDelete` erases the row across every schema-version branch its originating node can reach — that is, every live schema in its own `(env, user_branch)` family, since a `Db` handle is bound to a single `user_branch`. Branches on which the row is absent are ignored. Reaching the row's copies under _other_ user-branches is the server's job ([Server-side cross-prefix fan-out](#server-side-cross-prefix-fan-out)); together they cover every schema-version and every user-branch within the originating env, and never cross envs. This is new engine work — a loop over the family's branches, each with its own index-clear and tombstone.

### Server-side cross-prefix fan-out

A single `Db` handle is bound to one `user_branch`, and it cannot reach other prefixes. The server holds all prefixes. On receiving an authoritative hard delete for a `row_id`, the server must erase that `row_id` across every prefix sharing the originating `env` (any `schema_hash`, any `user_branch`). Today the server applies a batch to its target prefix only; this fan-out is new.

### Environments

Cross-environment erasure is out of scope. A `prod` erasure does not affect `dev`.

## Propagation to clients

A client erases its copy of a row when, and only when, it receives the hard-delete tombstone. Two delivery paths are needed: one for clients actively subscribed to the row, and one for clients that hold a stale cached copy without a covering subscription. The second is the larger piece, and it is required — the cached-without-subscription case is not an edge case but the ordinary residue of normal use (one-shot `query()` calls, and subscriptions torn down when a view closes).

### Active subscribers

When a client is subscribed to a query whose result contains the row, the server forwards the tombstone on delete via the existing path (`forward_update_to_clients_except`, gated by `is_in_scope`), and the client erases its local copy (`update_indices_for_hard_delete_on_branch`). A client that is offline with a still-live subscription catches up on reconnect.

This delivery should work for hard delete exactly as it does for soft delete; slice 4 adds the test.

### Cache-holders without a subscription — reconcile-on-settle

When a client (re)runs a query, the server already sends the authoritative result membership in `QuerySettled.scope: Vec<(ObjectId, BranchName)>`, and the client already stores it (`remote_query_scopes`) and erases locally on receipt of a tombstone. The missing step, added on the client at settle time:

1. Diff the client's local result against the server's authoritative `scope`. Any row the client surfaces locally that the server omits is a candidate.
2. For each candidate, the client obtains the row's authoritative current state from the server. The client does not infer the reason for absence — "absent from the result" can mean erased, soft-deleted, or simply no longer matching the filter, and treating a filtered-out row as deleted would destroy valid data and violate local-first availability.
3. A returned hard-delete tombstone causes the client to erase via the existing path; an updated row causes the client to apply the update, after which the row leaves the view with its data intact.

This step requires a **protocol change**. The chosen mechanism is a **per-row pull**:

- **The client requests authoritative state for the discrepant rows.** There is no client→server fetch today (client→server traffic is limited to query (un)subscription, batch sealing, batch-fate requests, and the client pushing its own writes), so this adds one new client→server request message. The client batches the discrepant `(row_id, branch)` set from a settled query into a single request rather than issuing one per row.
- **The server resolves each by id, and the resolver already exists.** A hard-deleted row is gone from every index, so an index- or query-based lookup cannot find it — but a _by-id_ load can. `load_current_row_from_storage` reads the retained locator for the table, then the visible/history regions, and the tombstone passes the `is_visible()` filter because it keeps `state = VisibleDirect` (deletion is marked by `delete_kind`). So the server reuses that existing by-id load; the only genuinely new work for the pull is the request message itself.
- **The reply reuses existing delivery.** The server answers with `RowBatchNeeded` — the hard-delete tombstone for an erased row, or the current row if it turns out to have been merely filtered or updated out. So only the request is new protocol; the response and the client's tombstone-application path already exist.

The alternative transport (frontier catch-up) was considered and rejected as heavier; see [Other approaches considered](#other-approaches-considered). Closing this gap also resolves the existing [stale-cache-after-scope-removal](../../issues/stale-client-cache-after-scope-removal.md) issue.

Cost: nothing changes on the happy path, where active subscriptions already receive deletes. The reconciliation runs only at a _server_ settle (the client online with the subscription active), and only for genuine discrepancies.

Residual, accepted: reconciliation fires only when a covering query **settles against the server** — the client must be online with that subscription active long enough to receive the server's `QuerySettled` and complete the pull. A query run while offline settles from the local cache and does _not_ reconcile, and merely coming online while unsubscribed does nothing (reconciliation is tied to a query settle, not to the connection). So a cached row is erased the next time the client runs its covering query _while online_; a client that only ever runs that query offline — and is online only while unsubscribed — never reconciles it. Ordinary use (opening a view while online) reconciles promptly. Closing the residual fully would require reconciling on reconnect or server-side possession tracking, both rejected for the MVP (see [Other approaches considered](#other-approaches-considered)). It is accepted for now.

## Erasure guarantees and limits

The system can erase data it controls and propagate the erasure to honest, cooperating clients — immediately for active subscribers, and on the next covering query that settles against the server (i.e. while online) for other cache-holders. It cannot:

- erase a row cached on a client that never runs a covering query _through to a server settle_ again — e.g. one that only ever runs the query offline (see [Propagation to clients](#propagation-to-clients));
- erase a client that never reconnects;
- compel a modified client to act on a tombstone (the tombstone is data; code execution cannot be forced on a device the system does not control);
- recover copies taken out of band (screenshots, exports).

This is a property of distributed replication, not specific to Jazz. In general, compliance requirements do not require hard guarantees against uncontrollable copies, and instead require rigour on erasing controlled data and taking reasonable and practical steps to propagate. Documentation must describe the capability as best-effort erasure across controlled stores and honest clients, and must not imply erasure everywhere.

## Transactions

### Transaction support for hard delete

Soft `delete`, `insert`, `update`, and `restore` are transaction-aware: each checks `write_context_is_open_batch` and, inside an open transaction, stages its change and defers index work to commit. `hard_delete` and `truncate` take no `write_context` and apply immediately, so they cannot currently participate in a transaction.

Required work, following the `restore` template:

- `hard_delete` and `truncate` accept `write_context: Option<&WriteContext>`.
- Add the open-batch staging branch so a hard delete inside a transaction stages the tombstone and clears indices at commit.
- Thread the context through `schema_manager` and `runtime_core`.

The `write_context` is what lets a hard delete join a transaction (batch identity and branch targeting). It does not yield a discernible operator identity: a backend write stamps the Jazz default provenance, so the tombstone is not a usable record of _who_ erased (see [Audit](#audit)).

**Conflict rule.** Within one transaction the rule is asymmetric. A hard delete staged _after_ other writes to the same row supersedes them — the committed result is the history-truncating authoritative tombstone. A write staged _after_ a hard delete on the same row is rejected: the staged tombstone is visible to later operations via `staged_row_for_write`, so the operation throws `RowHardDeleted`. The engine already does the equivalent today — the update path returns `RowAlreadyDeleted` when the staged or current row `is_hard_deleted()`; this work makes the hard-delete case throw the precise error consistently across insert/update/delete/restore. Throwing, rather than silently letting the delete win, surfaces the likely programmer error of operating on a row already erased in the same transaction.

### Rollback semantics

A hard delete is irreversible only once its transaction commits. Inside an open transaction it is staging state: the destructive steps — history truncation and index removal — are deferred to commit and are not performed at the `hardDelete` call. A transactional batch publishes only on acceptance (staging prefix; see [Opt-In Transactions](../a_mvp/opt_in_transactions_replayable_reconciliation.md)), so if the transaction is aborted or rejected before commit, the staged tombstone is discarded and nothing is erased. There is no irreversible operation to undo: the irreversibility coincides with commit, after which the transaction is no longer open to rollback. This matches the staging model `insert`, `update`, and soft `delete` already use. The constraint specific to hard delete is that truncation must be gated on commit, never applied at staging time.

### Erasing across rows

Because cascade is not automatic, erasure of referenced data is application-orchestrated, performed atomically in one transaction:

```text
begin transaction
  query the subject's rows (including soft-deleted rows; see below)
  hardDelete each row
  insert an audit record (metadata only; see Audit)
commit
```

The erasure and its audit record commit together or not at all.

This is an interim state until cascades land.

## Querying soft-deleted rows

The engine supports `query(...).include_deleted()`, which scans both the live `_id` index and `_id_deleted`, returns soft-deleted rows with preserved content, and excludes hard-deleted rows. It is not exposed in the public API. `restore` can return a soft-deleted row by id, but soft-deleted rows cannot currently be enumerated.

This is a gap for erasure: a subject's previously soft-deleted rows still hold content, and an erasure pass that can only see live rows would miss them.

`includeDeleted` is exposed on the backend query path (the same exposure boundary as `hardDelete`). This enables two cases:

1. Erasure reaching the subject's soft-deleted rows.
2. A retention-GC job, run on the backend: select soft-deleted rows whose `$updatedAt` is older than a threshold and `hardDelete` them. `$updatedAt` is a provenance-sourced magic column already resolvable in queries (`MagicColumnsNode`; TS support in `query-adapter.ts`), so the age predicate is expressible. This is the server-side truncation-of-aged-out-soft-deletes use case described under [Relationship to the original deletion model](#relationship-to-the-original-deletion-model).

## Audit

The tombstone records _when_ the erasure happened and retains no content, but it does not usefully record _who_: the erasure runs through the backend handle, which stamps the Jazz default provenance rather than a discernible operator identity. It is also not enumerable, because a hard-deleted row is absent from all indices.

As a result, any audit log must be the application's responsibility. The documented pattern records audit metadata contemporaneously, in the same transaction as the erasure:

- The audit row is a normal insert into an application-owned table and records subject id, table, row ids, operator, and timestamp.
- The audit row records metadata only, never erased content.
- Because it commits atomically with the hard deletes, an erasure cannot occur without its record, nor a record without the erasure.

## API surface

- `db.hardDelete(table, id)`, on the backend handle only.
- Returns a `WriteHandle`, as `delete` does, so a caller can `.wait()` for the erasure to reach a durable tier before reporting completion.
- Already-hard-deleted target: idempotent success (the desired end state holds; a retried erasure must not error).
- Missing target: error (`ObjectNotFound`), distinct from already-erased.
- Only `hardDelete` is exposed. The engine's `truncate` (the soft-first variant) remains internal; it is `hardDelete` with a precondition the application can enforce, and its name is misleading at the API surface.
- Reactivity is unchanged: subscribers observe the row leave query results, as with soft delete.

## Work slices

1. **Engine.** Add `write_context` and open-batch staging to `hard_delete`/`truncate`; make a single hard delete erase across every schema-version branch in the family. Extend `manager_tests/deletes.rs` for transactional and multi-branch erasure.
2. **Plumbing (backend only).** `schema_manager::hard_delete` → `runtime_core::hard_delete` → backend `Db.hardDelete` and `DbTransaction.hardDelete`, following the `restore` diff. Not added to the scoped client `Db` or the bindings.
3. **Server cross-prefix fan-out.** On an authoritative incoming hard delete, erase the `row_id` across every prefix the server holds in the originating `env`.
4. **Active-subscriber tombstone delivery (test only).** Add a downstream-forwarding test confirming the existing in-scope delete-forwarding delivers a hard-delete tombstone to active subscribers. No code change is expected — the tombstone keeps a visible `RowState` and is marked by `delete_kind` — but this is currently untested for hard delete.
5. **Reconcile-on-settle (per-row pull).** Add one client→server request carrying a settled query's discrepant `(row_id, branch)` set. Server-side: resolve each by id off the retained locator via `HistoryScan::Row`, replying with the hard-delete tombstone (or current row) over `RowBatchNeeded`. Client-side: at settle, diff the local result against `QuerySettled.scope`, batch the request, and apply the replies. This also resolves the [stale-cache-after-scope-removal](../../issues/stale-client-cache-after-scope-removal.md) issue.
6. **Expose `includeDeleted`** on the backend query path, including filtering on `$updatedAt`.
7. **Documentation.** The subject-erasure transaction pattern with contemporaneous metadata-only audit logging; the retention-GC job; and a precise statement of erasure guarantees and limits, including the online-settle requirement and its residual.

## Risks

- **Reconcile-on-settle requires a protocol change.** Closing the cache-holder gap adds one new client→server request message (the per-row pull); the server-side resolver largely already exists (`load_current_row_from_storage` surfaces the tombstone by id). This is the largest piece of new design in the spec. It is justified because the gap it closes is ordinary cached data, not an edge case, and because it also fixes the existing stale-cache issue.
- **Bulk-erasure reconciliation cost.** A retention-GC pass can hard-delete many rows at once. Every client holding them then produces a large discrepancy set at its next settle, i.e. a burst of reconciliation. The mechanism should batch, and the worst case (a large erasure intersecting many clients' caches) should be measured.
- **Active-path delivery confirmed, untested.** Active subscribers receive hard-delete tombstones through the same forwarding path as soft delete, because the tombstone keeps a visible `RowState` and is marked deleted via `delete_kind`. No code change is expected; slice 4 adds a downstream-forwarding test to lock it in.
- **Offline-settle residual.** Reconciliation fires only when a covering query settles against the server (client online, subscription active). A row whose covering query is only ever run offline, or whose online windows never coincide with an active covering subscription, is not reconciled. This is accepted; it is far narrower than the cached-without-subscription residual that accepting no reconciliation would leave, and ordinary online use reconciles promptly.

## Other approaches considered

### Initiation origin

- **User-initiated**. Instead of binding to the `Db` handle in a backend context only, it was considered whether this should also be included on scoped handles. This was rejected because app owners/developers may be under obligation to keep data (legal hold, etc.). Subdividing delete permissions based on hard vs. soft delete permissions would be extensive, and there is a reasonable application-level pattern already (an authenticated request to a server endpoint to delete data). It would also require threading the `includeDeleted` flag through to scoped `Db` handles.

### Client propagation

The chosen approach is reconcile-on-settle by per-row pull (above). The alternatives considered:

- **Frontier catch-up transport.** Instead of pulling discrepant rows by id, the client sends its last-seen sequence on subscribe and the server replays hard-delete tombstones in the query's table since then over the existing `RowBatchNeeded`. Rejected as heavier: there is no `HistoryScan::SinceSeq`, so the server would need either a new seq-indexed deletion scan or a full-branch history scan on every catch-up; it also requires per-client frontier state and a new `QuerySubscription` field, and its volume scales with total deletions in the table rather than with a client's own result. Its one advantage — also catching cached rows absent from the current result — is marginal against per-row pull's reach.
- **Accept the residual (no reconciliation).** Deliver tombstones only through active-subscription forwarding, and accept that any row cached without a covering subscription persists on the client indefinitely — re-running the query would not erase it, because the tombstone is never delivered. Rejected: that residual is the ordinary product of normal use (one-shot queries, closed views), so it would leave erased personal data on honest, online clients indefinitely — unacceptable for a compliance feature. It is recorded as the cheapest fallback only if the reconcile protocol change proves infeasible.
- **Client cache-eviction** (drop a cached row when its subscription ends) would break offline availability and is incompatible with local-first.
- **Server-side possession tracking** (record every row each client has received and push tombstones to all holders) adds heavy per-client server state for limited benefit.
- **Reconcile on reconnect** (on reconnect, re-settle recently-used queries, or send the client's cached row-ids to the server to check) would narrow the offline-settle residual without the client re-opening a view. Rejected for the MVP: it would likely mean a lot of back-and-forth with the server on every reconnect — re-issuing queries or enumerating the cache — for limited benefit over what reconcile-on-settle already covers in ordinary online use. Logged as a future idea ([reconcile-on-reconnect](../../ideas/3_later/reconcile-on-reconnect.md)) in case the residual proves too broad.

### Erasure mechanism

- **Crypto-shredding** (per-subject content encryption with key destruction as "erase") was considered and rejected. Retaining subject-keyed ciphertext may be problematic even if the key is destroyed. A cipher being weakened exposes a harvest-now-decrypt-later (HNDL) threat, and there is a further risk that a key copy persists somewhere (relatively high given Jazz's distributed nature); either could make the information recoverable. In Spain, for example, the data protection authority issues guidance that clearly states the threat of post-quantum compromise of existing encryption algorithms and strongly implies that crypto-shredding is _not_ adequate ([AEPD guidance](https://www.aepd.es/guias/10-malentendidos-anonimizacion.pdf)). Combined with the fact that Jazz's content-encryption layer is not built yet, crypto-shredding almost rules itself out. Documented here for future reference in case the question returns when implementing E2EE or cascading deletes.

## Related

- [Row Histories — Status Quo](../../status-quo/row_histories.md)
- [Schema Manager — Status Quo](../../status-quo/schema_manager.md)
- [Stale client cache after scope removal](../../issues/stale-client-cache-after-scope-removal.md)
- [Built-in File Storage: CASCADE Integration](../../b_launch/file_storage_cascade_integration.md)
