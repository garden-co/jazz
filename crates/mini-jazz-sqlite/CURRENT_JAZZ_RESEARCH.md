# Current Jazz Research Capture

This document is a raw collection area for behavior, invariants, product surface,
status-quo mechanics, and future-feature pressure from current Jazz that may be
missing or only lightly represented in `SPEC.md`.

It is intentionally not yet a polished spec. The next pass should classify,
deduplicate, resolve contradictions, and decide what belongs in the product spec,
the implementation strategy, or a status-quo comparison document.

## Capture Protocol

- Keep findings provenance-heavy: include source files, tests, docs, or symbols.
- Prefer product-visible semantics and durable invariants over implementation
  trivia, but collect implementation constraints when they shape the product.
- Do not fold findings into `SPEC.md` during this pass.
- Mark uncertainty explicitly rather than forcing premature design.

## Open Classification Buckets

- Product contract: behavior the new system should preserve or intentionally
  replace.
- Implementation constraint: status-quo detail that matters for feasibility,
  migration, performance, or compatibility.
- Missing product area: whole feature family absent from the new spec.
- Test/invariant candidate: behavior worth expressing as an invariant or whole
  system test.
- Status-quo-only: behavior useful for comparison, but not a desired new-system
  contract.
- Open question: needs Anselm/design clarification before folding into the spec.

## Raw Findings

Findings below are deliberately unclassified or lightly classified. Classification
comes next.

### Product/API/Docs

- The public product entrypoint is table-first TypeScript, not SQL-first. Users
  define `schema.ts`, derive `app.todos` table/query handles, and call `db.all`,
  `db.one`, `db.insert`, `db.update`, `db.delete`, `db.subscribeAll`, and current
  `beginBatch` / `beginTransaction` APIs. The new spec should preserve the
  high-level table/query shape while renaming/removing batch terminology in the
  new API. Sources: `specs/status-quo/ts_client.md`,
  `starters/ts-localfirst/README.md`.
- Framework bindings are thin lifecycle/reactivity layers over the same `Db`
  and typed app surface. Product matrix includes React, Vue, Svelte, Expo, plain
  TS, server-side TS, React Native, Cloudflare Workers, Rust bindings, and
  planned Go/Swift/Kotlin/SQL-over-HTTP/Webhooks. Sources: repository
  `README.md`, `crates/jazz-rn/README.md`,
  `examples/cloudflare-worker-runtime-ts/README.md`, `specs/ANNOUNCEMENT.md`.
- Developer workflow is product surface: `create-jazz`, `jazzPlugin`,
  `withJazz`, and `jazzSvelteKit` spawn local dev server, push/republish schema,
  and inject app/server environment. Sources: `packages/create-jazz/README.md`,
  starter READMEs.
- Inspector/devtools are visible product features. Inspector connects by
  `serverUrl`, `appId`, `adminSecret`, `env`, and `branch`. Source:
  `packages/inspector/README.md`.

### TypeScript DSL, Schema, and Generated App Surface

- Schema types include scalar SQL types `TEXT`, `BOOLEAN`, `INTEGER`, `REAL`,
  `TIMESTAMP`, `UUID`, `BYTEA`, plus enums, arrays, refs, JSON schemas,
  defaults, nullability, explicit indexed columns, and per-column merge strategy
  metadata. Source: `packages/jazz-tools/src/schema.ts`.
- `select()` always preserves `id`; `select("*")` resets to all root columns;
  selected root columns can coexist with includes; magic/provenance columns can
  be selected, filtered, ordered, and projected through nested includes.
  Sources: `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`,
  `packages/jazz-tools/src/runtime/query-adapter-tests/basic-query-structure.test.ts`.
- Runtime row alignment is by declared schema order, including includes and
  subscription deltas; magic projection values are kept ahead of included rows.
  Source: `packages/jazz-tools/src/runtime/client-tests/schema-order.test.ts`.
- Defaults are applied for integer, float, bytea, enum, JSON, timestamp, string,
  array, boolean, nullable, and refs. Explicit `null` for nullable fields does
  not trigger defaults. Source:
  `packages/jazz-tools/tests/ts-dsl/insert-api.test.ts`.
- JS boundary conversions are semantic: `BYTEA` returns `Uint8Array`, JSON text
  parses, timestamps become `Date`, provenance timestamps scale to JS
  milliseconds, and invalid JSON/date/bytea/enum values throw. Sources:
  `packages/jazz-tools/src/runtime/row-transformer.test.ts`,
  `packages/jazz-tools/src/runtime/value-converter.test.ts`.
- Transformed columns expose transformed row/write types, but `where` uses the
  raw stored type. Sources:
  `packages/jazz-tools/tests/ts-dsl/transformed-columns.test.ts`,
  `packages/jazz-tools/tests/ts-dsl/typed-app.test.ts`.
- Migration DSL serializes column add/drop/rename, table rename, table add/drop;
  table renames can combine with column migrations; create/drop tables cannot be
  mixed with column migrations; explicit table renames must structurally match
  after column migrations. Source:
  `packages/jazz-tools/tests/ts-dsl/migrations.test.ts`.

### Data Model, Rows, CoValues, and History

- Current Jazz is row-history based: logical rows have stable ids, concrete
  versions are identified by `(row_id, branch_name, batch_id)`, current reads
  use compact visible entries, and history remains for sync/reconnect/replay.
  Sources: `specs/status-quo/row_histories.md`,
  `specs/status-quo/batches.md`.
- History rows carry reserved engine columns plus user columns. Important
  status-quo fields include parents, updated/created timestamps, created/updated
  principals, state, confirmed tier, delete kind, deletion marker, and metadata.
  Source: `specs/status-quo/row_histories.md`.
- Visible entries store the current body, current branch frontier, tier preview
  batch ids, and optional synthetic merge sidecars. The spec’s new current
  projection table must preserve the product behavior without necessarily
  copying the status-quo visible-row encoding. Source:
  `specs/status-quo/row_histories.md`.
- Delete semantics are more detailed than the current mini spec: soft delete,
  hard delete, undelete, and truncate are present in current code. Deletes are
  row versions; hard deletes truncate history and override concurrent/subsequent
  commits in status quo. Sources:
  `crates/jazz-tools/src/query_manager/writes.rs`,
  `crates/jazz-tools/src/row_histories/resolution.rs`,
  `crates/jazz-tools/src/query_manager/manager_tests/deletes.rs`.
- Merge strategies are schema-relative. Current implemented strategies include
  implicit `lww` and explicit integer `counter`; the same stored conflicting
  history may resolve differently under different schema versions. Source:
  `specs/status-quo/row_histories.md`.

### Query, Subscription, and Reactivity Semantics

- One-shot queries and live subscriptions share semantics. `db.all`/`db.one`
  compile and settle the same graph used by `db.subscribeAll`. Source:
  `specs/status-quo/query_manager.md`.
- Subscriptions expose full materialized results plus deltas: callbacks receive
  the current `all` snapshot and row-level add/update/remove delta information.
  Sources: `starters/ts-localfirst/README.md`,
  `packages/jazz-tools/tests/browser/db.subscribeAll.test.ts`.
- Null semantics intentionally differ from SQL: `{ col: null }` and
  `{ col: { eq: null } }` mean `IS NULL`; `{ ne: null }` means not-null;
  `undefined` filters are no-ops. Sources:
  `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`,
  `packages/jazz-tools/src/runtime/query-adapter-tests/condition-translation.test.ts`.
- Multiple predicates on the same column are conjunctive and must be fully
  applied after index scanning. Source:
  `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`.
- `contains` works for text substring and array membership; empty text contains
  matches all strings. Sources: `packages/jazz-tools/tests/browser/db.all.test.ts`,
  `packages/jazz-tools/tests/browser/db.subscribeAll.test.ts`.
- Include semantics are product-visible: scalar includes return `null` for null
  or missing referenced rows; forward/reverse array includes skip missing
  referenced rows; includes only resolve requested relations. Source:
  `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`.
- `requireIncludes()` filters rows with missing scalar referenced entities and
  missing forward-array entities, but not rows with null scalar refs or reverse
  relation misses. Nested `requireIncludes()` is scoped; skipped rows affect
  limit/offset pagination. Source:
  `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`.
- Hop queries traverse scalar FK and UUID-array FK relations, support multiple
  hops, and subscriptions react when FK paths change. Gather queries are
  first-class relation IR; gather cannot combine with include, must use a
  forward hop, and can be followed by `hopTo`. Sources:
  `packages/jazz-tools/tests/browser/db.all.test.ts`,
  `packages/jazz-tools/tests/browser/db.subscribeAll.test.ts`,
  `packages/jazz-tools/src/runtime/query-adapter-tests/full-query-translation.test.ts`.
- Deep include subscriptions react to inserted dependencies several levels away.
  Source: `packages/jazz-tools/tests/ts-dsl/deep-include-reactivity.test.ts`.
- Query graphs currently include recursive relation nodes and recursive policy
  nodes. Recursive query and recursive permission lowering should be a next
  derisking target. Sources:
  `crates/jazz-tools/src/query_manager/graph_nodes/recursive_relation.rs`,
  `crates/jazz-tools/src/query_manager/manager_tests/recursive_queries.rs`,
  `crates/jazz-tools/src/query_manager/rebac_tests/recursive_inheritance.rs`.

### Transactions, Writes, Conflicts, and Reconciliation

- Simple `insert`/`update`/`delete` are one-member direct batches in status quo;
  new spec language should map this to one sealed transaction per write call,
  with `mergeable` and `exclusive` transaction modes. Sources:
  `specs/status-quo/batches.md`, `specs/status-quo/ts_client.md`.
- Insert/update/delete are synchronous locally and return waitable handles;
  `wait({ tier })` can target local/edge/global durability. Sources:
  `packages/jazz-tools/tests/ts-dsl/insert-api.test.ts`,
  `packages/jazz-tools/tests/ts-dsl/update-api.test.ts`,
  `packages/jazz-tools/tests/ts-dsl/delete-api.test.ts`.
- Update semantics: omitted fields and explicit `undefined` do not modify
  values; nullable fields clear with `null`; required fields reject `null`;
  returned fixture objects are not mutated by later updates. Source:
  `packages/jazz-tools/tests/ts-dsl/update-api.test.ts`.
- Open transaction/batch reads see their own staged rows and remain isolated
  from global reads. Writes after commit and reads after commit are rejected;
  callback transactions cannot be manually committed/rolled back. Sources:
  `packages/jazz-tools/tests/browser/db.transaction-reads.test.ts`,
  `packages/jazz-tools/src/runtime/db.transaction.test.ts`.
- Current transactional conflict behavior includes same-data conflict rejection
  for concurrent authority-decided transactions. Source:
  `packages/jazz-tools/tests/browser/db.transaction-reads.test.ts`.
- Batch/transaction fate is whole-unit: rejecting one member rejects the entire
  unit. New spec should preserve this at transaction level. Sources:
  `specs/status-quo/batches.md`,
  `crates/jazz-tools/src/sync_manager/tests/settlements.rs`.
- Conflict examples document optimistic eventually-consistent writes as LWW
  unless stronger transaction semantics are used. Source:
  `examples/moon-lander-react/walkthrough/jazz-moon-lander.md`.

### Sync, Storage, Topology, and Durability

- Browser persistent mode is a two-runtime topology: main thread is an in-memory
  UI peer; worker owns durable OPFS, upstream WebSocket, durable transaction
  records, settlements/fate, and submissions. Sources:
  `specs/status-quo/browser_adapters.md`,
  `packages/jazz-tools/tests/browser/worker-bridge.test.ts`.
- Browser OPFS durability includes persistence across shutdown/recreate and WAL
  recovery after crash. Sources:
  `packages/jazz-tools/tests/browser/worker-bridge.test.ts`,
  `specs/status-quo/storage.md`.
- Sync payload vocabulary includes catalogue entries, row batch entries, needed
  rows, explicit seals, batch fate, fate-needed, query subscription and
  unsubscription, query scope snapshot, query settled, schema warnings, and
  errors. Source: `specs/status-quo/sync_manager.md`.
- `QuerySettled` is a delivery barrier separate from row delivery; first
  subscription callback waits until the requested tier’s settled snapshot is
  present. Sources: `specs/status-quo/sync_manager.md`,
  `packages/jazz-tools/tests/browser/worker-bridge.test.ts`.
- Active query subscriptions are desired state and must be replayed on upstream
  reconnect/attach. Sources: `specs/status-quo/sync_manager.md`,
  browser reconnect tests.
- Initial query sync should send only the current visible row for deep history,
  but still be self-contained enough for a fresh peer to converge. Sources:
  `crates/jazz-tools/src/sync_manager/tests/settlements.rs`,
  `crates/jazz-tools/tests/clients_sync.rs`.
- Fate/settlement may arrive before rows; receivers persist it and materialize
  visibility once rows arrive. Source:
  `crates/jazz-tools/src/sync_manager/tests/settlements.rs`.
- Clients cannot author authoritative durability settlements; if settlement
  persistence fails, authority must not publish acceptance/rejection. Source:
  `crates/jazz-tools/src/sync_manager/tests/settlements.rs`.
- Server topology distinguishes core/global from edge by upstream URL. Edges
  connect to core as authenticated peers and use normal sync paths. Writes
  through one edge must become visible through core and peer edges at requested
  tier. Sources: `specs/status-quo/sync_manager.md`,
  `crates/jazz-tools/tests/edge_server_sync.rs`.
- Stale persisted edges must replay catalogue updates from core before accepting
  client work after restart. Source:
  `crates/jazz-tools/tests/edge_server_sync.rs`.
- Transport is app-scoped WebSocket at `/apps/:app_id/ws`; auth shapes include
  JWT, backend secret, admin secret, and peer secret. Source:
  `specs/status-quo/http_transport.md`.
- Storage isolation is app/namespace/driver scoped in browser/runtime tests.
  Sources: `packages/jazz-tools/tests/browser/db.storage-isolation.test.ts`,
  `packages/jazz-tools/src/runtime/db.persisted.test.ts`.

### Auth, Accounts, Groups, Permissions, and Policy

- Local-first auth is a product mode where the device can be the account. It
  mints/stores an Ed25519 secret; no login is required; clearing storage can
  lose account continuity. Production servers must explicitly allow it. Sources:
  starter READMEs, `crates/jazz-tools/tests/local_first_auth.rs`.
- Local-first identity is currently a self-signed Ed25519 JWT. Stable user id is
  `UUIDv5(KEY_NAMESPACE, public key bytes)` from a 32-byte secret-derived signing
  key. Source: `crates/jazz-tools/src/identity.rs`.
- Self-signed identity proofs have bounded validation: known issuers
  `urn:jazz:local-first` / `urn:jazz:anonymous`, `alg=EdDSA`, expected audience,
  `sub` equal to derived user id, 60s `iat` skew, future `exp`, and max TTL
  default 3600s. Source: `crates/jazz-tools/src/identity.rs`.
- Auth mode is policy input: `external`, `local-first`, `anonymous`. Session refs
  accept snake/camel paths for `user_id` and `auth_mode`. Sources:
  `crates/jazz-tools/src/query_manager/session.rs`,
  `packages/jazz-tools/src/runtime/client-session.ts`.
- A live client must not hot-swap principals. Token/cookie refresh can update
  auth state only when `user_id` stays the same; auth loss preserves last-known
  session plus error. Source:
  `packages/jazz-tools/src/runtime/auth-state.test.ts`.
- Hybrid auth has identity-continuity semantics: sign-up can bind Better Auth to
  the existing local-first Jazz identity with proof/verification so existing
  rows remain owned by the same principal. Cross-device pre-signup local data
  may become inaccessible. Sources: hybrid starter READMEs.
- Request/session resolution priority is backend impersonation, then JWT, then
  no session. Admin secret is separate and used for schema/policy sync, not
  normal user identity. Sources:
  `crates/jazz-tools/src/middleware/auth.rs`,
  `crates/jazz-tools/tests/auth_test.rs`.
- Backend permission identity and row authorship attribution are distinct:
  backend authority can stamp `$createdBy`/`$updatedBy` as a specific user.
  Source:
  `crates/jazz-tools/tests/policies_integration/authorship_policies.rs`.
- External JWT verification includes JWKS TTL, forced refresh cooldown,
  stale-if-error window, unknown `kid` refresh, and fail-closed expired/bad
  signatures. Source: `crates/jazz-tools/tests/auth_test.rs`.
- Permissions are authored separately in `permissions.ts`. Enforcing runtimes
  fail closed for missing explicit policy; current local structural-only mode is
  permissive but the new design likely removes that as a separate mode. Sources:
  `specs/status-quo/schema_files.md`, `specs/status-quo/query_manager.md`.
- Policy language supports comparisons, session refs, null checks, contains,
  `in`, exists, relation exists, forward/reverse inheritance, recursion, and
  boolean composition. Source: `packages/jazz-tools/src/schema.ts`.
- Invite links can be modeled as claim-mediated pre-membership read
  authorization; insert still requires membership rows. Source:
  `examples/chat-react/permissions.ts`.
- Group/team permissions are relation-shaped rather than special built-ins:
  user-team edges, team-team edges, resource access edges, grant roles, and
  recursive reachable-team policies. Sources:
  `packages/jazz-tools/src/permissions/index.test.ts`,
  `crates/jazz-tools/tests/policies_integration/recursive_policies.rs`.
- Related-row mutations that grant/revoke access must produce query/subscription
  deltas for affected users. Some current recursive/ExistsRel cases are marked
  known failing, but the intended invariant is important. Sources:
  `crates/jazz-tools/tests/policies_integration/complex_policies.rs`,
  `crates/jazz-tools/tests/policies_integration/recursive_policies.rs`.

### Crypto, Privacy, and Encrypted Columns

- Implemented crypto today is mostly identity secret lifecycle and passkey
  backup, not row/column E2EE. The new spec’s per-column E2EE section is mostly
  aspirational relative to current code.
- Local-first auth secrets are exactly 32 random bytes encoded base64url and
  generated with platform CSPRNG. Source:
  `packages/jazz-tools/src/runtime/auth-secret-store.ts`.
- Browser auth secret storage is local and namespaceable by app/user/session;
  default storage key is `jazz-auth-secret`. First-visit creation is explicitly
  not atomic across tabs. Source:
  `packages/jazz-tools/src/runtime/auth-secret-store.test.ts`.
- Passkey backup uses auth secret as WebAuthn `user.id` / restored
  `userHandle`, requires exactly 32 bytes, platform authenticator, resident key,
  user verification, credential protection, non-silent restore, and UP+UV flags.
  Source: `packages/jazz-tools/src/runtime/passkey-backup.test.ts`.

### Branching, Time Travel, and Historical Views

- Current schema manager composes environment, schema hash, and user branch into
  branch names such as `{env}-{schemaHash8}-{userBranch}`. New design should
  separate product branch semantics from this status-quo encoding. Source:
  `specs/status-quo/schema_manager.md`.
- Branch-aware visible entries and row histories are already core to status quo,
  but product-visible Git-like branch APIs are still sparse. Sources:
  `crates/jazz-tools/src/query_manager/manager_tests/branches.rs`,
  `specs/todo/c_later/branching_snapshots.md`.
- Arbitrary point-in-time/per-object historical queries appear in todo/future
  plans and should stay explicit as slower-but-important product goals. Sources:
  `specs/todo/b_launch/per_object_time_travel.md`,
  `specs/todo/c_later/point_in_time_queries.md`.

### Files, Blobs, Media, and Upload/Serving

- Files are visible today as row-modeled assets, not just a future blob
  subsystem. Examples use conventional `files` / `file_parts` tables,
  `db.loadFileAsBlob`, inherited read policy from parent rows, and attachments
  without a separate asset server. Sources:
  `examples/chat-react/README.md`, `examples/wequencer/README.md`,
  `examples/file-upload-react/README.md`.
- File/blob permissions lean on reverse-reference inheritance and child-to-parent
  access. Source: `examples/chat-react/permissions.ts`.
- Launch-plan docs mention file storage cascade integration, image/file serving,
  upload limits, and mutable files/smart chunking; these should be sparse
  product placeholders unless explicitly out of scope. Sources:
  `specs/todo/b_launch/file_storage_cascade_integration.md`,
  `specs/todo/b_launch/image_and_file_serving.md`,
  `specs/todo/b_launch/upload_limits_and_rules.md`,
  `specs/todo/c_later/mutable_files_and_smart_chunking.md`.

### Runtime Adapters, Browser Workers, React Native, and Server Contexts

- Browser default topology is main-thread in-memory runtime plus durable OPFS
  worker runtime. Memory mode skips the worker and is useful for tests/demos.
  Source: `specs/status-quo/browser_adapters.md`.
- WorkerBridge is a trusted peer using sync-shaped concepts over `postMessage`,
  not merely a storage adapter. It forwards sync payloads, auth/session state,
  shutdown/crash simulation, and readiness. Sources:
  `packages/jazz-tools/src/runtime/worker-bridge.ts`,
  `packages/jazz-tools/tests/browser/worker-bridge.test.ts`.
- Auth token refresh can recover from auth loss and flush queued local writes;
  clearing JWT forwards `null` while preserving admin/backend secrets. Sources:
  `packages/jazz-tools/tests/browser/db.auth-refresh.test.ts`,
  `packages/jazz-tools/src/runtime/client.test.ts`.
- React/Vue/Svelte adapters should stay thin: core owns reactivity and
  subscription semantics; adapters integrate idiomatically. Source:
  `specs/status-quo/ts_client.md`.

### Developer Tooling, Codegen, CLI, Inspector, and MCP

- CLI schema workflows include hash generation, permission warnings, migrations,
  schema export/import by hash, custom migrations dirs, CJS-compiled migration
  loading, and admin migration push. Source:
  `packages/jazz-tools/src/cli.test.ts`.
- Permission-only changes do not create structural schema hashes or require
  migrations in current CLI behavior. Source:
  `packages/jazz-tools/src/cli.test.ts`.
- CLI warns when delete policy is omitted; delete can fall back to `update.using`
  at runtime but explicit delete policy is recommended. Source:
  `packages/jazz-tools/src/cli.test.ts`.
- Schema watcher/dev-server behavior is part of the DX product and likely needs
  at least a placeholder. Sources:
  `packages/jazz-tools/src/dev/schema-watcher.test.ts`,
  `packages/jazz-tools/src/dev/dev-server.test.ts`.
- MCP exists as a product/tooling surface with SQLite and naive backends. Source:
  `packages/jazz-tools/src/mcp/`.

### Performance, Storage Layout, and Operational Constraints

- Current status quo is index-first. `_id` acts as an all-rows manifest; queries
  start from persisted index scans and materialize only surviving candidates.
  Source: `specs/status-quo/query_manager.md`.
- Storage currently assumes synchronous operations. Browser achieves durable sync
  storage by putting OPFS sync access in a dedicated worker. Source:
  `specs/status-quo/storage.md`.
- Current row storage has moved toward raw table instances with uniform headers,
  row locators, exact visible/history table locators, and shared `row_format`.
  This is status-quo detail, but it contains useful constraints for the SQLite
  lowering. Source: `specs/status-quo/storage.md`.
- Branch ordinal registry is one durable row to avoid torn bidirectional mapping
  state after crashes in backends without cross-call atomic multi-put. Source:
  `specs/status-quo/storage.md`.
- Todo/issues mention storage compression, row-storage common-case encoding,
  text-encoded enum overhead, oversized visible row storage, verbose batch
  payloads, and memory profiling accuracy. Sources:
  `specs/todo/b_launch/storage_compression_strategy.md`,
  `specs/todo/issues/row-storage-common-case-encoding.md`,
  `specs/todo/issues/text-encoded-storage-enums.md`,
  `specs/todo/issues/oversized-visible-row-storage.md`,
  `specs/todo/issues/verbose-batch-payloads.md`,
  `specs/todo/b_launch/memory_profiling_accuracy.md`.

### Current Todo / Future-Plan Pressure

- MVP/launch/later todo docs include explicit indices, lens hardening, optimistic
  update DX, storage limits/eviction, sync protocol reliability, globally
  consistent transactions, edge transaction authorities, scope-based
  contraction, sharding, protocol/storage version tags, policy/schema change
  lockstep, auth integrations, data export/external sync, dev dashboard/billing,
  additional language bindings, webhooks/SQL-over-HTTP style surfaces, and
  serverless KV adapters. Sources: `specs/todo/**`.
- Export/restore/data pipelines are mostly userland or admin snapshotting per
  prior discussion, but current todo docs still show product demand. Sources:
  `specs/todo/b_launch/data_export_external_sync.md`.
- Current known issues document useful negative requirements: stale cache after
  scope removal, reconnect outbox dedup, worker upstream-connected optimism,
  policy error reasons, update/forward inherits bugs, and session UUID mismatch.
  Sources: `specs/todo/issues/*.md`.

### Potential Spec Mismatches or Tensions

- New spec removes current structural-schema-only permissive runtime as a product
  concept; current docs/tests still describe `PermissiveLocal`. Need clarify
  replacement as ordinary admin-token/no-upstream Jazz instance.
- New spec wants `j_` system row prefix while current code/docs use `_jazz_*`.
  Need make user-column escaping and migration compatibility explicit.
- New API wants no batch terminology, but current tests/docs and persisted table
  names are batch-shaped. Need classify what is status-quo-only versus
  transaction contract.
- Current hard-delete/truncate semantics may conflict with “preserve history” as
  a core Jazz value. Need decide whether hard delete remains product-visible,
  admin-only, or status-quo-only.
- Current visible-entry sidecars encode tier previews and synthetic merge
  provenance; new SQLite design currently prefers queryable history plus main
  current projection. Need decide which preview/provenance UX must survive.
- Current row parents exist in status quo; new design leans on read/write sets
  instead of explicit parents. Need ensure sync/convergence/conflict tests cover
  the semantics that parents used to provide.
- Current permission evaluation has known intended-but-failing recursive/ExistsRel
  reactivity cases. New spec should treat the intended behavior as requirement
  and mark implementation risk.
- Current files are row-modeled; new blob design may move bytes out of row
  history. Need preserve permission/sync behavior while changing mechanics.
- Current local-first auth identities are concrete Ed25519/JWT/UUIDv5. New spec
  may want abstract principals, but product compatibility likely requires this
  derivation at least for migration/interoperability.

### Follow-Up Searches

- Schema/co-value/list/reference/migration source and tests.
- Explicit index and query-planner tests.
- Current permission language edge cases and known bugs.
- Browser worker and multi-tab lifecycle invariants.
- Storage isolation, reconnect, and auth refresh tests.
- File upload and serving semantics.
