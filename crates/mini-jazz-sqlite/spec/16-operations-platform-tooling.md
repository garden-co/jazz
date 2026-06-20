# Operations, Platform, And Tooling

## 27. Security, Privacy, And Encryption

Query-scoped sync can leak information if scope is over-approximated across
authorization boundaries. Enforcing runtimes must evaluate policy before sending
bundles to untrusted clients.

Rejected transactions and history rows remain stored. Implementations must
consider whether rejection reasons or rejected row values are safe to sync.

Per-column end-to-end encryption is the long-term encryption model. Table-level
or row-level E2EE are not the primary target.

Confidentiality classes may include:

- server-readable
- client-decrypted
- encrypted but indexable
- opaque blob

Server-enforced policies must not depend on client-only encrypted fields unless
the field is explicitly server-readable or has a defined index/proof mechanism.

Server-readable values can participate in server-side policy, indexes,
predicates, ordering, sync scope, and authority validation.

Client-decrypted values are stored and synced as opaque encrypted bytes. They
can be queried after local decryption, but an untrusted server or edge cannot
filter, sort, index, or enforce policy over their plaintext.

Sync facts themselves can leak information. Predicate/range/absence facts,
policy dependencies, rejection reasons, and conflict metadata may reveal
information even when row values are encrypted. Future protocols may need
opaque or summarized facts; v0 may send full facts where policy allows.

File content digests should be treated as privacy-sensitive because they leak
equality across branches, users, or sessions.

Conflict metadata for encrypted fields should mark opaque encrypted blobs as
conflicting without exposing plaintext candidate values.

Generated indexes must declare what they leak. They should require columns to be
server-readable or explicitly indexable-encrypted.

Open issues:

- confidentiality metadata syntax in `schema.ts`
- key management and sharing
- encrypted index/proof mechanisms
- policy compiler diagnostics
- encrypted file digest strategy

## 28. Data Export, Backup, And External Sync

Export, ingest, and external connectors are userland patterns, not core
database semantics.

Ordinary user export should be expressible as normal policy-filtered queries,
optionally with userland expansion for includes, files, or history.

Operational backup and disaster recovery are admin-only and likely expressed
through embedded database snapshotting/restoring plus blob storage backup. This
is distinct from product-level row restore/undelete, which is an append-only
write that reuses insert authorization semantics.

External connectors should be built above the core as application or service
code. They may write Jazz transactions using service/admin sessions, source
branches, or application tables, but the core does not prescribe connector
semantics.

Open issues:

- operational backup format for SQLite/native/browser storage
- hosted convenience export APIs built from normal queries

## 29. Platform Bindings And Packaging

Rust is the semantic source of truth for query execution, transactions, sync,
subscriptions, policy evaluation, catalogue application, conflict metadata, and
tiered delivery.

TypeScript and framework packages provide schema/query DSLs, generated types,
tooling integration, and idiomatic UI bindings over those semantics.

Bindings must agree on:

- row and result semantics
- idiomatic value conversion semantics
- transaction modes, outcomes, and durability receipts
- subscription diff semantics
- tiered query delivery semantics
- policy/session semantics
- branch/source selection
- schema/catalogue/lens interpretation
- conflict metadata shape
- error/rejection shape

Binding APIs may be idiomatic, but they must remain thin over the core semantic
operations. A binding can wrap query descriptors in a fluent DSL, generated
types, callbacks, promises, or framework hooks; it should not introduce
host-specific query helpers that cannot be expressed as the same core query
descriptor. The cross-binding contract is limited to descriptors supported by
the core query language.

Higher-level language bindings should expose idiomatic values while preserving
the same database semantics. TypeScript/JavaScript bindings are the concrete
compatibility example: `BYTEA` values become `Uint8Array`, timestamps become
`Date` values at the JS boundary, JSON payloads parse into JS values, provenance
timestamps use JS millisecond conventions, and invalid JSON/date/bytea/enum
values fail loudly rather than silently coercing. Other language bindings should
choose idiomatic equivalents while keeping validation and round-trip behavior
explicit.

Transformed columns may expose transformed read/write types at the product
boundary, but query predicates such as `where` operate over the raw stored
semantic type unless a transformed-query contract is explicitly specified.

Generated row/result layout must be stable. Runtime row alignment follows
declared schema order plus requested includes and subscription deltas, so typed
bindings and generated clients can decode results without depending on
incidental SQL column order.

Framework integrations should be thin adapters over the same reactive Jazz
client. Jazz's reactive machinery lives in the core/client runtime.

Platform storage choices remain binding-specific:

- browser durable mode: SQLite WASM plus browser storage such as OPFS where
  available
- Node/NAPI and server runtimes: native SQLite through Rust
- React Native/native mobile: native SQLite integration
- edge/global authority runtimes: native Rust SQLite or another embedded
  database behind the same lowering contract

Package boundaries are implementation guidance, not product semantics. The
current Jazz package model is a reasonable starting point.

Open issues:

- SQLite WASM binary size and startup budget
- OPFS availability and fallback behavior
- SharedWorker/tab-broker support
- React Native SQLite packaging
- NAPI/native distribution
- generated TypeScript types and Rust catalogue codec lockstep

## 30. Developer Tooling, Admin Workflow, And Inspector

Developer tooling is a product surface. The exact CLI and package names may
evolve, but the workflow invariants should remain:

- project creation scaffolds schema, permissions, migrations, app id, and local
  development configuration
- dev plugins/watchers compile schema and permissions, detect catalogue changes,
  and surface diagnostics without requiring application restarts where possible
- schema/catalogue tooling computes stable hashes/revisions, validates
  `schema.ts` and `permissions.ts` together, and fails closed on missing
  explicit permissions
- migration tooling generates reviewed stubs, loads custom migration
  directories, supports schema import/export by hash, and publishes catalogue
  heads through an admin-controlled workflow
- permission-only changes do not require structural storage migrations
- tooling warns when delete policy is omitted and the runtime would fall back to
  update semantics
- admin/server configuration and secrets are separate from normal user identity
  and row authorship

Inspector/devtools are product surfaces too. They should connect by app/server
identity, environment, admin/service credentials, and branch/view context, and
should inspect catalogue state, generated storage layout, policies, indexes,
sync state, transactions, query scopes, and branch/history state without
exposing private physical ids as public API.

MCP, SQL-over-HTTP, webhooks, and additional language bindings are integration
surfaces above the same semantic core. They should not define separate database
semantics.

Open issues:

- exact CLI/package names and command surface
- inspector permission model and redaction
- generated type/codegen lockstep across Rust and TypeScript
- dev dashboard, billing, and hosted operational workflows
