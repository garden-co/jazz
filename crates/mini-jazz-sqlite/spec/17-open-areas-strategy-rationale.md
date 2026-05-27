# Open Areas, Strategy, And Rationale

## 31. Undefined Areas

The following areas remain intentionally underspecified:

- transaction outcome/receipt encoding
- compact dotted vector encoding
- local-to-global vector upgrade broadcast
- predicate/range scope closure
- query-scope repair candidate bounding
- full hop/gather query lowering and product constraints
- active query-descriptor replay across reconnects and upstream restarts
- retained-data cache eviction for rows no longer covered by active queries
- authority validation over large read sets
- multi-base branch conflict semantics
- branch provenance encoding
- policy language and recursive policy bounds
- recursive policy lowering performance and diagnostics
- full schema lens semantics
- reconnect summaries
- subscription settlement and reconnection protocol
- hot branch projection heuristics
- audit-grade append-only receipt history
- hard-delete/truncate authorization, sync, and retention semantics
- garbage collection and compaction
- benchmark thresholds for launch readiness
- representative adopter-shaped benchmark datasets

## 32. Performance Research Discipline

The embedded-database direction is justified only if it improves realistic Jazz
workloads while preserving local-first, policy, history, branch, lens, and sync
semantics. Performance work should therefore be scenario-shaped and tied to
specific design choices. Microbenchmarks are useful when they falsify or support
a concrete lowering decision, not as isolated numbers.

Initial performance results should be comparative rather than hard-coded target
thresholds. Compare candidate designs against each other and, where possible,
against current Jazz behavior on realistic app-shaped data.

Required benchmark families:

- **Current app reads**
  - equality and ref filters
  - ordered pages by `$createdAt` / `$updatedAt`
  - includes
  - policy filters
  - recursive queries
  - aggregate `COUNT`
- **Writes and projection maintenance**
  - mergeable insert/update/delete
  - create-only insert and explicit upsert
  - multi-row transactions
  - empty explicit transactions as no-ops
  - duplicate same-row mutations normalized to final semantic state
  - patch updates preserving omitted fields
  - writes with policy dependencies
  - conflict-producing concurrent writes
- **Sync export/apply**
  - query-scoped export
  - refresh after rows leave scope
  - recursive query export
  - policy dependency export
  - branch-scoped export
  - duplicate/reordered bundle apply
- **Authority validation**
  - exclusive row reads
  - absence reads
  - predicate/range reads
  - policy-dependent writes
  - branch-source reads
  - cross-schema read/write-set translation
- **Branch/history**
  - pinned branch reads
  - transitive source graph reads
  - conflict candidate retrieval
  - historical snapshot by global epoch
  - full-system snapshot export by global epoch
  - historical "as of wall-clock time" queries
- **Browser topology**
  - cold open
  - worker startup
  - main-thread active-scope rehydration
  - reconnect after offline
  - multiple tabs through one broker

Measurements should include:

- p50/p95 latency where applicable
- query plan and `EXPLAIN QUERY PLAN`
- rows scanned and rows returned
- history/current/index bytes written
- transaction, read/write-set, query-fact, branch, and catalogue metadata bytes
- raw user payload bytes versus total SQLite bytes
- SQLite page count and page-size configuration
- compression savings for page/range compression experiments
- bundle bytes before transport stream compression
- memory high-water mark where available
- write amplification from generated indexes
- projection rebuild and repair time
- authority validation scanned rows/facts
- startup and time-to-first-usable-query for browser topology

Each benchmark record should include:

- SQLite version and build flags
- storage backend and platform
- schema/catalogue revision
- row count, version count, branch/source graph shape, and policy shape
- indexes present
- data generator seed
- topology and message schedule seed for distributed scenarios

Comparative success criteria:

- common one-shot reads should beat or clearly match current Jazz under
  realistic data
- query-scoped reconnect should avoid whole-table sync for ordinary screens
- write throughput should remain acceptable with automatic indexes enabled
- current projection disk overhead should be explainable and bounded
- history overhead should benefit from page/range compression and locality
- authority validation should scale with read/write-set size and indexed facts,
  not whole-table scans
- multi-tab browser topology should not multiply durable cache memory per tab

Open issues:

- representative adopter-shaped benchmark datasets
- exact current-Jazz baseline scenarios
- memory measurement APIs across native, WASM, and browser storage
- benchmark thresholds for launch readiness
- how to retain query-plan regressions in CI without excessive noise

## Appendix A: Working Prototype Status And Strategy

The SQLite core spike is no longer throwaway. It should remain the working
prototype while the design evolves through collaborative improvements,
clarifications, review comments, and focused experiments. There is no single
planned "next attempt"; the prototype and spec should move together as hard
questions get answered.

All prototype stores should use SQLite, including memory-only stores. In-memory
means in-memory SQLite, not a parallel fake implementation. This keeps storage
boundaries honest across local tests, browser-like topologies, edge replicas,
and global authority replicas.

The implementation should organize around data artifacts and verbs rather than
manager objects.

Core artifacts:

- `SchemaDef`
- `CatalogueRevision`
- `PhysicalLayout`
- `IdCodec`
- `EnumCodec`
- `TablePlan`
- `ProjectionPlan`
- `VisibilityPlan`
- `QueryPlan`
- `ObservedFacts`
- `ReadSet`
- `WriteSet`
- `SyncBundle`
- `Effect`

Core verbs:

- `lower_schema`
- `open_store`
- `apply_local_write`
- `run_query`
- `export_scope`
- `apply_bundle`
- `validate_at_authority`
- `repair_projection`
- `poll_subscription`

Suggested slices:

1. physical layout, id codec, enum codec, and DDL
2. local write/query/current projection
3. deterministic projection rebuild
4. observed facts and query scope
5. subscriptions
6. sync export/apply
7. authority validation
8. branch visibility
9. historical snapshots
10. conflict candidates
11. narrow but real policies
12. narrow but real lenses

Implemented slices so far:

- SQLite-backed in-memory and file-backed runtimes
- schema-driven DDL for narrow structural schemas
- local writes, generic transactions, updates, deletes, and current projection
- deterministic projection rebuild from history and transaction fate
- public ids with local physical surrogates
- transaction fate, edge/global receipts, rejection repair, and idempotent sync
- query-scoped sync bundles using public ids
- branch metadata, branch-local writes, pinned main base snapshots, and sparse
  overlays
- branch provenance sync for simple branch sources
- equality query lowering with predicate observed facts
- query-scope repair for rows that leave equality predicates via update or
  delete
- one-shot subscriptions via rerun-and-diff semantic row diffs
- narrow read/write policies, including ref-readable policies
- transitive policy read-set recording for recursive write policies
- trusted edge validation of untrusted bundles
- recursive query reads over self refs
- recursive query-scope export of deleted subtrees
- cycle rejection and bounded acyclic recursive policy lowering
- narrow schema lenses for renamed fields and refs
- system-column prefix escaping

Tests should be product-shaped integration tests using projects, todos, Alice,
Bob, and a core authority.

The full distributed system harness should support memory-only topologies using
in-memory SQLite so tests can run several local/edge/global runtimes without
browser-specific APIs. It should also support durable SQLite-file nodes in the
same topology so crash safety and reconciliation can be tested.

Performance tests should follow Section 32: scenario-shaped, comparative, and
tied to falsifying concrete layout, lowering, sync, and topology choices.

Ongoing work should bias toward whole-system tests over narrow helper tests. The
goal is to learn whether the semantic model composes under realistic distributed
conditions, not only whether individual SQL statements work.

Recommended harness shape:

- create several SQLite-backed runtimes in one process
- mix in-memory SQLite nodes and durable SQLite-file nodes
- model multiple in-memory browser-tab nodes connected to one durable
  worker/tab-broker node
- assign each runtime a node id, user/session, catalogue revision, and
  optional upstream peer
- support local, edge, and global roles
- allow explicit message passing rather than hidden synchronous replication
- allow dropped, delayed, duplicated, and reordered bundles
- simulate asynchronous systems deterministically by making node progress,
  network progress, and disk progress explicit scheduler choices
- expose query/subscription observations as testable events
- expose transaction outcomes, receipts, observed facts, and projection diffs
- provide deterministic clocks/epochs for repeatable tests
- support crash/reopen of durable nodes
- support disconnect/reconnect and replay-window/full-snapshot recovery

The first harness should be boring and explicit. It does not need production
transport, threads, or browser APIs. It does need SQLite from the start, clean
boundaries between runtime, storage, sync, policy, and query planning, and
enough topology to prove that local replicas, trusted peers/edges, and the
global authority keep the same invariants when messages move in uncomfortable
orders. Property-style tests should randomize the next progress step among
specific node progress, network delivery, and disk/reopen progress while keeping
the run deterministic and reproducible from a seed.

The harness should mirror browser-plus-cloud product topology early:

- multiple browser main-thread-like in-memory SQLite tab runtimes
- one shared browser worker/tab-broker-like durable SQLite runtime
- optional edge SQLite runtime
- global authority SQLite runtime

The current Rust harness includes reusable trusted-edge and trusted-mesh
topologies with client and trusted-peer nodes, in-memory and durable trusted
edge variants, and helper sync paths for trusted apply, untrusted apply,
user-scoped untrusted apply, and exclusive forwarding. The remaining harness
gap is an explicit scheduler for delayed, duplicated, dropped, and reordered
message delivery.

The working prototype should keep policies and lenses in scope. The goal is to
prove that the whole system composes, not to defer the two features most likely
to change scope, query planning, validation, and sync.

Implementation lessons from the prototype:

- The useful architecture is verb-shaped: write, validate, apply, export,
  repair, read, subscribe. Thin data artifacts are useful, but manager-style
  abstractions should not become the design center.
- SQLite is a good semantic substrate for the prototype. Recursive CTEs,
  transactions, projection tables, and ordinary indexes are already carrying
  real Jazz semantics.
- Correctness depends on making read contexts explicit. The same logical policy
  must evaluate against main current, branch overlay, pinned base snapshot, or
  historical snapshot depending on the operation.
- Query-scoped sync needs repair semantics from the beginning. A bundle cannot
  merely export current result rows and hope the receiver removes stale rows.
- Read/write sets are becoming the bridge between policy, validation,
  replayability, causality, and future conflict explanation.
- Whole-system tests are more valuable than narrow helper tests for this design:
  most important bugs appeared only when branch snapshots, policies, sync, and
  query scopes were composed.

Known implementation tensions:

- Query-scope repair currently uses local history that ever matched a supported
  equality predicate. This is correct for the prototype but can over-export.
- Projection repair is intentionally broad in several incoming-sync paths.
- Recursive policy/query lowering works for narrow acyclic cases, but helper
  SQL is duplicated and needs consolidation.
- Exclusive transaction conflict handling is row-coarse for write conflicts, but
  versioned read/write-set validation now covers several row, absence, policy,
  branch-source, update, and delete cases. Predicate/range validation remains
  incomplete.
- Branch source/provenance now has executable transitive source graphs,
  source-depth precedence, source-list replay ordering, and conflict behavior,
  but product branch backing-row permissions and merge APIs remain incomplete.
- Active query descriptors now drive reconnect refresh and subscription
  recovery in the prototype. They should be replayed by downstream clients
  rather than persisted as durable query state.
- Lenses are currently field-level storage-name mappings for text/ref renames.
  There is no schema-versioned catalogue, inverse lens graph, compatibility
  graph, or copy-forward storage yet; physical table names are still
  `schema_v1`.
- The spec now prefers physical layouts keyed by structural storage shape rather
  than one table set per catalogue/schema version. The prototype still uses
  per-version column-history tables and should be used to compare that baseline
  against JSONB-style history payloads.
- Conflict candidates are exposed through side APIs and conflict-aware row
  reads; product conflict metadata shape and resolved-from-candidates
  provenance remain incomplete.
- Conflict merge execution should live in deterministic semantic merge code,
  with SQLite doing candidate retrieval. Rich text should be treated as an early
  blessed merge strategy rather than waiting for arbitrary app-defined merge
  code.
- Predicate facts now cover equality, contains, IN, not-equal, null-present,
  selected system fields, ordered pages, absence, and recursive refs in the
  prototype. Range predicates and a final compact predicate model remain open.
- Recursive query reads use two strategies: current projection can use recursive
  CTEs, while pinned branch snapshot reads may fall back to in-memory traversal
  over already visible rows. This is a correctness-first shortcut, not the final
  planner shape.
- Receipt representation is minimal. Receipt tiers and timestamps exist, but
  authority identity, signatures, and detailed receipt payload semantics remain
  open.
- Trusted/admin policy bypass exists in the harness, but audit/provenance
  semantics for bypassed writes are thin.

High-value things to try next:

- add a small `COUNT` aggregate primitive over versioned/lensed tables
- build an account-aggregator-shaped example that stresses aggregation, joins,
  policies, schema versions, sync scope, and subscriptions
- validate exclusive predicate/range read sets under writer-user policy
  context
- compare column-history with JSON/BLOB-history layouts
- test SQLite VFS/page or range compression for deep histories, including
  physical ordering that co-locates redundant history
- test transport stream compression over long-lived sync connections
- stress recursive queries and recursive policies beyond toy examples
- prove the blessed rich-text conflict-resolution path across sync/rebuild
- benchmark multi-version schema/lens union queries
- benchmark browser topology startup, memory, reconnect, and worker/main-thread
  mirroring costs
- replace remaining fixture-shaped APIs with generic process-shaped verbs when
  they block understanding or tests

## Appendix B: Rationale

Append-only history plus rebuildable projections handles rejection repair,
restart/rebuild, sync replay, and historical reads with one source of truth.

Outcome plus durability receipts is preferred over a single overloaded fate enum
because local pending, edge durability, global acceptance, and rejection are
different axes.

Local integer surrogates and integer enum discriminants are the physical
baseline because repeated text ids and string enums are expensive in hot rows.

Query-scoped sync is preferred over table replication because clients should
receive the history/facts needed for active queries, not unrelated table state.

Rerun-and-diff subscriptions are the correctness baseline because one-shot and
live query semantics stay aligned.

Most file/blob behavior is kept as a blob adapter contract because otherwise
the spec grows a second storage system beside the relational core.

## Appendix C: Future Revisits

Future work may revisit:

- fixed-width binary public ids
- append-only audit receipts
- hot branch projections
- indexed read/write-set side tables
- columnar history tables if JSONB-style payloads make policy, lenses,
  conflicts, or historical queries too slow or complex
- payload compression for special large metadata/blob cases
- custom SQLite VFS/page or range compression
- opaque policy proofs
- compact encrypted indexes
- query-scope repair via durable observed-fact indexes rather than broad
  "ever matched" scans
- consolidating snapshot/effective-branch SQL builders into one read-context
  lowering layer
