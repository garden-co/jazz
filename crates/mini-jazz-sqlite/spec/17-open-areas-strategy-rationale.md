# Open Areas, Strategy, And Rationale

## 31. Undefined Areas

The following areas remain intentionally underspecified:

- transaction outcome/receipt encoding
- compact dotted vector encoding
- local-to-global vector upgrade broadcast
- predicate/range scope closure
- query-scope repair candidate bounding
- query descriptor non-persistence plus explicit resubscribe/query-settlement
  protocol
- full hop/gather query lowering and product constraints
- active query-descriptor replay across reconnects and upstream restarts
- retained-data cache eviction for rows no longer covered by active queries
- authority validation over large read sets
- exclusive upsert semantics over existing rows
- multi-base branch conflict semantics
- branch provenance encoding
- policy language and recursive policy bounds
- recursive policy lowering performance and diagnostics
- full schema lens semantics
- reconnect summaries
- subscription settlement and reconnection protocol
- negotiated upload in-flight capacity below the initial `1000` default
- server-durable uploaded-transaction inbox/interest state, if reconnect replay
  from the client queue is not sufficient for a future topology
- unordered transport recovery below the upload protocol; current intended
  transports provide ordered delivery
- final read-set wire shape for uploaded exclusive transactions
- true local-only transaction mode that deliberately skips upstream upload
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

## 33. Optimization Recommendations And Performance Baselines

This section is non-normative. It records performance evidence and recommended
optimization priorities from the current SQLite-core prototype. These numbers
are not semantic guarantees, but they are design pressure and regression
context.

### 33.1 Primary Fast Path: Bounded Policy-Scoped Pages

The primary fast path should be indexed, policy-scoped page reads over large
shared relations:

- user or policy predicate
- stable ordering
- small page result
- query-scoped sync
- full topology propagation
- local subscription diffing

Observed baseline in the current prototype:

- `100k` documents, `10k` visible to the user, page `50`: full-topology
  first result in roughly `33 ms`
- `200k` documents, `20k` visible to the user, page `50`: full-topology
  first result in roughly `63 ms`
- client-side apply, query, and subscription poll work usually remain small for
  bounded pages; export and read-set/policy-dependency collection dominate

The target product shape is not whole-table replication. Bundle size and
client work should scale with observed rows and required dependencies, not with
the full source table.

For ordered pages, observed page-boundary state is part of the optimization
model. Refresh should be bounded by the new visible page, previously observed
page rows that may need repair or removal, dependencies, and metadata. It
should not widen to every row matching the page predicate merely because the
ordered scope is large.

### 33.2 Export And Read-Set Collection

SQLite query planning solves local result selection, but Jazz export has a
larger job. A query-scoped export must collect:

- visible result rows
- dependency/include rows
- policy dependencies
- read-set and observed-fact records
- transaction metadata and outcomes
- repair rows and tombstones needed for scope contraction
- branch/base/source provenance when applicable

Export/read-set collection is therefore a first-class hot path. Implementations
should optimize it as carefully as SQL query execution. In the current
prototype, policy-dependency history collection is often the dominant part of a
bounded page export even when the actual SQLite current read is sub-millisecond.

Same-shape query descriptors for one downstream peer should be batchable before
bundle assembly. Dashboards often contain many similar page queries that share
table, policy, branch, ordering, and dependency structure. Batching at export
time can avoid repeated dependency, read-set, transaction, and branch
collection that cannot be recovered by merging already-assembled bundles.
The current prototype batches compatible ordinary predicates, ordered page
descriptors, and recursive ref descriptors into one refresh bundle per
compatible group. Ordered page reads over the main-branch current projection
also lower compatible bound values into one SQL statement using a values table
and per-value window ranking. Branch overlays currently keep the conservative
per-value read path because sparse-overlay precedence is more subtle.

### 33.3 Recursive Scopes

Recursive queries and recursive policies are product-relevant and first-class.
They also produce the most expensive observed workload shapes.

Broad recursive subscriptions are viable, but expensive through a full
topology. At `10k` tree nodes, the current prototype showed full-scope export,
apply, re-export, and subscription polling costs on every hop. Repeated no-op
refresh over a broad recursive scope is especially wasteful: the system can
spend significant time re-exporting, re-applying, and re-diffing in order to
discover no semantic change.

The mechanism for optimizing no-op or near-no-op refresh is intentionally open.
Candidates include narrower invalidation facts, observed-set versioning,
dependency fingerprints, or more precise per-query change clocks. The spec
should not choose one prematurely.

Materialized transitive closure tables or other recursive derived indexes are a
future optimization candidate. Early measurements show much faster recursive
reads at meaningful storage and write-maintenance cost. They are not part of
the baseline storage contract.

### 33.4 Current Projection

Main-branch current projection is recommended for hot current reads. It costs
disk space, but keeps ordinary current reads predictable and indexable.

The prototype's current-projection tradeoff benchmark showed that history-only
reads can be surprisingly competitive for small synthetic cases, but the main
product workload is repeated current reads over bounded, indexed pages. The
recommended baseline remains:

- maintain main current projection tables
- optimize current projection indexes first
- allow slower query-time visibility for arbitrary historical snapshots and
  pinned branch-base reads until a derived projection is justified

### 33.5 Topology

Benchmarking isolated core queries is not enough. Jazz performance is
product-perceived through a topology:

```text
memory tab -> durable worker/broker -> edge -> core/global authority
```

Memory-only runtimes should still use in-memory SQLite. Durability is a storage
mode, not a different semantic runtime. Product-shaped benchmarks should keep
measuring full-topology latency, including intermediary export/apply and local
subscription diffing.

Current measurements suggest durable intermediaries are acceptable for bounded
page queries: the file-backed edge/worker path added about `1 ms` compared with
all-memory intermediaries in the primary page-flow probe. This is encouraging
for browser-worker and cloud-edge deployments, but broad recursive scopes still
make every hop visible.

### 33.6 Subscriptions And Diffs

Subscription performance should be measured at the semantic callback boundary.
The observed update path includes incoming apply, local rerun/diff or poll/diff
work, and deterministic diff production.

Semantic diff categories include `added`, `updated`, `moved`, and `removed`.
`moved` is important for ordered pages because an order-only change is visible
to the product even when row values are otherwise unchanged.

The current bounded page probes show local diffing is small for page-sized
results. Broad recursive subscriptions remain the stress case.

Incoming sync application should remain idempotent as both a correctness and
performance invariant. Reapplying already-known history should be cheap enough
for reconnect and broad refresh paths, especially while refresh bundles may
include repair rows or previously observed recursive-scope rows.

### 33.7 Query Lowering

Supported indexable current-query forms should lower to SQL over current
projection tables. Fallback filtering over all visible rows should be explicit
optimization debt, not an accidental implementation shortcut.

Current-query lowerings that should stay covered include:

- equality predicates
- `IN` predicates
- selected semantic system-field predicates
- ordered top-N/page queries
- declared user-field indexes
- ref predicates through physical row surrogates

Slower fallback paths are acceptable for historical pinned-base snapshots,
arbitrary time-travel reads, and other query-time visibility baselines, but they
should be named in benchmarks and revisited when they become product-hot.

Index order should be generated from the query being served, not from a blanket
policy-first rule. Early measurements showed that prefixing ordinary
user-declared current indexes with policy columns can badly regress
owner/page-style queries, while only modestly helping some recursive policy
cases. Policy-specific acceleration should be explicit, targeted, and checked
against the SQL plan it is meant to serve.

Local integer interning for system user fields is a recommended storage
optimization. Public user ids still exist at API, auth, and sync boundaries,
and ordinary app user fields remain ordinary schema fields. The storage layer
may use local integer surrogates for row system metadata such as creator/updater
to reduce repeated long-id footprint and keep policy/query lowering compact.

SQLite tuning knobs are secondary to query/export/apply mechanics. Default page
size is a reasonable baseline; larger pages did not clearly improve the tested
workloads. Larger SQLite page caches may help some file-backed broad refreshes,
but do not fix CPU-bound recursive refresh. WAL and synchronous settings are
deployment choices rather than semantic requirements. Compression should remain
stream-level for transport and page/range-level for future storage work, not
per-row payload compression by default.

### 33.8 Benchmark Families

The benchmark suite should remain product-shaped and topology-aware. The
current important families are:

- large owner-scoped page over a shared table
- permissioned dashboard with many page queries
- dashboard query-count scaling
- recursive tree subscription
- recursive full-topology propagation
- recursive closure-table comparison
- cold reopen of durable intermediaries
- project-board app shape
- mixed mutation refresh with semantic page diffs
- subscription storm
- branch sparse overlay
- pinned branch snapshot
- branch fan-in/source traversal
- storage topology comparison
- current projection versus history-only tradeoff
- transaction granularity
- multi-tenant many-user pages
- wide-schema/narrow-sync path

Detailed scenario descriptions and numeric runs belong in benchmark reports,
PR descriptions, and decision logs rather than in the normative spec. The spec
should capture the product-shaped design pressure and the optimization
recommendations that survive those experiments.

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
- Fate and receipt merge semantics must be monotonic under duplicate/stale
  direct acceptance and sync replay; otherwise old bundles can move a
  transaction backward after the authority has enriched it.
- Observable ordering must be semantic. Physical SQLite row numbers are useful
  locally, but leaking them into default query order creates cross-replica
  divergence when bundles are applied in different orders.
- Query descriptor lifetime is a protocol concern. The prototype showed that
  simply making descriptor tables temporary breaks reconnect repair when stale
  cached facts survive restart. The durable product contract should be active
  descriptor replay plus settled-query repair, not persisted query interest as
  user data.
- Create/update intent should be explicit at the API boundary. Treating
  `insert` as an accidental update hid important product semantics; the current
  shape is create-only `insert`, explicit `update`, and explicit `upsert`.
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
  rather than persisted as durable query state. A storage-only switch to
  connection-local descriptors is not enough, because retained cache facts and
  active query truth need an explicit settlement/resubscribe boundary.
- Mergeable upsert is now product-shaped for create, update, sync, and
  restore-after-delete. Exclusive upsert over an existing row is still a real
  semantic gap: it needs either expected-version/read-set requirements or a
  deliberately specified globally ordered update rule.
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
