# Meeting Research Targets

Source: team review meeting on 2026-05-26. This document captures concrete
research targets, questions, and implementation pressure points raised in the
meeting. The transcript was produced by speech-to-text, so obvious transcription
errors are normalized here, for example "musculite" means SQLite.

The purpose of this file is collection and classification. It should be folded
into `SPEC.md` once each point has either become a requirement, an invariant, a
deferred optimization, or an explicit non-goal.

## Overall Team Read

- The SQLite-core direction was accepted as a promising way forward.
- The prototype code was perceived as significantly easier to understand than
  the current Jazz codebase.
- The team liked organizing the implementation by process/timing, for example
  read, write, and sync-message apply, instead of stacked manager objects.
- The current prototype is understood as incomplete and fixture-heavy, but the
  spec/tests/intended architecture are considered the important artifact.
- The working model is to use the spike PR as the base, leave comments there,
  and open small follow-up PRs on top.

## Spec And Collaboration

Research / work targets:

- Hand-restructure the spec so concepts build in a clearer order for humans who
  did not participate in the spike.
- Keep the spec comprehensive enough that LLM-assisted implementation can use
  it as the main context source.
- Keep unspecified areas explicitly marked instead of leaving hidden ambiguity.
- Expand the invariant section so every important distributed-system behavior
  can eventually be mapped to a whole-system test.
- Use the current spike branch as the shared base; teammates can make comments
  or small PRs on top.
- Coordinate implementation/research choices in Discord while the spike is
  moving quickly.

Concrete points:

- The code is easier to understand than the current Jazz codebase, but the spec
  language is not yet good enough.
- Some runtime methods are still example/fixture-shaped; this is acceptable for
  the spike but should not leak into the product architecture.
- Delete paths may not yet enforce policies correctly; this should be verified
  and either fixed or captured as a known failing invariant.

## Schema Versions And Lenses

Current design discussed:

- Schema changes create separate table sets per schema version.
- Migration lenses are compiled into queries.
- Writes go to the client's current schema version.
- Old clients may keep writing to old schema-version tables.
- Mergeable cross-schema writes merge-with-translation.
- Exclusive cross-schema writes require conflict/read-set detection across all
  relevant schema versions.

Research targets:

- Measure and reason about table proliferation as applications evolve.
- Explore background migration into the newest schema version to keep old
  schema-version tables cold or mostly empty.
- Explore creating new table sets only for tables whose shape actually changed,
  rather than cloning every table for every schema revision.
- Detect migrations that do not require data movement, such as pure renames,
  index-only changes, and permission-only changes.
- Decide whether rename-only migrations can remain purely query/lens changes
  without creating new physical tables.
- Validate that conflict detection across schema versions works for exclusive
  transactions under realistic concurrent old/new client writes.

Risks / questions:

- How expensive do multi-version union queries become as schema versions
  accumulate?
- How does background schema compaction interact with history preservation,
  branch snapshots, and transaction identity?
- How should compatibility be represented when only indexes or policies change?

## Query Planning, Policies, And Sync Scope

Current design discussed:

- Policy filters lower into SQL subqueries.
- Sync-scope calculation should be integrated into query lowering, so a query
  can also identify rows needed for policy checks and client-side query
  recreation.
- Recursive queries and recursive permissions are key proof points.

Research targets:

- Stress recursive query lowering with more complex examples than the current
  simple trees.
- Stress recursive permission lowering with multi-hop and mixed-policy chains.
- Ensure sync scopes distinguish result rows from rows needed only for policy
  enforcement.
- Ensure query lowering can return enough provenance to sync the same client
  result and policy context.
- Explore diagnostics for unsupported recursive policy/query shapes.
- Add invariants for policy-sensitive exclusive conflict detection.

Concrete invariant raised:

- Exclusive transaction validation must run under the same effective policies as
  the user whose transaction is being validated, otherwise conflict detection
  can produce false positives or leak/consider inaccessible rows.

Risks / questions:

- How do we represent policy identity/fingerprint for validation without
  trusting the sender's policy catalogue?
- Which policy-influencing rows must be sent to an edge/global authority before
  validation?
- Can aggregate queries expose only aggregate results, or must all contributing
  rows be synced to preserve local correctness?

## Aggregates And Account Aggregator

Research targets:

- Implement an account-aggregator-shaped example to validate multi-table
  aggregation, joins across schema versions, policies, and sync scope.
- Explore `count` and other aggregate queries as first-class query descriptors.
- Determine whether aggregate query scopes must sync every contributing row, as
  current Jazz often effectively requires.
- Explore whether synced query metadata can include an aggregate result preview
  in addition to contributing rows/facts.
- Determine how aggregate subscriptions should refresh and diff.

Risks / questions:

- Aggregates may be correct only if all contributing rows are synced, which can
  be too broad for large datasets.
- Aggregate result previews might be useful but could be stale, policy-sensitive,
  or hard to reconcile with local recomputation.
- Multi-schema table unions may make aggregates a good stress test for query
  planning and indexing.

## Transactions: Mergeable, Exclusive, And Local Visibility

Terminology discussed:

- `mergeable` replaces old batch terminology.
- `exclusive` means globally consistent/admitted by a core authority.
- Avoid using `optimistic` as a synonym for mergeable because it usually implies
  a pending preview of a transaction that may be rolled back.

Current semantics discussed:

- Mergeable transactions are eventually consistent, immediately visible locally,
  atomic, and participate in merge/conflict-resolution semantics.
- Exclusive transactions are only globally visible after authority acceptance.
- Exclusive transactions capture read sets and write sets.
- Read sets may contain exact row versions or predicate facts, for example
  "stock > 0".
- The core authority checks whether the read/write facts are still valid before
  accepting; otherwise it rejects.

Research targets:

- Specify local visibility for pending exclusive transactions. This may be a
  hidden/advanced option for offline-capable apps rather than the default read
  model.
- Verify exclusive validation under policy context, including avoiding false
  conflicts from rows the user could not see.
- Expand predicate read-set coverage beyond simple row reads and absence reads.
- Clarify how pending exclusive local previews interact with subscriptions,
  sync, rejection callbacks, and branch views.
- Document that most apps should prefer mergeable transactions; exclusive
  transactions are for limited resources and global invariants.

Risks / questions:

- Does exposing pending exclusive state in ordinary reads create confusing UI
  semantics?
- Can predicate read sets be compact and still precise enough for global
  authority validation?
- How do policy changes between local read and authority validation affect
  exclusive transaction fate?

## History Layout And Storage Shape

Current prototype:

- Current rows use user columns as real SQLite columns.
- History currently also stores user data as columns in per-schema-version
  history tables.

Research targets:

- Explore storing user data in history as JSON/BLOB payloads rather than actual
  user columns.
- Keep system columns/index keys relational and compact while making history
  payloads cheaper to store and compress.
- Use SQLite JSON functions to expand history payloads into semantic columns in
  query results.
- Compare query performance and disk/memory overhead for column-history versus
  JSON/BLOB-history layouts.
- Explore whether one unified history table or fewer history table shapes are
  viable.
- Preserve efficient prefix scans / primary-key order for common history access
  patterns.
- Evaluate point-in-time historical queries by global epoch and by wall-clock
  time.
- Consider an `updated_at` / commit-time index for user-facing "as of time"
  historical queries.

Risks / questions:

- JSON/BLOB history may complicate lenses, policy evaluation, and historical
  query planning.
- SQLite JSON extraction may be fast enough for cold history, but this needs
  measurement.
- Conflict resolution needs fast access to conflicting versions; history layout
  must preserve that.

## Compression And SQLite Storage

Research targets:

- Explore page-level compression through a custom SQLite filesystem/VFS.
- Evaluate LZ4 specifically because it is small to bundle and fast.
- Measure compression impact for deep histories, network payloads, and OPFS
  disk pressure.
- Compare page compression against payload compression for history JSON/BLOBs.
- Measure memory overhead while operating over compressed or blob-backed history.

Concrete intuition from meeting:

- Disk may be the bottleneck, especially with OPFS, so writing less data could
  help even if compression costs CPU.
- Compression is especially attractive for append-only deep history and system
  metadata.

Risks / questions:

- A custom filesystem/VFS may complicate bundling across browser, native, and
  server runtimes.
- Compression must not make startup or random access noticeably worse.

## Indexing And SQLite Escape Hatches

Current direction:

- Indexes remain automatic by default.
- Users can opt out or use explicit index-only style controls later when they
  understand load patterns.
- SQLite indexes should cover ordinary relational columns where possible.

Research targets:

- Define the automatic-index policy for current projection tables.
- Decide how explicit schema index controls are expressed.
- Explore Jazz-maintained side tables for features SQLite does not index well,
  such as deeply nested JSON fields or custom derived views.
- Keep side-table/index maintenance inside the same SQLite transaction as the
  write or sync apply.
- Evaluate SQLite extensions as an escape hatch, while accounting for bundling
  cost across runtimes.

Risks / questions:

- Automatic indexes improve read ergonomics but may slow writes.
- Explicit index controls must not make schemas too hard for beginners.
- Extensions may be useful but could damage portability.

## Browser / Worker Runtime Topology

Current direction:

- Browser main thread can use an in-memory SQLite node.
- A worker can use durable disk-backed SQLite.
- The main thread and worker are both SQLite-backed nodes; durability/topology
  is configuration, not a different semantic engine.

Research targets:

- Keep simulating memory main-thread plus durable worker plus edge/core topology.
- Explore async subscriptions that run directly against the worker.
- Sync to the main thread only for truly synchronous UI needs, such as text
  editing or immediate local state.
- Measure startup, memory, and sync overhead for main-thread memory SQLite plus
  worker durable SQLite.

Risks / questions:

- How much state should be mirrored into the main thread?
- How do subscriptions split between sync main-thread updates and async worker
  updates?
- Does this topology preserve crash safety and reconnect invariants under real
  browser constraints?

## Conflict Resolution

Current understanding:

- SQLite should be responsible for quickly retrieving conflicting row versions.
- Conflict resolution logic can remain deterministic Rust/application logic
  applied to those versions.
- Current prototype behavior is last-writer-wins per column where implemented.
- Updating a conflicting row can resolve the conflict by writing a resolved row.

Research targets:

- Prove custom conflict resolution for rich text and other nontrivial data
  types.
- Decide whether conflict merging happens in Rust over result sets, through
  SQLite custom functions, or through extensions for hot paths.
- Measure the size of conflict candidate sets in realistic scenarios.
- Monitor write amplification from resolving conflicts on update.
- Preserve deterministic behavior across client, edge, and server.
- Capture resolved-from-candidates provenance where product UX needs it.

Risks / questions:

- Putting too much logic into SQL may make custom merge strategies awkward.
- Doing all merge logic in Rust may leave performance on the table.
- Rich text resolution is a key Jazz product promise and should be tested early.

## Performance Priorities

Meeting priority:

- Before implementing every feature, answer whether the SQLite approach gives
  acceptable performance for the use-case patterns Jazz cares about.

Research targets:

- Benchmark recursive queries and recursive permissions over realistic data
  shapes.
- Benchmark multi-schema query unions and lens translation.
- Benchmark account aggregator / aggregate query patterns.
- Benchmark branch source graphs and conflict candidate queries.
- Benchmark history-heavy apps with and without compression.
- Benchmark current projection writes under automatic indexes.
- Track disk size, memory use, query latency, write latency, and startup time.

Potential success criteria to define:

- One-shot query latency for common app screens.
- Subscription refresh latency after reconnect.
- Write throughput for mergeable transactions.
- Authority validation latency for exclusive transactions.
- Startup/rehydration time for browser worker topology.
- Disk overhead over raw user data for current + history + metadata.

## Testing And Simulation Harness

Current direction:

- Use Rust for the prototype and whole-system harness.
- Simulate full distributed topologies before adding framework bindings.
- Use tests to assert invariants across reads, writes, sync messages, disk
  failures, network delays, restarts, and policy changes.

Research targets:

- Strengthen the distributed simulation harness so it can model delayed network
  messages, disk failures, restarts, and multiple peers.
- Make invariant tests read like product scenarios.
- Add coverage for policy-context-sensitive exclusive validation.
- Add aggregate/account-aggregator scenarios.
- Add richer recursive query/permission scenarios.
- Add rich text/custom conflict-resolution scenarios.
- Keep framework binding tests thin until core parity and performance are
  convincing.

## Product / API Follow-Ups

Research targets:

- Preserve high-level Jazz APIs where possible while renaming batch concepts to
  mergeable/exclusive transactions.
- Decide how visible `local updates` / pending exclusive previews should be.
- Keep selected magic fields such as `id` and created metadata aligned with
  status quo.
- Specify required includes and optional includes generically.
- Keep deletion, restore, and conflict-resolution permissions explicit.
- Ensure rejection detail supports both promise rejection and global error
  callback use cases.

## External Signals

Notes from meeting:

- A public Discord user had a terrible time implementing real-time cursors on
  current Jazz because of performance problems, which prompted them to ask
  about libSQL, Turso, and SurrealDB. This is a useful external signal that
  current query/sync performance pain is visible to adopters and that people
  naturally compare Jazz to embedded/database-engine approaches.
- LocalFirst conference coordination happened but is not relevant to the spec
  except as team logistics.

## Suggested Near-Term Order

1. Rewrite `SPEC.md` for clarity while preserving the current executable
   discoveries.
2. Add explicit invariants for policy-context-sensitive exclusive validation.
3. Build the account-aggregator example as an aggregate/query/sync benchmark.
4. Explore history JSON/BLOB layout versus column-history layout.
5. Explore LZ4/page or payload compression for deep histories.
6. Stress recursive queries and recursive policies beyond toy examples.
7. Prove one custom conflict-resolution path, ideally rich-text-shaped.
8. Clean fixture-shaped runtime APIs into generic processes as they become
   bottlenecks for understanding or testing.
