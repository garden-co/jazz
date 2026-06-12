# SQLite Core Performance Benchmark Scenarios

Status: sprint preparation draft.

Goal: define synthetic but app-shaped scenarios for de-risking SQLite-core
performance under realistic load patterns. The benchmarks should measure
whole-topology performance as perceived by the client main thread, not only
isolated SQLite query latency.

## What We Need To Learn

The big question is whether the SQLite-core architecture stays fast when Jazz
features are exercised together:

- query-scoped sync through a topology
- policy-filtered reads and writes
- recursive permission dependencies
- subscriptions and semantic diffs
- cold clients where only core has the data
- warm/subset clients that need reconciliation
- small pages over very large tables
- many users and many active query scopes
- realistic app boot sequences with many concurrent subscriptions

The benchmark suite should report numbers that map to product experience:

- time from `db.all` or `subscribeAll` call to first usable main-thread result
- time to settled result at the requested tier
- number and timing of intermediate callbacks
- visible rows returned
- history rows, policy rows, and observed facts synced
- bundle bytes and rows applied at each tier
- SQLite query time at each node
- sync apply time at each node
- main-thread callback/diff delivery time
- memory, database bytes, and row/index counts before and after

Where possible, each scenario should run in these topology/cache modes:

- **Core-only cold:** core has durable data; edge, worker, and tab start empty.
- **Edge warm:** core and edge have data; worker/tab start empty.
- **Worker subset warm:** worker has a previously observed subset; tab starts
  empty or with only its active mirror.
- **Client subset warm:** the client has page/scope data, but core has newer
  rows, deletes, policy changes, or sort-key changes.
- **Reopen:** durable worker/client restarts and resubscribes.

## Measurement Harness Shape

The benchmark should use the topology construction-set spirit from the
mini-SQLite prototype:

```text
tab/main-thread node -> durable worker/broker -> optional edge -> core authority
```

All nodes should still use SQLite, including in-memory nodes. This keeps the
storage/query boundary consistent while allowing browser-like topologies:

- main-thread node: SQLite in memory, only active UI scope
- worker/broker: durable SQLite, broader local cache
- edge: durable SQLite, trusted peer/cache/permission boundary
- core: durable SQLite, global authority

For native sprint benchmarks, "main-thread perceived" can be modeled with an
instrumented client API boundary. Browser/WASM/OPFS measurements should follow
once the semantic benchmark exists.

## Scenario 1: Scoped Ordered Page Over Huge Table

Anchor: the workload from PR #898.

App shape:

- many organizations/users
- one very large table such as `documents`, `items`, `tasks`, or
  `activity_events`
- each row has an owner or scope id
- rows have `updated_at` or `created_at`
- user can read rows where `owner_id = current_user` or through a simple
  membership policy

Hot query:

```sql
WHERE owner_id = current_user
ORDER BY updated_at DESC
LIMIT 50
```

PR #898 exact baseline:

- table: `documents`
- columns: `owner_id`, `org_id`, `updated_at`, `title`
- single-column indexes: `owner_id`, `org_id`, `updated_at`
- composite case: `(owner_id ASC, updated_at DESC)`
- target owner: `alice`
- page size: 50
- default benchmark rows: 100k total, 10k for target owner
- serious one-shot target: 10M total rows, 10k for target owner, 20 query
  iterations
- comparison cases: `baseline_single_column` vs `composite_owner_updated`

Scale targets:

- total rows: 100k, 1M, 10M when practical
- rows per user: 100, 1k, 10k
- visible page size: 20, 50, 200
- users: 100, 1k, 10k

Variants:

- no policy, direct owner predicate only
- owner policy lowered to SQL
- organization membership policy
- current projection read
- cold core-only subscription
- warm subset reconciliation
- rows outside the page update their sort key and move into the page

Main thing to learn:

- whether the whole topology preserves the storage-level win of composite
  limited scans
- whether query-scoped sync avoids pulling every row for the user
- whether page-boundary invalidation is precise enough
- whether write amplification from extra indexes is acceptable once sync,
  projection maintenance, and authority validation are included

## Scenario 2: Permissioned Operations Dashboard Boot

Anchor: abstracted from a difficult adopter load pattern, with customer-specific
names and domain details intentionally removed.

App shape:

- large operational SaaS schema with roughly 100-150 tables
- home screen starts about 39 live subscriptions concurrently
- core organization tables:
  - `organizations`
  - `user_profiles`
  - `teams`
  - `team_entries`
  - `team_access_edges`
- repeated resource families:
  - `resource`
  - `resource_access_edges`
  - optional child table inheriting permissions from `resource`
- one high-cardinality child table under a permissioned parent

Permission shape:

- compute teams reachable from `session.user_id`
- compute admin-reachable teams separately
- allow resource read if a reachable team has an access edge with a read role
- access-edge rows are readable when their team is reachable/admin-reachable
- child rows inherit read/update permissions through parent reference
- team graph is recursive, with depth up to about 32

Representative scale:

- users/profiles: 20-25 for the visible workspace
- teams: 40 small case, 1.2k large cold case
- team entries: 40 small case, 2.2k large cold case
- resource access edges: tens to hundreds per family
- high-cardinality child table: about 20k visible rows
- subscriptions at boot: about 39

Benchmark phases:

- cold core-only boot: client subscribes to all home-screen queries
- warm shared-context boot: schema/catalogue/team context already present
- subset warm boot: small resource queries cached, high-cardinality table cold
- policy-change repair: team/access edge change hides or reveals many children

Metrics:

- first callback per subscription
- time until all subscriptions have first visible result
- tail latency for the high-cardinality child query
- repeated recursive policy work across the 39 subscriptions
- rows synced only for policy enforcement vs rows visible in results
- callback ordering and UI usability before the slow tail completes

Main thing to learn:

- whether shared policy context is reused enough
- whether one large permissioned child table blocks unrelated small scopes
- whether cold cache latency is dominated by SQL, sync, decoding, or callback
  delivery

## Scenario 3: Project Board Parity Suite

Anchor: existing realistic benchmark schema in
`dev/benchmarks/realistic/schema/project_board.schema.json`.

App shape:

- `users`
- `organizations`
- `memberships`
- `projects`
- `tasks`
- `task_comments`
- `task_watchers`
- `activity_events`

Existing profiles:

- S: 10 users, 3 organizations, 30 projects, 3k tasks, 12k comments, 9k
  activity events
- M: 100 users, 20 organizations, 500 projects, 100k tasks, 400k comments,
  250k activity events

Queries:

- board page:
  `tasks WHERE project_id = ? ORDER BY updated_at DESC LIMIT 200`
- my work:
  `tasks WHERE assignee_id = ? AND status = 'in_progress' ORDER BY updated_at DESC LIMIT 200`
- task detail with comments/watchers/activity includes or follow-up queries
- activity feed:
  `activity_events WHERE project_id = ? ORDER BY created_at DESC LIMIT 200`

Variants to port:

- CRUD sustained
- mixed reads
- reads with background write churn
- cold load/open and first query
- fanout updates
- hot history on a small task set
- many branches
- subscribed write path

Main thing to learn:

- continuity with existing Jazz benchmark history
- whether the new topology/harness sees the same bottlenecks as current Jazz
- how much SQLite-core overhead exists over raw app data for a familiar app

## Scenario 4: Core-Only First Paint

This is the most important topology-specific benchmark.

Shape:

- seed through an admin/core writer
- shut down the seed client
- open a fresh user client with no local data
- subscribe to one app screen query
- measure from API call to first usable main-thread result and settled result

Screen variants:

- small scoped list: 50 visible rows
- normal screen: 200 visible rows
- rich screen: primary rows plus comments/activity/detail subqueries
- policy-heavy screen: primary rows plus recursive permission dependencies

Main thing to learn:

- whether query-scoped sync makes an empty client feel fast when only core has
  durable data
- whether first paint can happen before the entire active screen graph has
  settled
- how much each topology hop contributes

## Scenario 5: Warm Subset Reconciliation

Shape:

- client subscribes to page 1 and caches the observed scope
- client disconnects or closes
- core receives changes:
  - row enters visible page
  - row leaves predicate
  - visible row updates non-sort field
  - visible row updates sort key
  - offscreen row crosses page boundary
  - row is deleted
  - policy parent changes visibility
- client reopens and resubscribes

Metrics:

- time to stale local answer, if emitted
- time to reconciled answer
- callback count before stable
- semantic diff size
- rows/history/facts applied
- unnecessary scope churn

Main thing to learn:

- whether reconciliation is bounded by actual semantic changes
- whether page boundary facts are sufficient
- whether stale local answers are useful without creating confusing callback
  storms

## Scenario 6: Policy-Heavy Recursive Read

Anchor: existing `B5`/`R5` recursive permission benchmarks, extended to cold
and subset-warm topology cases.

App shape:

- folders or groups form a recursive tree/graph
- documents/resources inherit permissions from ancestors
- allowed and denied users query the same table

Query:

```sql
documents
ORDER BY updated_at DESC
LIMIT 200
```

Scale/variants:

- recursive depth: 1, 3, 6, 10, 32
- allowed ratio: 70%, 10%, 1%
- warm ancestors but cold documents
- warm documents but stale ancestors
- policy parent change reveals/hides many children

Main thing to learn:

- whether recursive CTE policies are fast enough
- whether policy context hydration is shared across queries
- whether denied rows are filtered without leaking privileged detail

## Scenario 7: Subscription Invalidation Storm

Shape:

- one client has several active scopes:
  - board page
  - my work
  - task detail comments
  - activity feed
  - permissioned documents
- another client or core applies writes:
  - visible row update
  - offscreen irrelevant update
  - offscreen row enters the visible page
  - sort-key boundary update
  - policy parent update
  - delete/tombstone
  - many unrelated writes

Metrics:

- remote commit to final main-thread callback
- number of rerun queries
- number of callbacks
- rows diffed vs rows changed
- false-positive invalidations
- bundle and apply cost

Main thing to learn:

- whether invalidation precision is good enough for real subscribed UIs
- whether unrelated writes remain cheap
- whether policy changes are expensive but understandable

## Scenario 8: Chat/Room Fanout With Membership Policy

Anchor: current chat examples.

App shape:

- users/profiles
- rooms
- memberships
- messages
- reactions
- attachments/files

Queries:

- visible rooms for current user
- recent messages in one room ordered by created time
- message detail with reactions/attachments
- unread or activity feed

Variants:

- public room
- private room with membership policy
- member vs non-member
- 1, 10, 50, 200 subscribers in a room
- append-heavy messages
- reaction toggles
- attachment/file metadata include

Main thing to learn:

- append-heavy subscription throughput
- many-user fanout
- membership policy cost
- file metadata and attachment policy chains

## Scenario 9: Offline Operational Field App

Anchor: ICP for offline and operational software.

App shape:

- organizations
- field users
- assignments/jobs
- forms/checklists
- form responses
- media attachments
- sync status/event log

Flow:

- worker/client starts with assigned jobs and form templates
- user goes offline
- user creates many responses and attachments locally
- another user/core updates assignments while offline
- reconnect merges accepted local work, rejected/conflicting updates, and new
  remote assignments

Metrics:

- offline write latency
- local subscription responsiveness while offline
- reconnect time to settled state
- upload/sync bytes
- conflict/rejection callback behavior
- memory and database growth

Main thing to learn:

- whether local-first write/read performance remains excellent under a dirty
  local queue
- whether reconciliation after disconnect is non-pathological

## Scenario 10: Subgraph/Include Stress

Anchor: existing subgraph/include docs and examples.

App shapes:

- project board task includes project/comments/watchers
- chat message includes author/reactions/attachments
- file upload includes file and file parts
- travel/world-tour stop includes venue/place metadata

Variants:

- 50, 200, 1k outer rows
- one nested include
- two nested includes
- per-row correlated query lowering vs shared subquery/CTE lowering
- policies on included parents/children

Main thing to learn:

- whether include-heavy UI queries avoid per-row query explosion
- how observed facts and sync scope grow with nested data
- whether policy enforcement rows can be distinguished from result rows for
  measurement and future sync optimization

## Scenario 11: Branch And Snapshot Scaling

Anchor: existing `R8` and SQLite-core branch semantics.

Shape:

- many branches over shared history
- branch-local writes and merges
- branch-filtered queries
- snapshot reads by global epoch and full vector

Variants:

- branches: 10, 100, 1k
- commits per branch: 10, 100
- merge fan-in: 2, 10, 100
- current branch vs non-current branch
- branch with policy-dependent visibility

Main thing to learn:

- whether pure-query branch/snapshot reads remain acceptable at expected scale
- whether hot branch materialization is needed
- whether branch provenance metadata grows cleanly

## Scenario 12: File And Binary Metadata Chain

Anchor: file-upload, chat attachments, and operational media.

Shape:

- uploads table
- files table
- file parts/chunks table
- parent row controls attachment visibility

Variants:

- small avatar/image
- medium attachment
- large audio/video represented as many parts
- query parent with included file metadata and parts
- delete parent and later cleanup
- encrypted opaque payload columns

Metrics:

- metadata query time
- sync rows/bytes for metadata
- blob bytes excluded/included as configured
- policy chain cost
- storage growth

Main thing to learn:

- whether row-modeled file metadata is cheap enough
- whether large binary payloads stay out of hot query/sync paths

## First Overnight Cut

The first overnight sprint should bias toward scenarios that answer the most
architectural questions with the least bespoke benchmark infrastructure.

Recommended order:

1. **Core-only first paint** using the project-board schema.
2. **Scoped ordered page over huge table** from PR #898, with policy and
   topology timing added.
3. **Warm subset reconciliation** for top-N/page scopes.
4. **Permissioned operations dashboard boot** with 39 subscriptions and one
   20k-row child table.
5. **Policy-heavy recursive read** cold and subset-warm.
6. **Subscription invalidation storm** over board/my-work/activity scopes.
7. **Chat fanout with membership policy** if time permits.

Minimum useful benchmark output for every scenario:

```text
scenario_id
profile_id
topology
cache_mode
seed_rows_by_table
visible_rows_returned
policy_rows_synced
history_rows_synced
observed_facts_synced
bundle_bytes
db_bytes_by_node_before_after
api_to_first_result_ms
api_to_settled_result_ms
callback_count
sqlite_query_ms_by_node
sync_apply_ms_by_node
main_thread_delivery_ms
```

## Implementation Notes

- Prefer synthetic fixtures with app-shaped table names and policies. They
  should be public-safe and deterministic.
- Keep scales configurable so CI can run smoke profiles and overnight/manual
  runs can push toward realistic cardinalities.
- Measure native SQLite first, but structure the report so browser/WASM/OPFS
  can later fill the same fields.
- Do not only benchmark isolated SQL statements. Every important scenario
  should have a whole-topology version.
- Capture cold and warm/subset numbers separately; averages that mix them will
  hide the most important product behavior.
- Track correctness alongside speed: expected row ids, stable ordering,
  policy-denied absence, and deterministic semantic diffs should be asserted.
