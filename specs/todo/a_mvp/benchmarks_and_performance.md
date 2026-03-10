# Benchmarks & Performance — Concrete MVP Plan

Define a practical benchmark suite that catches pathological regressions in Jazz now, while setting up a clean path to full browser/server end-to-end benchmarks later.

## Current Goal

Before investing in dedicated runner infra and long-term reporting, we want local benchmarks that exercise realistic schemas and load shapes and answer:

- sustained write throughput for insert/update/delete
- sustained read/query throughput
- cold-start/cold-load time on larger persisted datasets
- update fanout behavior to many subscribed clients
- read/write cost under complex permission policies (including recursive checks)
- time/space behavior with deep commit histories on a small hot object set

## Grounding in Status Quo Runtime

These benchmarks should reflect current architecture, not an idealized one:

- Runtime uses two ticks:
  - `immediate_tick()` for synchronous local settle/work
  - `batched_tick()` for sync message flush and parked-message processing
- Query engine is reactive and graph-based (`QueryGraph`, subscriptions, incremental settle)
- Policy checks use simple checks plus graph-backed evaluation for `EXISTS`/`INHERITS`
- Storage API is synchronous (MemoryStorage, FjallStorage, OpfsBTreeStorage)
- Object model is commit-graph based per row object, so hotspot history depth is first-class
- Three-tier sync wiring is already test-proven in `runtime_core` via synthetic message routing

Relevant references:

- `specs/status-quo/batched_tick_orchestration.md`
- `specs/status-quo/query_manager.md`
- `specs/status-quo/query_sync_integration.md`
- `specs/status-quo/storage.md`

## Phasing

## Phase 1 (Now): Pure Rust + Criterion + Synthetic Transport

Scope:

- local-only and in-process synthetic client/server/tier topologies
- deterministic seeded datasets
- criterion-driven measurement harness
- no browser/main-thread/worker IPC yet

Primary implementation location:

- `crates/jazz-tools/benches/` (new realistic bench harness + scenarios)

## Phase 2 (Next): Browser + Server End-to-End

Scope:

- main thread + worker (`jazz-wasm`) + HTTP/SSE server transport (`jazz-tools server`)
- OPFS-specific cold-start and persistence behavior
- client/server network and serialization costs

Primary implementation location:

- `packages/jazz-tools/tests/browser/` + benchmark artifacts in `benchmarks/realistic/`

## Phase 1 Harness Design (Concrete)

## Topology Modes

- `T0_local`:
  - one RuntimeCore
  - no network sync path
- `T1_single_hop`:
  - client RuntimeCore + server RuntimeCore
  - synthetic transport by draining sender outbox and parking inbox entries
- `T2_three_tier`:
  - client -> worker-tier -> edge-tier RuntimeCores
  - routing logic mirrors `runtime_core` three-tier tests
- `T3_fanout`:
  - one writer client + one server + N reader clients with active subscriptions

## Storage Modes

- `M0_mem`: MemoryStorage (logic throughput, policy/query/graph pressure)
- `M1_fjall`: FjallStorage (cold-load + persistence effects)

## Execution Conventions

- fixed seeds for dataset generation and operation selection
- warmup and measurement windows long enough for sustained behavior
- include both:
  - operation latency stats (`p50/p95/p99`)
  - sustained throughput (`ops/sec`/`queries/sec`)
- always capture:
  - run metadata (git SHA, scenario id, profile id, seed, topology, storage)
  - failure counts (permission denials, query errors, retries)

## Schema Set (Phase 1)

## Schema S1: Project Board (baseline realistic app)

Use the existing board model (already in `benchmarks/realistic/schema/project_board.schema.json`):

- `users`, `organizations`, `memberships`, `projects`, `tasks`, `task_comments`, `task_watchers`, `activity_events`

Representative queries:

- board view: `tasks by project/status order by priority/updated_at`
- my work: `tasks by assignee/status`
- task detail: task + comments + recent activity
- activity feed by project

## Schema S2: Permission Graph (complex + recursive policy stress)

Purpose: force `EXISTS`/`EXISTS_REL`/recursive `INHERITS` in read and write paths.

Tables:

- `users`
- `teams` (`owner_id`)
- `team_edges` (`parent_team_id`, `child_team_id`) for recursive hierarchy
- `folders` (`team_id`, `parent_folder_id`, `owner_id`)
- `documents` (`folder_id`, `author_id`, `status`, `updated_at`, `body`)
- `memberships` (`team_id`, `user_id`, `role`)
- `admins` (`user_id`)

Policy shape (target):

- `documents SELECT`:
  - `author_id = @session.user_id`
  - OR membership grants via `EXISTS_REL`
  - OR recursive `INHERITS SELECT VIA folder_id` with bounded depth
- `documents UPDATE USING`:
  - same visibility rule as SELECT
- `documents UPDATE WITH CHECK`:
  - editor/admin role requirement via `EXISTS`/`EXISTS_REL`
- `documents DELETE USING`:
  - admin-only path (exercise deny-heavy flows)

## Schema S3: Hotspot History (deep commit chain stress)

Purpose: model many edits against few rows and measure history growth effects.

Tables:

- `hot_docs` (`owner_id`, `title`, `body`, `updated_at`, `version_marker`)
- optional `hot_doc_tags` for extra indexed updates

Pattern:

- tiny hot set (e.g. 10-100 rows)
- very deep update histories per row
- periodic delete/undelete/hard-delete/truncate variants

## Dataset Profiles (Phase 1)

Profiles are per schema and deterministic by seed.

- `P0_smoke`:
  - fast local sanity
  - S1: ~3k tasks, ~12k comments
  - S2: depth 3 hierarchies, low branching
  - S3: 10 hot docs, target 5k commits/doc
- `P1_team`:
  - realistic team scale
  - S1: ~100k tasks, ~400k comments
  - S2: depth 6 hierarchies, moderate branching
  - S3: 25 hot docs, target 20k commits/doc
- `P2_large`:
  - cold-load and stress profile
  - S1: ~1M tasks, multi-million dependent rows
  - S2: depth 8 hierarchies + larger ACL sets
  - S3: 50 hot docs, target 50k+ commits/doc

## Phase 1 Scenario Matrix

Each scenario is concrete and maps directly to the immediate goals.

## R1: Sustained CRUD Throughput

Goal:

- sustained insert/update/delete throughput under realistic table/index pressure

Setup:

- schema/profile: `S1/P1`
- topology: `T0_local`, `T1_single_hop`
- storage: `M0_mem` baseline, `M1_fjall` variant

Operation mix (per writer stream):

- 20% task inserts
- 35% task updates (status, assignee, priority)
- 20% comment inserts
- 15% soft deletes
- 5% undeletes
- 5% hard-delete/truncate path

Metrics:

- write ops/sec by operation type
- latency p50/p95/p99 by operation type
- outbox batch sizes and sync message volume in `T1`

## R2: Sustained Read Throughput

Goal:

- sustained query throughput and tail latency for realistic read mix

Setup:

- schema/profile: `S1/P1`
- topology: `T0_local`, `T1_single_hop`
- storage: `M0_mem`, `M1_fjall`

Read mix:

- 40% board query
- 30% my-work query
- 20% task-detail query
- 10% activity feed query

Variants:

- read-only
- 5% background write churn (from R1 writer mix) during reads

Metrics:

- queries/sec
- latency p50/p95/p99 by query shape
- result row counts and variance

## R3: Cold Load Time on Large Data

Goal:

- quantify startup/open + first-query latency on persisted datasets

Setup:

- schema/profile: `S1/P2`, `S2/P1`
- topology: `T0_local`
- storage: `M1_fjall` only

Flow per cycle:

- open runtime on existing dataset
- run first board/read query
- run first permissioned read query (S2)
- close runtime

Metrics:

- runtime open time
- first-query latency
- first permissioned-query latency
- trend over repeated reopen cycles

## R4: Update Fanout to Many Clients

Goal:

- detect pathological subscription fanout costs

Setup:

- schema/profile: `S1/P1` hot-project subset
- topology: `T3_fanout` with `N in {10, 50, 200}`
- storage: `M0_mem` first, `M1_fjall` optional

Pattern:

- one writer updates hot project tasks at sustained target rate
- all readers subscribe to overlapping board queries

Metrics:

- end-to-end delivery latency (write commit -> client callback)
- per-update subscriber fanout cost
- missed/stale update count (if any)
- throughput collapse point as N increases

## R5: Complex Permission Read Load (Including Recursive)

Goal:

- read throughput under policy graphs and recursive checks

Setup:

- schema/profile: `S2/P1`
- topology: `T0_local`, `T1_single_hop`
- storage: `M0_mem` baseline

Read/query pattern:

- document visibility queries under policy filtering
- subtree/team reachability queries touching recursive hierarchy

Churn during reads:

- 15% ACL/membership/folder-parent updates that force policy re-evaluation

Depth variants:

- recursion `max_depth` in `{1, 3, 6, 10}`

Metrics:

- permissioned query throughput
- latency p50/p95/p99
- settle cost sensitivity vs recursion depth

## R6: Complex Permission Write Load (Including Recursive)

Goal:

- write throughput and deny-path cost under complex USING/WITH CHECK rules

Setup:

- schema/profile: `S2/P1`
- topology: `T0_local`, `T1_single_hop`
- storage: `M0_mem`

Operation mix:

- 25% insert document
- 45% update document
- 15% delete document
- 15% ACL/membership updates

Authorization mix:

- ~70% expected allow
- ~30% expected deny

Metrics:

- allowed ops/sec
- denied ops/sec
- latency split allow vs deny
- error/denial reason distribution

## R7: Deep History Hotspot (Time + Space)

Goal:

- characterize performance and storage growth when few objects accumulate very deep histories

Setup:

- schema/profile: `S3/P1`, `S3/P2`
- topology: `T0_local`
- storage: `M1_fjall` primary, `M0_mem` reference

Pattern:

- repeatedly update hot set (`10-50` docs) for large commit depth
- interleave point reads and indexed queries on same docs

Variants:

- `R7a_no_compaction`: no hard-delete/truncate
- `R7b_periodic_truncate`: periodic hard-delete/truncate and recreate

Metrics:

- update and read latency as depth grows
- reopen/load time for hotspot rows
- on-disk bytes growth vs commit count
- memory usage growth (`ObjectManager` + `QueryManager` memory stats)

## R8: Long-Run Mixed Soak

Goal:

- detect drift or pathological behavior only visible over longer runs

Setup:

- schema/profile: `S1/P1` + permission variant `S2/P1`
- topology: `T1_single_hop`
- storage: `M1_fjall`

Mix:

- combine R1+R2 workloads with low-frequency R5/R6 events

Duration:

- 30-120 minutes

Metrics:

- throughput stability
- p95/p99 drift
- storage growth slope
- memory growth slope

## Outputs (Phase 1)

Each run should emit JSON with:

- metadata: scenario/profile/seed/topology/storage/git SHA
- latency summaries per op/query type
- throughput summaries per op/query type
- sync/message counters (when topology includes transport)
- memory/storage snapshots where relevant (R3/R7/R8)

Also emit a concise markdown summary table for human review.

## Phase 2 Benchmark Scenarios (Browser + Server)

Phase 2 keeps the same scenario intent but adds browser architecture costs.

## B1: Main-thread <-> Worker throughput

- run S1 workload with real worker bridge
- measure roundtrip latency for batched sync payloads

## B2: OPFS cold reopen

- run cold-start cycles with persisted OPFS datasets
- measure open + first-query + first-subscription-settled

## B3: Browser fanout over HTTP/SSE

- one writer + many browser clients via server `/sync` + `/events`
- measure fanout latency and stream stability

## B4: Permission-heavy browser flows

- run S2 permission read/write scenarios through browser runtime + server
- compare against Rust-only baselines to isolate transport/worker overhead

## Out of Scope for This MVP Spec Update

- publishing external benchmark claims
- final regression thresholds for CI gating
- dedicated machine scheduling/report automation details

Those will be layered on top once Phase 1 scenarios are implemented and locally validated.
