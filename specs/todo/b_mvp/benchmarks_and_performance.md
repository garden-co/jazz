# Benchmarks & Performance — E2E Replacement Plan

Replace `groove` crate microbenchmarks with end-to-end scenario benchmarks that reflect real Jazz usage.

## Why Replace Current Benches

Current `crates/groove/benches/*` are useful for local profiling but not representative:

- mostly in-process runtime + `MemoryStorage`
- limited sync/tier topology coverage
- synthetic schema and operation mix
- weak comparability with browser/TypeScript runtime behavior

The new benchmark system should answer: "Is Jazz fast enough for realistic collaborative apps across native and browser stacks?"

## Core Principles

1. **E2E-first**: include storage, runtime ticks, sync routing, and settlement behavior.
2. **Parity across stacks**: every benchmark scenario must run in:
   - native client + native server (`surrealkv`)
   - TypeScript client + worker runtime (`opfs-btree`) with equivalent topology
3. **Realistic app model**: one canonical collaborative schema + realistic load profiles.
4. **Tier-aware**: explicitly measure worker/edge/core settlement and propagation.
5. **Deterministic and repeatable**: fixed seeds, fixed dataset profiles, reproducible harness.

## Scope and Phasing

### MVP (Internal De-risking)

Goal: find show-stoppers before broader adoption.

- ship new e2e benchmark harness and retire current `groove` microbenchmarks
- run daily on dedicated hardware (not CI-shared runners)
- compare native vs TypeScript parity for same scenarios

### Launch (Publishable)

Goal: externally defensible performance story.

- publish selected benchmark results and methodology
- include large-scale demo app traces (query + sync + settle by tier)
- optionally include traditional DB comparisons (carefully normalized)

## Canonical Benchmark App Schema

Use one realistic "collaborative project board" schema (same schema for all suites).

### Tables

- `users`
- `organizations`
- `memberships` (`organization_id`, `user_id`, `role`)
- `projects` (`organization_id`, `name`, `archived`, `updated_at`)
- `tasks` (`project_id`, `title`, `status`, `priority`, `assignee_id`, `updated_at`, `due_at`)
- `task_comments` (`task_id`, `author_id`, `body`, `created_at`)
- `task_watchers` (`task_id`, `user_id`)
- `activity_events` (`project_id`, `task_id`, `actor_id`, `kind`, `created_at`, `payload`)

### Query Shapes (must be benchmarked)

- board view: tasks by project + status, ordered by priority/updated_at
- my work: tasks by `assignee_id` + status
- task detail: task + comments + recent activity
- project activity feed: recent `activity_events`
- watch list: tasks watched by current user

### Access Pattern Assumptions

- many reads/subscriptions, fewer writes
- hot working set (active project), cold background data
- high fan-out on shared projects
- occasional offline burst followed by reconnect

## Dataset Profiles

Use fixed profiles so regressions are comparable over time.

- `S` (developer smoke): 10 users, 3 orgs, 30 projects, 3k tasks, 12k comments
- `M` (team-scale): 100 users, 20 orgs, 500 projects, 100k tasks, 400k comments
- `L` (launch-scale): 1k users, 100 orgs, 5k projects, 1M tasks, 4M comments

Each profile should include:

- skewed activity (top 10% projects receive most writes)
- realistic text payload sizes (short titles, medium comments, occasional large comments)
- mixed update locality (some users only touch own tasks, some shared hot projects)

## Workload Profiles

All workloads are scenario-driven and replayable from seed.

### W1: Interactive Board Session (Read-heavy)

- 60% query/subscription refresh work
- 25% task updates (status/assignee/priority)
- 10% comment inserts
- 5% project/task metadata updates

Measure:

- end-user op latency (p50/p95/p99)
- steady-state throughput
- subscription update latency

### W2: Collaboration Burst (Write-heavy)

- multiple concurrent users editing same hot project
- rapid status transitions, reassignments, comments
- fan-out to many subscribers

Measure:

- write throughput under contention
- subscription fan-out latency
- queue/backpressure behavior

### W3: Offline Queue + Reconnect

- client disconnected, performs N local writes
- reconnect and settle to configured tier

Measure:

- time-to-catch-up
- bytes/messages transferred
- reconciliation cost and tail latency

### W4: Cold Start + Reopen

- open runtime from persistent storage, run first board query, attach subscriptions

Measure:

- startup time
- first-query latency
- first-subscription settled time

### W5: Long-running Soak

- 30-120 minute mixed workload run

Measure:

- throughput stability
- tail latency drift
- storage growth (snapshot/WAL/db files)
- memory growth and leak signals

## Topology Matrix (Required)

Each workload must run on equivalent topologies:

1. **Local only**
   - native: client runtime + `surrealkv`, no server
   - TS: worker runtime + `opfs-btree`, no server
2. **Single-hop**
   - client -> edge server
3. **Multi-tier**
   - client -> edge -> core
4. **Multi-client**
   - 1 writer + N readers
   - N writers + N readers on same hot project

For each topology, capture settlement at relevant tiers (`worker`, `edge`, `core`).

## Stack-Specific Benchmark Suites

### Native Suite

- native client runtime with `surrealkv` local persistence
- native server runtime(s) with `surrealkv`
- process/network boundaries should mirror production where possible

### TypeScript Suite

- `jazz-ts` client with dedicated worker runtime
- worker persistence via `opfs-btree`
- same schema, same dataset profile, same workload script semantics
- equivalent server topology for sync scenarios

## Metrics and Output Format

For every scenario run, record:

- metadata: git SHA, date, machine info, profile/workload/topology, seed
- latency: p50/p95/p99 for key user operations
- throughput: ops/sec and bytes/sec
- sync: message counts, payload bytes, settle times by tier
- durability: flush/checkpoint times, reopen/catch-up times
- resource usage: RSS/heap (where possible), storage file sizes over time

Output:

- machine-readable JSON (raw)
- markdown summary tables (human-readable)
- trend charts for key KPIs over commits

## Regression Policy

Two lanes:

- **CI smoke lane** (`S` profile, short duration): fail on severe regressions
- **daily performance lane** (`M` + selected `L` scenarios): track trends and alert

Initial thresholds (to tune):

- fail CI if p95 latency regresses >20% on stable scenarios
- fail CI if throughput regresses >15%
- daily alert if 7-day rolling median degrades >10%

## Migration Plan (Groove Benches)

1. mark current `crates/groove/benches/*` as deprecated
2. introduce new e2e benchmark harness and scenario definitions
3. keep a minimal internal microbench set only for local profiling (not performance claims)
4. update docs/scripts so benchmark entry points run new suites by default

## Open Design Decisions

- exact harness split:
  - single cross-language scenario spec (preferred), or
  - parallel native/TS harnesses with shared seed + semantics
- how to run multi-tier (`edge -> core`) in automation with stable timing
- which `L` profile scenarios are feasible for daily runs vs weekly runs
- which metrics become hard release gates vs advisory trends
