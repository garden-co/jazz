# Examples & Benchmarks Program

## Decision

Jazz will consolidate its product-shaped examples and realistic benchmarks into
one public collection of interactive, music-themed applications. Each app is
both a useful reference implementation and the source of reproducible,
anonymized workload definitions. The Jazz documentation homepage and examples
reference will present the collection as a prominent gallery once canonical
apps are ready.

This document is the planning contract for that program. It deliberately does
not define new product APIs: individual app work must use documented public APIs
and turn gaps it finds into separately reviewed API/design work.

## Goals and non-goals

**Goals**

- Make the most important Jazz patterns easy to explore in polished, runnable apps rather than isolated snippets.
- Exercise real schemas and user flows across the supported topology matrix.
- Reuse the same public fixtures and scenario semantics for benchmarks, correctness E2E tests, and performance receipts.
- Make regressions reproducible: an app failure must leave behind a minimal core regression and a durable end-to-end reproduction.
- Keep all public data synthetic, deterministic, and name-blind.

**Non-goals**

- Replacing framework starters or making one app per framework/auth provider.
- Treating benchmark scores as a product-compatibility promise or a substitute for focused core tests.
- Importing customer schemas, production traces, identifiers, or PII. Those remain in `jazz-private`; public scenarios preserve load _shape_, not customer data.
- Generalizing app-specific UI models into a new public API before an actual API proposal is reviewed.

## Catalogue

| App                   | Product slice                                            | Distinct Jazz feature/workload role                                                                                                                                                                                          |
| --------------------- | -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **BandChat**          | Band/fan chat with rooms, invites, images, and reactions | Small hello-world reference; identity and permission boundaries, attachments, fan-out, offline/reconnect.                                                                                                                    |
| **WorldTour**         | Plan a tour with venues, dates, maps, and collaborators  | Relations, filtered/ordered queries, calendar/map views, shared planning.                                                                                                                                                    |
| **RecordPlayer**      | Personal/shared music library and playlists              | Files and durable streams, large values, partial availability, playlist sharing.                                                                                                                                             |
| **Jamazon**           | Music-instrument storefront                              | Offline-first cart, checkout workflow, Stripe sandbox integration, idempotent external effects.                                                                                                                              |
| **Jamazon Warehouse** | Warehouse operations console for Jamazon                 | Recognizably TPC-C-shaped warehouses, districts, customers, stock, orders, order lines, and payments; multi-row exclusive transactions, stock contention, indexed status reads, batch delivery, and stock-level aggregation. |
| **SongBook**          | Songwriter notes and drafts                              | Rich nested documents, inherited/deep permissions, draft/suggestion and branch flows.                                                                                                                                        |
| **Wequencer**         | Collaborative step sequencer                             | High-frequency collaborative writes, hotspot behavior, presence, synchronization/reconnect; clock-perfect playback is an app aspiration, not a benchmark assertion until its contract exists.                                |
| **PosterShop**        | Collaborative gig-poster design canvas                   | Real-time cursors/edits, canvas-shaped fan-out, history rewind, and branches.                                                                                                                                                |
| **BigLabel**          | Multi-tenant record-label operations                     | SaaS-scale tenant filtering, organization/team policy graphs, indexed relational reads, migrations, and large synthetic datasets.                                                                                            |
| **MusicAgent**        | LLM agent for a music agent                              | Streamed transcript turns, tool calls/results, attachments, conversation branches, durable server execution, and recovery after interrupted generation.                                                                      |
| **EpicDrop**          | Web file browser plus native mounted folder              | Large binary values, streaming and range access, partial residency, local cache eviction, shared-folder permissions, filesystem events, and offline file conflicts.                                                          |

Jamazon and Jamazon Warehouse share branding and synthetic product assets, but
remain separate schemas and scenarios. The warehouse schema stays close enough
to TPC-C to make its operations understandable and comparable; it is not a
claim of TPC-C compliance.

## Shared contract

Every catalogue app has these artifacts, versioned together where their
semantics are coupled:

1. A polished canonical UI with a short, deterministic demonstration path.
2. A public schema and deterministic fixture generator. Fixtures must be synthetic, anonymized, seedable, and capable of small smoke and larger benchmark profiles.
3. A framework-neutral scenario driver expressing user-visible operations, setup, assertions, fault schedule, and scale profile. UI adapters and headless runners consume that driver; they must not fork its workload semantics.
4. Benchmark profiles and machine-readable performance receipts, including topology, storage/runtime, fixture/profile version, seed, and phase metrics.
5. An E2E topology matrix with explicit expected outcomes and recovery checks.

The canonical UI is the gallery showcase. Minimal React, Vue, Svelte, Solid,
Expo, SSR, edge, and auth examples remain small compatibility/reference shells
where useful; they are not duplicate full product apps.

## Existing material: adoption and retirement

The following is an ownership map, not an immediate rename. Existing scenarios
remain supported until a catalogue scenario has equivalent assertions, a
reproducible receipt, and a migration note. Retire only duplicated harnesses;
keep independent microbenchmarks and core canaries.

| Current material                                         | Destination app(s)                     | Migration intent                                                                                                                  |
| -------------------------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `examples/chat-react` and `examples/auth-*-chat`         | BandChat                               | Preserve auth-specific teaching value as small shells; migrate product workflow, room policy, attachments, and fan-out scenarios. |
| `examples/world-tour`                                    | WorldTour                              | Evolve it into the canonical WorldTour app rather than rebuild it elsewhere.                                                      |
| `examples/branching-project-planner-ts`                  | SongBook and PosterShop                | Split branch/history lessons by product flow; keep any minimal branch API example needed for reference.                           |
| Todo local-first families                                | BandChat/Jamazon plus framework shells | Retain framework-baseline starters/examples; adopt offline persistence and CRUD load shape only where it belongs.                 |
| Realistic `W1`, `W3`, `W4`                               | BandChat, WorldTour, Jamazon           | Recast interactive, offline-reconnect, and cold-start behavior as app scenarios; retain shared runner plumbing.                   |
| Realistic `B1`/`R1`, `B2`/`R2`, `B3`/`R3`                | BigLabel and Jamazon Warehouse         | Use for sustained CRUD, indexed reads, and cold-load scale profiles.                                                              |
| Realistic `B4`/`R4`, `R9`                                | BandChat, PosterShop, Wequencer        | Adopt subscription fan-out and subscribed-write semantics.                                                                        |
| Realistic `B5`/`R5`/`R6`                                 | BigLabel and SongBook                  | Adopt recursive-policy, write-heavy, and permission-filtered resume scenarios.                                                    |
| Realistic `B6`/`R7`, `R8`                                | PosterShop, SongBook, Wequencer        | Adopt hotspot history and branch-view workloads.                                                                                  |
| Realistic `B7`                                           | BigLabel and WorldTour                 | Adopt large relation-result hydration as an indexed relational-read profile.                                                      |
| `jazz-sim` `s1_saas`, policy-graph, customer cold-start  | BigLabel                               | Make BigLabel the product meaning of SaaS, policy-graph, and tenant cold-load shapes.                                             |
| `jazz-sim` `s2_canvas`, `s8_branch_views`                | PosterShop (and SongBook for branches) | Reuse canvas live/replay and branch-view workload semantics.                                                                      |
| `jazz-sim` `s3_permissions`, `s7_migrations`             | SongBook and BigLabel                  | Exercise deep permissions and multi-version migration/reconnect.                                                                  |
| `jazz-sim` `s4_order_processing`, `s9_durable_execution` | Jamazon Warehouse, Jamazon, MusicAgent | Keep order-processing/reference comparison and durable-workflow semantics, surfaced through their respective UIs.                 |
| `jazz-sim` `s5_durable_stream`                           | RecordPlayer, EpicDrop, MusicAgent     | Adopt stream lifecycle, persistence, resume, and bounded-memory transfer behavior.                                                |

`moon-lander-react`, server/runtime examples, and framework/auth starters remain
valuable focused references. They are out of catalogue scope unless a later
scenario needs their specific runtime surface.

## Required topology matrix

Each app declares the applicable rows below before it is called canonical. A
row may be deferred only with a named blocker and a focused substitute check.

| Topology                                        | Required evidence                                                                  |
| ----------------------------------------------- | ---------------------------------------------------------------------------------- |
| Single client, in-memory                        | Core interaction and deterministic fixture smoke.                                  |
| Persistent browser reopen                       | State survives reopen; first load and subscription state are correct.              |
| Two browser contexts via SharedWorker           | Concurrent edits, subscription delivery, and identity isolation.                   |
| Client ↔ edge ↔ global service                  | Sync, permissions, reconnect/resume, and server-side reads where applicable.       |
| Offline edit → reconnect                        | Convergence plus authorization/rejection behavior.                                 |
| Edge or worker restart during subscription      | Recovery without duplicate, missing, or unauthorized visible state.                |
| Native RocksDB restart                          | Persistent data, subscriptions, and recovery behavior.                             |
| Native SQLite relay/server restart              | Server persistence and reconnection behavior.                                      |
| React Native client through native SQLite relay | Included when the runtime supports the app's required persistence/device features. |

The gallery's interactive deployment is not itself the topology test harness.
E2E tests control failure injection, clocks where needed, fixture seeds, and
assertions; benchmark runs additionally record their environment and phases.

## Correctness-forcing loop

When an app exposes a defect, preserve three levels of evidence:

1. Add a minimized public core regression test that captures the violated invariant without app/UI machinery.
2. Add or strengthen the app's topology E2E scenario so the original user flow cannot regress silently.
3. If scale, timing, storage, allocation, or throughput contributed, add a benchmark profile/receipt with a stated comparison baseline.

The performance receipt is conditional: it is not required for a purely
semantic bug. Conversely, a benchmark regression must point back to a scenario
and should gain a focused correctness assertion when it reveals one.

## Staging and blockers

Start with products that exercise stable, currently supported surfaces, while
recording blockers rather than designing around them:

1. **Program foundation and BandChat:** repository layout, fixture/scenario contract, gallery shell, and first topology harness.
2. **BigLabel and WorldTour:** scale/policy ownership and evolution of the existing map app.
3. **Jamazon Warehouse and Jamazon:** exclusive transaction/order flow, durable execution, and external-effect boundary.
4. **PosterShop and SongBook:** canvas/history/branches and rich nested permission flows.
5. **RecordPlayer and EpicDrop:** add after large binary values, partial chunk fulfillment, bounded-memory transfer, and native cache/VFS contracts are stable enough to test honestly.
6. **MusicAgent:** add after large streamed text values and durable server-side agent execution have explicit recovery and secret-handling contracts.
7. **Wequencer:** add after synchronization and high-frequency update contracts are explicit and testable.

Before starting an app lane, classify its dependencies as **ready**,
**implement alongside**, or **blocked**. A blocked app may still contribute a
schema, synthetic fixture, and headless scenario design, but it must not imply
that an unavailable runtime feature works.

## Acceptance criteria

The collection is established when:

- The docs/homepage gallery links to every canonical, runnable app and states its distinct Jazz capability.
- Every app has the five shared-contract artifacts, or an explicit approved deferral with blocker and substitute coverage.
- App scenarios cover their applicable topology rows and produce deterministic smoke results from public fixtures.
- Existing realistic and `jazz-sim` workloads have an owner, migration status, and no accidental duplicated scheduled workload.
- Benchmark history/receipts can identify the app, scenario, profile, topology, fixture version, and seed.
- Each app-found bug follows the correctness-forcing loop, and all public fixtures pass the sensitive-data guard.

## Implementation plan

1. Establish an `examples-and-benchmarks` manifest (catalogue ownership, status, links, and scenario identifiers) plus a shared schema/fixture/scenario package layout.
2. Make BandChat the reference implementation of that contract and add the first deterministic UI and headless topology scenarios.
3. Move one existing scenario at a time behind app-owned names while preserving its old runner/report ingestion until parity is verified.
4. Add gallery cards from the manifest; do not hard-code a second catalogue in the homepage and reference docs.
5. Expand through the staging order, using every defect and performance investigation to improve the core test, app E2E, and receipt layers.
