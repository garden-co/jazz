# Structured query results — implementation PR stack

## Purpose and basis

This is a planning document, not an implementation. It decomposes the target
contract in `6_queries.md` §6.4/§6.4.1, the related Groove collector and
recursion contracts, and the v4 protocol contract as they stand at
`origin/spec/structured-results` (PR #1245). Each PR below is independently
reviewable and must be independently green through the applicable canonical
gates in `A_impl_discipline.md` and `D_testing_gates.md`; the focused checks
listed below are iteration checks, not a replacement for the landing gates.

Tests named below are proposed test names. Jazz tests use `Db`/`JazzClient` /
`TestingClient`, public schema and query builders, and `row_input!`; Groove
tests use its public `Database` and `GraphBuilder` APIs. No test is to build a
schema, permission, or query with JSON-like definitions. A narrow internal
test is allowed only where the graph descriptor validator or byte decoder is
the only observable boundary, and its reason must be stated next to the test.

## Foundation audit

These two mergeable PRs are prerequisites, but neither changes an invariant
registry row.

| Foundation                                | What is actually supplied                                                                                                                                                                                                                                                                                                                                                                | Consequence for this stack                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| #1250, `origin/feat/output-occurrence-id` | `OutputOccurrenceId` is an ordered root-plus-joined-source tuple, with canonical bytes (`crates/jazz/src/tools/object.rs:78-130`); subscription root indexes and public added/removed rows carry it (`crates/jazz/src/db.rs:7844-7901`). The public integration test pins stable plain-root identity and join-position sensitivity (`crates/jazz/tests/output_occurrence_id.rs:50-145`). | It is sufficient identity plumbing for v1 whole-parent addresses. It does **not** construct a tree, lower an array query, replace a parent, or change the v3 relation snapshot delivery shape. Merge it before the Jazz terminal work.                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| #1251, `origin/feat/record-valued-type`   | `ValueType::Record(Box<RecordDescriptor>)` is inline (`crates/groove/src/records/values.rs:170-191`). Decode recreates and byte-compares the child (`:319-329`), and construction checks descriptor equality and canonical bytes (`:495-505`). Its tests cover nested values and malformed child bytes (`crates/groove/src/records/tests.rs:200-265`).                                   | It satisfies the inline-descriptor and canonical-child-byte parts of `INV-STORAGE-27`. It is only a **partial** delivery of the durable-key part: `DirectRecordStoreSchema::new` accepts any key descriptor (`crates/groove/src/schema.rs:128-167`), so a direct or recursively containing record type can enter durable schema metadata. The direct-store codec later refuses `Value::Record`/arrays/nullables on use (`crates/groove/src/db/mod.rs:2083-2135`, `:3279-3338`, `:3447-3454`), and its one test covers only a direct record value at `set` time (`crates/groove/src/db/tests.rs:1589-1609`). That is too late and does not prove recursive schema rejection. |

Therefore `INV-STORAGE-27` must remain `target`/`untested` after #1251 and is
closed by PR 1 below, not credited merely because #1251 adds a `Record` enum
variant.

## Status (2026-08-06)

| stage | state |
|---|---|
| PR 1 — durable-key rejection | **merged** (#1260); `INV-STORAGE-27` now/✓ |
| PR 2 — Groove `CollectBy` terminal | **merged** (#1263); `INV-QUERY-27`, `INV-QUERY-28`, `G-INV-REC-16` now/✓ |
| PR 3 — canonical `ResultTree`, explicit boundedness | **merged** (#1282); `INV-QUERY-29` now/✓ |
| PR 3.5 — terminal integration | **blocked**, see _Terminal integration blocker_ below |
| PR 4 — atomic structured delivery on v4 | **blocked** on the same question |
| PR 5 — structured differential oracle | not started; `INV-TEST-5` target |

Two decisions taken after this plan was written, and not part of its original
five stages:

- **Explicit boundedness.** `INV-QUERY-29` means *boundedness must be declared*,
  not *a finite limit must be present*. An array subquery declares `limit(n)`
  (zero valid, rendering `[]`) or `unbounded()`; declaring neither is a
  validation error. `unbounded()` is a first-class supported mode — three
  correctness gates use it deliberately, so a literal ban would have forced them
  through a code path production never takes.
- **One unified output terminal** (#1279, spec; #1281, Expand mode). Joins
  produce flat wide rows in the graph; the terminal decides output shape, nest
  or expand; nothing else renders. `CollectBy` gained an Expand mode over the
  same machinery, and `INV-INC-2` was restated to bound both modes.

## Terminal integration blocker

Two attempts to split terminal integration from the v4 delivery cut both
stopped before mutating anything. The reasons compose into one question.

**`CollectBy` renders one collection slot.** `CollectByOp` carries a single
`collection_field` and produces one parent with one `Array<Record>`.
`ResultTree` requires *named ordered child arrays* — sibling relations — and
recursion through parent → child → grandchild. The obvious bridge is composing
collectors, but `INV-QUERY-27` forbids precisely that: a collector may not be
an input to any graph node, *including another collector*. The rule that makes
the terminal clean is the rule that prevents composing one into a tree.

**The carrier forces flattening.** The only public maintained carrier is
`SubscriptionEvent`, exposing flat root rows, related rows and `RelationEdge`
deltas (`crates/jazz/src/db.rs:8128`); the remote carrier encodes
`RelationEdgeEntry` (`crates/jazz/src/protocol.rs:1890`). A structured terminal
result cannot traverse either without flattening back into rows and edges —
which is the facade-side reconstruction the design forbids. So over the
retained v3 path a changed child *cannot* be delivered as a whole-parent
replacement.

Together these mean terminal integration and the wire cut cannot be separated
while the carrier is unchanged.

🔶 **Open question — should one `CollectBy` render a whole tree?** Three
candidate resolutions:

1. **One atomic PR** combining terminal integration and the v4 cut. Honest, but
   it enlarges the stage the plan already identifies as riskiest.
2. **Extend `CollectByOp` to describe a tree** — nested collection slots within
   a single terminal operator. `INV-QUERY-27` survives intact because there is
   still exactly one terminal, and the operator comes to match what §6.4 already
   promises. This is the current recommendation.
3. **Narrow the scope** — terminalize only one-shot `all_result_tree` and leave
   maintained delivery explicitly flat. Cheapest, at the cost of a structured
   read path and an unstructured subscription path.

If the answer is that a single terminal should not render sibling and recursive
arrays, then `ResultTree`'s shape needs revisiting before any of these.

## Stack

### PR 1 — Reject record-containing durable keys at schema admission

**Depends on:** #1251.

**Implementation.** Add one recursive `ValueType::contains_record()`-style
validation used by the durable-key schema admission path. `Database::new` (or
the existing schema validation boundary if that is the public fallible boundary)
must reject a direct-record key and every wrapper containing it, such as
`Array<Record>` and `Nullable<Array<Record>>`, for a direct record store. Keep
the existing codec rejections as defense in depth. Confirm ordinary table
primary-key metadata remains structurally incapable of naming such a type;
do not widen its key vocabulary. This PR must not add record values to
arrangement keys—that is part of collector graph validation in PR 2.

**Invariant put in force.** `INV-STORAGE-27` in full: inline descriptor,
canonical child bytes, and no direct or recursive record-valued durable key.

**Enforcing tests.** Add public-API Groove tests that:

- build a nested `RecordDescriptor`, store it as a value, and round-trip a
  canonical `Array<Record>` value through a direct record store;
- make an `OwnedRecord::new` with noncanonical raw child bytes and show the
  parent descriptor/direct-store value admission rejects it; and
- build otherwise valid direct-store schemas whose key is respectively
  `Record`, `Array<Record>`, and `Nullable<Array<Record>>`, then assert that
  opening the public `Database` fails before a store handle or durable write is
  available. A scalar durable key in the same test must still open and round
  trip, proving this is key-specific rather than a blanket record ban.

The test should live with public database integration tests; the existing
record-codec test may remain as a focused decoder regression, but cannot be the
only registry citation.

**Registry change.** In `crates/groove/SPEC/INVARIANTS.md`, change
`INV-STORAGE-27` from `target`/`untested` to `now`/`✓`, citing the new
direct-store schema-admission test and the canonical child-byte test (plus the
implementation anchors in `records/values.rs` and schema admission).

**Size and gates.** S. Focused: `cargo test -p groove` with the database and
record suites. Landing: the canonical set plus `dev/benchmarks/smoke.sh`,
because this changes storage admission.

### PR 2 — Groove `CollectBy` as a true terminal, including the recursion boundary

**Depends on:** PR 1 (and therefore #1251).

**Implementation.** Introduce `CollectBy`/its descriptor in Groove's public
graph builder and runtime. It consumes flat weighted rows and retains only the
per-group flat/ranked state required to render `Array<Record>` at the terminal.
The descriptor must include group/parent projection, child projection,
collection slot, order/tie fields, direction, offset, and limit. Validate the
full output descriptor; require complete scalar deterministic order/tie input;
and reject a record-containing group/order/tie/arrangement field.

Make terminality structural: every graph-node construction/validation route,
including a second collector, must reject a `CollectBy` input. The evaluator
must group touched flat deltas, calculate old and new selected child arrays,
encode both complete parents, byte-compare, and emit no delta or exactly
`-old,+new`. It must never emit child deltas. Keep collector state outside
recursive fixed-point state and do not apply the terminal's touched-group work
bound to recursive iterations, arrangements, or sub-ticks.

**Invariants put in force.**

- `INV-QUERY-27` — terminal-only validation.
- `INV-QUERY-28` — byte-equal suppression and exactly one parent replacement.
- `INV-REC-16` — collector placement does not alter recursive accounting or
  fixed-point work.

`INV-INC-2` deliberately remains target here: its required scale canary must
exercise the Jazz output-terminal delivery path, not merely Groove's generic
operator notification.

**Enforcing tests.** Use `Database::subscribe_one_sink` and public
`GraphBuilder` construction:

- `collect_by_round_trips_ordered_explicit_child_ids`: seed parents/children in
  non-order order; assert the terminal record contains the declared child id
  field and child array order.
- `collect_by_rejects_every_consumer_including_another_collector`: attempt
  filter/project/join and a second `CollectBy` above a collector and assert the
  named terminal error. If direct `IvmGraph::validate_node` coverage is needed
  to enumerate every low-level node descriptor, retain one explicitly justified
  internal test in addition to the public attempts.
- `collect_by_suppresses_unchanged_rendered_group_and_replaces_once_at_boundary`:
  an out-of-window touched child produces no notification; a front insert into a
  finite ordered window produces one notification with exactly two weighted
  records, `-1` then `+1`, each a complete parent array.
- `collect_by_after_recursive_closure_keeps_recursive_state_outside_limit`:
  construct a public recursive reachability graph whose closure/iterations
  exceed a collector limit of one. Enable the public runtime statistics,
  mutate a relevant edge, and assert correct closure-derived output plus
  recursive accumulated-row/iteration metrics greater than the one rendered
  child. A paired larger closure must increase those metrics while the rendered
  replacement remains limit-bounded. This catches the tempting but wrong
  implementation that applies the terminal bound inside recursion.

**Registry changes.** In `crates/groove/SPEC/INVARIANTS.md`, flip
`INV-QUERY-27`, `INV-QUERY-28`, and `INV-REC-16` from `target`/`untested` to
`now`/`✓`, citing the corresponding tests above and the graph-validation,
terminal evaluator, and recursion-boundary anchors.

**Size and gates.** M/L, concentrated in graph descriptors, validation,
runtime state, and black-box database tests. Focused: `cargo test -p groove`.
Landing: full canonical set plus `dev/benchmarks/smoke.sh` (engine work).

### PR 3 — Canonical `ResultTree`, array windowing, and Jazz terminal lowering

**Depends on:** PR 2 and #1250.

**Implementation.** Define a single recursive `ResultTree`/node/relation
representation at the Jazz result boundary, with ordered roots and named,
ordered child arrays; include output occurrence identity, values, explicit child
source-row id fields, and null/hole/empty relation state. Define the reducer
for a reset snapshot and a whole-parent replacement, but do not leave a second
facade-side tree materializer.

Extend `ArraySubquery` and every public builder/normalizer/shape identity with
the normative child `offset`; allow an optional finite `limit` (zero is valid
and renders `[]`) while omission selects the complete ordered suffix. Preserve child
`order_by`, default row-id order and tie-break, filters, requirements, and
nested arrays in the descriptor-complete lowering. Lower flat parent/child
association facts into the PR 2 terminal collector tree only at the serving
output boundary. Large logical replacements are decomposed and reassembled by
the transport; a partial replacement must never be admitted. This PR may prepare the v4 structured
payload types but does not change wire protocol version or delete v3 carriers.

**Invariant put in force.** `INV-QUERY-29` (optional finite windows and
atomic large-message transport). `INV-QUERY-22` remains target until the next
PR proves this terminal is what remote snapshots and deltas actually deliver.

**Enforcing tests.** At the Jazz public API boundary:

- a nested parent → child → grandchild query with explicit child projections,
  order, offset, and finite limits; assert `Db::all` returns the canonical tree
  with child ids as projected fields, ordered arrays, an empty `limit(0)` array,
  and the same order after a one-shot and a local maintained subscription reset;
- try root and nested array subqueries with no `limit`; assert public query
  preparation/read/subscription treats both as unbounded, while `limit(0)`
  renders an empty child array;
- insert enough children that one logical parent exceeds a transport frame;
  assert the public operation returns the complete array atomically.

**Registry change.** In `crates/jazz/SPEC/INVARIANTS.md`, flip
`INV-QUERY-29` from `target`/`untested` to `now`/`✓`, citing these public
validation/over-size tests and the query validator/terminal size-check anchors.

**Size and gates.** L. Focused: `cargo test -p jazz --no-default-features
--features test` for the new integration target plus relevant query tests.
Landing: full canonical set and smoke (public query/engine work); build the full
workspace because this introduces public Jazz result types.

### PR 4 — Atomic structured delivery on v6

**Depends on:** PR 3 and #1250. This is the first PR allowed to make structured
results remotely observable.

**Implementation.** Extend the protocol cut with generic logical-message
fragmentation. Replace the prior `RelationSnapshot`/`RelationEdge` delivery
family with the one `ResultTree` vocabulary. Update postcard enums,
`SyncMessage::ViewUpdate`, transport fragmentation, server/peer/receiver reduction,
WASM, N-API, native runtime, and cross-language fixtures within v6. A reset
authoritatively replaces cached terminal state. Incremental items are typed,
stable-keyed root/path `Insert`, `Update`, `Remove`, and `Move` edits emitted by
the Groove terminal.

Transport reassembly admits the complete structured message atomically. Add
`MAX_STRUCTURED_RESULT_DEPTH` and `MAX_STRUCTURED_RESULT_WIDTH` validation at
the untrusted receive boundary, before recursive semantic application or
unbounded allocation, for complete updates and chunk accumulation. Reject v3
at handshake/frame admission and retain no compatibility decoder.

**Invariants put in force.**

- `INV-QUERY-22` — real maintained graph input stays flat; the only public
  structured change is an ordered whole-parent replacement at the output
  terminal.
- `INV-SYNC-28` — v4 recursive snapshots/replacements in complete and chunked
  updates, depth/width admission, and no v3 path.
- `INV-INC-2` — the collector exception is now proven at the public delivery
  boundary: work/delivery is one touched rendered group, bounded by `R(limit)`,
  never accumulated view size.

**Enforcing tests.**

- A two-client `JazzServer`/`TestingClient` test subscribes to an ordered,
  finite nested array query, inserts a child that sorts into the middle of one
  parent, and reduces public events. It must observe one retraction and one
  addition for that parent occurrence, each carrying the complete recursively
  ordered parent; no child-only event is permitted. A second unaffected parent
  must not be replaced.
- Force both a complete update and a multi-chunk update for the same nested
  snapshot/replacement. Deliver chunks through the public transport pump in
  order and assert no visible partial tree before the last chunk, then exact
  `ResultTree` equality with the complete path.
- Build valid encoded v4 trees just below and malicious trees just above the
  public named depth and width constants; feed them through the receiver frame
  boundary and assert over-limit messages fail before the subscription state
  changes. A v3 hello/frame is rejected rather than decoded through a shim.
- Extend `incremental_delivery_canary.rs`: seed a very large number of unrelated
  parents, use a finite relation limit, mutate one child in one group, and
  assert only that parent replacement is delivered. Pin notification count and
  encoded bytes to the old/new rendered parent (a function of `R(limit)`), then
  rerun at a larger unrelated-parent scale and assert the bound is unchanged.
- Refresh Rust/WASM/N-API/TypeScript golden fixtures and run
  `dev/gates/ts-wire-codec.sh`; add a browser/native-runtime fixture proving
  the same v4 tree decodes identically on both sides.

**Registry changes.**

- In `crates/jazz/SPEC/INVARIANTS.md`, flip `INV-QUERY-22` and `INV-SYNC-28`
  from `target`/`untested` to `now`/`✓`, citing the two-client complete/chunked
  delivery and version/limit-admission tests.
- In `crates/groove/SPEC/INVARIANTS.md`, flip `INV-INC-2` from
  `target`/`untested` to `now`/`✓`, citing the public Jazz scale canary as the
  required cross-crate delivery proof as well as the terminal evaluator anchor.

**Size and gates.** XL and an atomic protocol/API migration. Focused: Rust
wire/receiver/server tests, native-runtime/browser tests, and
`dev/gates/ts-wire-codec.sh`. Landing: every canonical gate, full workspace,
and `dev/benchmarks/smoke.sh` (protocol and engine changes).

### PR 5 — Structured maintained-vs-one-shot differential oracle

**Depends on:** PR 4. A reusable `ResultTree` equality helper may land in PR 3
as inert test infrastructure, but this PR is the earliest point at which the
registry invariant can honestly flip: it needs actual structured snapshots,
whole-parent deltas, and receiver chunk assembly to compare.

**Implementation.** Replace M3's root-id-set reducer with the canonical public
`ResultTree` reducer. At every seeded checkpoint, reduce the maintained
receiver-facing reset/delta stream and compare it with a one-shot tree at the
same frontier. Equality must include ordered roots and relations, output
identity and values, descendants, aggregate/group payloads, and null/hole/empty
semantics; it must not sort arrays, discard duplicates, or substitute identity
for values. Add receiver/chunk property coverage to the same canonical reducer.

**Invariant put in force.** `INV-TEST-5`.

**Enforcing tests.** Extend
`m3_maintained_one_shot_differential_oracle` with generated bounded nested array
shapes and mutations that exercise front insertions, reorder/tie changes,
offset/limit boundaries, empty and required relations, holes/nulls, duplicate
source rows/occurrences, and parent deletion. Seeded runs must compare the
canonical tree after every checkpoint, including a valid chunked replacement
whose receiver publishes only after the final chunk. Add deterministic
minimized fixtures for a non-root-order mismatch and a nested relation mismatch
so a future accidental set comparison fails loudly.

**Registry change.** In `crates/jazz/SPEC/INVARIANTS.md`, flip `INV-TEST-5`
from `target`/`untested` to `now`/`✓`, citing the strengthened M3 oracle and
the receiver/chunk property test.

**Size and gates.** M. Focused: low-seed M3 while iterating. Landing: all
canonical gates, including `JAZZ_SEED_COUNT=300 cargo test -p jazz
m3_maintained_one_shot_differential_oracle`; run the 2,000-seed soak before
declaring the feature stable.

## Required sequencing

1. #1251 precedes PR 1 and PR 2. It supplies the record value used by the
   collector, but PR 1 must close its durable-key hole before a rendered type is
   widely admitted.
2. #1250 precedes PR 3/4. The terminal can compute a tree without it in a
   local prototype, but v1 parent replacement cannot be addressable or
   receiver-reducible without a stable output occurrence.
3. Groove terminal semantics (PR 2) precede Jazz lowering (PR 3). Otherwise
   Jazz would either re-create a facade materializer or put a collection inside
   a graph—both violate §6.4.
4. #1259 introduced `OutputOccurrenceId` before this stack. PR 4 cuts wire v6
   for terminal operations after the canonical result model and terminal
   lowering (PR 3). Terminal delivery may not retain the earlier
   `RelationSnapshot` representation in parallel after migration.
5. The `ResultTree` reducer helper can land before the feature as non-enforcing
   test infrastructure. `INV-TEST-5` cannot flip until PR 4 provides real
   structured snapshots, whole-parent replacements, and chunk assembly for the
   oracle to exercise.

## Riskiest stage

**PR 4 is the riskiest stage.** It crosses semantic rendering, receiver state,
chunking, wire versioning, and four runtime bindings at once. The likely bugs
are deceptively plausible partial states: applying a chunk before the final
chunk, treating a reordered child as a child delta rather than a complete parent
replacement, retaining a v3 compatibility path in one binding, accepting a
deep compact payload before checking depth/width, or maintaining both the old
relation snapshot cache and the new tree reducer. The complete-versus-chunked
two-client equality test and the v3 rejection test are deliberate guards
against those failures.

## Spec-gap review

No additional normative decision is required to begin this stack. The two
questions that were genuine blockers—explicit child ids and unbounded array
policy—were decided in the base of PR #1245. The missing `ArraySubquery`
`offset` field is an implementation/API gap, not a specification gap: §6.4
already requires its semantics. Likewise, §8.8 deliberately requires _named_
structured depth/width limits without fixing numeric values; PR 4 should define
the constants together with their admission tests and rationale, not invent a
new semantic policy.
