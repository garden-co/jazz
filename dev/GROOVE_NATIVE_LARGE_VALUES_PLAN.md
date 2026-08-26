# Groove-native large values implementation plan

## Purpose and base

This plan implements Groove spec chapter 9 and Jazz spec chapter 19 on top of
PR #1661, `codex/async-ordered-storage-v2`. It replaces the experimental design
where Jazz encodes descriptors into ordinary scalar bytes and stores tree nodes
as hidden Jazz rows. No code or storage format is ported by default; algorithms
may be consulted, then implemented at their final Groove/Jazz ownership boundary.

The desired result is one semantic path:

```text
ordinary Groove scalar semantics
  + indirect physical representation
  + per-node interruptible chunk requests
  + Groove-owned async chunk storage
  + Jazz-gated locator discovery and peer miss fulfillment
```

This is not a compatibility migration. There is no released large-value format
to preserve.

## Final ownership graph

```text
Jazz Db / Node
  |-- ordinary row/version, policy, branch, history and sync state
  |-- high-level access/write APIs
  |-- authorization-gated locator disclosure
  |-- peer chunk-demand transport/forwarding
  `-- Groove Database
        |-- logical scalar and physical descriptor codecs
        |-- FastCDC/prolly tree and edit tails
        |-- locator allocation, staging and chunk KV
        |-- durable refs and reclamation queue
        |-- shared bounded verified-chunk cache
        |-- evaluation request registry
        |     |-- ordered-storage requests
        |     `-- exact authenticated chunk-storage requests
        `-- arbitrary IVM nodes with private resumable state
```

## Non-goals

- No hidden Jazz node tables.
- No Jazz-side tree traversal, chunking, tail application or JSON parsing.
- No `ResolveLargeValues` graph node or mandatory eager hydration boundary.
- No host loop that catches missing chunks and retries Groove.
- No second large-content sync protocol presented as canonical Jazz facts.
- No locator-based logical equality or index identity.
- No backwards-compatible descriptor-prefix decoder.
- No production direct-to-blob signed URL path in the first implementation.
- No history thinning.
- No mandatory public object-like content handle.

## Phase 0 — freeze receipts and remove wrong assumptions

1. Add spec registry rows and invariant-lint coverage for the new chapters.
2. Add a design test matrix before implementation:
   - bytes, text and JSON;
   - inline and indirect values;
   - no tail, live tail and consolidation boundary;
   - memory and persistent ordered storage;
   - memory and proxied blob backends;
   - one-shot, subscription, policy, join, sort, aggregate and index paths;
   - resident, delayed, missing, corrupt and unauthorized chunks.
3. Preserve an inline-only oracle that executes every logical operator against
   completely materialized values.
4. Record baseline memory, request-count and incremental-work receipts.

Stop if correctness tests require Jazz to interpret the content format or if
the async branch cannot distinguish private evaluation from publication.

## Phase 1 — general evaluation requests

### Groove changes

1. Rename the storage-specific request registry and dependency map to generic
   evaluation-request ownership.
2. Introduce typed storage and chunk request/output variants without type-erased
   downcasts in operators.
3. Let every `EvaluationEntry` retain bounded node-private continuation state.
4. Define one node-step outcome: complete, await requests, yield, or fail.
5. Preserve #1661's complete-frontier discovery, hash-equal request sharing,
   stale generation guards, cancellation, scoped failure and non-suspending
   prepared publication.
6. Add a deterministic `TestChunkProvider` with resident/delayed/paused/failing
   requests and explicit permits, analogous to `TestStorage`.

### Proofs

- Two unrelated nodes blocked on different chunks progress independently.
- Equal exact chunk requests share one future; Jazz gates discovery of those
  requests rather than Groove maintaining a second authorization context.
- One node can await storage and chunks over successive rounds.
- Cancellation drops private state and publishes nothing.
- A stale completion cannot target a replacement input/publication.
- A failing chunk terminates only the dependent closure.
- A CPU-resident streaming node yields to its work budget.

Do not yet add large-value semantics. This phase should be a small generalization
of #1661's existing evaluator, independently useful and fully green.

## Phase 2 — physical scalar and immutable tree

### Groove changes

1. Add an unambiguous physical storage arm for inline versus indirect bytes,
   string and JSON. Keep logical `ColumnType` unchanged.
2. Define versioned canonical descriptor, node, hash and metric codecs.
3. Implement FastCDC-like leaf boundaries with min/target/max limits.
4. Implement content-defined grouping of child descriptors into a recursive
   prolly tree.
5. Enforce UTF-8 leaf boundaries and aggregate UTF-16 metrics for text.
6. Store literal validated JSON source bytes.
7. Separate deterministic logical hash, exact locator-bearing object hash, and
   opaque locator everywhere; parent object hashes authenticate child locators.
8. Implement streaming create so memory is bounded by chunking/grouping buffers
   plus the emitted tree frontier.

### Proofs

- Same logical input produces the same logical hashes/tree shape across write
  fragmentation and history.
- Locators/object hashes may differ without changing equality, logical node
  identity or shape, while parent object hashes detect locator substitution.
- Random insertion rewrites a bounded probabilistic neighborhood.
- Every malformed node, dishonest metric, cycle/depth/fanout bomb, invalid text
  or invalid JSON fails closed.
- Multi-gigabyte synthetic creation keeps memory bounded; test with a generated
  stream rather than allocating the input.

## Phase 3 — lazy logical reader and edit tail

### Groove changes

1. Add logical reader operations to the evaluation context: metrics, byte and
   UTF-16 ranges, sequential cursor, equality, ordering, full materialization,
   JSON pointer and logical hash.
2. Traverse trees in dependency rounds and register every currently discoverable
   missing chunk before blocking.
3. Implement bounded ordered byte-edit tails for every content kind.
4. Map requested final ranges backwards through the tail into insertion bytes
   and base-tree ranges.
5. Implement consolidation by seeking to the edit's affected tree region,
   streaming only the necessary logical neighborhood through the proposed edit,
   and rechunking/regrouping upward until content-defined boundaries
   resynchronize. Consolidation MUST NOT scan or rechunk the whole logical value
   merely because the edit tail is being folded into the base tree. It must
   produce the same deterministic tree shape and logical hashes as fresh
   construction while reusing the exact `NodeRef`, including locator, for every
   unchanged leaf and branch.
6. Implement byte, UTF-8 and UTF-16 splice/append surfaces.
7. Implement deterministic complete JSON replacement lowering to byte edits.
8. Implement streaming JSON parsing for full validation and JSON pointer demand.

### Proofs

- Planted mutation tests prove tail application is exercised, not bypassed.
- Every range/arithmetic/UTF-16 edge is checked, including surrogate pairs,
  multibyte UTF-8 boundaries, empty values and tail edits crossing chunks.
- A narrow range reads only its tree paths and intersecting leaves.
- Equality and ordering stop requesting after a decisive mismatch.
- Length reads no chunks.
- Consolidation produces the same bytes, tree shape and logical hashes as fresh
  construction while work and rewritten nodes remain bounded to the
  content-defined affected neighborhood (with the expected probabilistic prolly
  bound rather than total value size).
- Every unchanged leaf and branch retains its exact object hash and locator
  across consolidation; planted tests fail if an implementation needlessly
  re-addresses an unchanged subtree.
- JSON pointer projection matches a full parse oracle under randomized edits.

## Phase 4 — make all scalar consumers interruptible

### Groove changes

Audit every place that assumes synchronous `Value` equality, hashing or
ordering. Route semantic operations through the evaluation context wherever a
large value may participate:

1. predicates and expression evaluation;
2. joins, semi-joins and anti-joins;
3. arrangement keys and collision verification;
4. grouping and distinctness;
5. ordering, `TopBy`, arg-min/max, windows and pagination;
6. aggregates;
7. schema indices and index probes;
8. variant/projection operators;
9. recursion and memoization;
10. terminal projection and structured results;
11. mutation validation and defaults.

Do not introduce one eager resolver node. Each consumer requests only the facts
needed to decide its output. Inline values complete through the same semantic
helpers without allocation or suspension.

### Initial index policy

- Metric, logical-hash, prefix and explicit JSON-path keys are allowed when
  their semantics are specified.
- Whole-value equality indices may use logical hash plus exact lazy collision
  verification.
- Whole-value lexical indices are either correctly hydrated during index
  maintenance or rejected explicitly; never index descriptor bytes.

### Proofs

For every operator, compare inline and indirect results and maintained deltas.
Plant descriptor-byte order/equality values that differ from logical order to
prove no physical comparison survives. Repeat with eviction between operations.

## Phase 5 — streaming node execution

### Groove changes

1. Expose a sequential logical cursor to any node step.
2. Let node-private state retain cursor, bounded operator state and explicit
   accumulator state across yields and chunk requests.
3. Implement a reference streaming checksum with a fixed-size accumulator and
   atomic conversion to one prepared output. Treat it as a black-box
   conformance/scaling probe, not a public product feature; its graph builder is
   exposed only so integration tests and benchmarks exercise the ordinary API.
4. Key persisted derived outputs by logical root+tail and exact operator identity,
   excluding locators.

### Proofs

- Peak resident input memory is independent of logical value size.
- Successive chunks are released after consumption.
- Cancellation mid-value leaves no derived row or retained input leases.
- A source replacement invalidates old work and installs only the new result.
- Repeated consumers share hash-equal computation.

## Phase 6 — bounded cache and binding copies

### Groove changes

1. Add a byte-weighted verified-chunk cache with strict configurable ownership
   budget and approximate LRU/CLOCK behavior.
2. Separate in-flight request registry from cache.
3. Return short-lived leases to evaluators and permit logical eviction while
   leases remain externally alive.
4. Ensure durable IVM records retain descriptors/derived outputs, not incidental
   input leases.
5. Expose metrics for cache-owned, evaluation-leased, materialized-result, and
   operator-state bytes while those bytes remain Rust-owned.

### Binding changes

This design covers NAPI and WASM bindings only. React Native integration is out
of scope.

1. Preserve the existing encoded-row boundary.
2. NAPI and WASM copy each completed encoded result into host-owned memory.
3. Release every Groove input lease before returning across the binding.
4. Require no Rust finalizer or external-memory adjustment for the host-owned
   copy; Groove subscriptions retain no historical emitted values solely for
   the application.

### Proofs

- A tiny cache repeatedly evicts while queries remain correct.
- Eviction with active leases is safe and eventually frees bytes.
- Streaming processing stays within window bounds.
- A deliberately retained JavaScript result owns only its host-side copy and
  pins no Groove cache entry or evaluation lease.
- Dropping results and subscriptions leaves no Rust-side binding lease.

## Phase 7 — Groove storage, locator discovery, and Jazz chunk transport

### Groove/Jazz changes

1. Move chunk storage, locator allocation and backend errors into Groove.
2. Add Groove's policy-blind async immutable byte-KV interface and memory plus
   persistent/blob-like implementations; prohibit list/hash lookup publicly.
3. Keep refcount/edge/staging metadata transactionally colocated with Groove
   physical records even when bytes use a remote KV.
4. Existing Jazz row/view authorization gates locator discovery; add no stateful
   root-grant registry or node-global authorization set.
5. Groove requests descendants only after authenticating the parent that
   disclosed them and redacts locators from diagnostics.
6. Install a Jazz `MissingChunkResolver` that fulfills Groove's still-pending
   future over auxiliary peer messages without an evaluation retry loop.
7. Forward local misses strictly upstream with hop-local ids, coalescing,
   cancellation, bounded hops and per-link byte/request backpressure.
8. Drive auxiliary traffic through a cloneable executor-neutral peer I/O pump,
   independent of `Node::tick` and the Jazz node lock. Bindings route decoded
   auxiliary frames, drain bounded output, and await readiness using browser
   microtasks, NAPI async notifications, or native async tasks.
9. Give that pump only Groove's cloneable exact-local-read service; Jazz never
   retains a callable byte-KV backend or gains staging/deletion primitives.

### Proofs

- A guessed content hash cannot retrieve a chunk.
- Possession of a disclosed opaque locator plus its authenticated hash is the
  read capability; Jazz maintains no second stateful authorization registry.
- A readable row enables root traversal; an unreadable row reveals no locator.
- Corrupt proxy/blob responses fail Groove integrity checks.
- Equal content under different locators deduplicates internally without an
  externally observable equality oracle.

## Phase 8 — high-level Jazz writes and Groove publication storage

### Jazz/Groove integration

1. Route every complete-value and edit mutation through Groove preparation.
2. Upload descriptor/root first. Groove authenticates nodes, derives a bounded
   restartable missing frontier from persisted nodes, stops collection at the
   wire batch bound, requests only absent descendants, and creates a timestamped
   descriptor-keyed retainer claim when graph closure validates; Jazz applies
   product quotas and expiry policy to the accounting receipts.
3. Evaluate ordinary Jazz Insert/Update policy against the owning row mutation.
4. Require the exact staged root to be complete and Groove-valid before row
   publication.
5. Publish the descriptor as the ordinary atomic Jazz cell/version.
6. Have the authorized physical-record batch consume any live retainer claim for
   the exact descriptor so staging ownership becomes durable root ownership
   atomically with publication. Upload-attempt ids never enter canonical state.
7. Let Jazz enforce staging quotas/expiry by querying accounting receipts and
   invoking Groove's idempotent accept/evict APIs; Groove owns mechanics only.
   Expose the same policy setter and bounded maintenance operation through Rust
   `Db`, server-shell, NAPI, and WASM. Environment-owned timers invoke it. TTL
   is maintenance policy rather than an operation-time deadline: continuation
   and row acceptance require the journal/receipt to remain present, while
   timer cadence determines when abandoned state is removed.
8. Persist root and recursive node counts in Groove's reserved metadata plane;
   zero-crossings append exact orphan work and Groove reclaims it without
   walking Jazz history.
9. Cover insert, update, transaction, merge, authority ingress, repair, Rust
   `Db`, NAPI, WASM and TypeScript through one lowering/admission seam.
10. Address partial operations through public `(table, row, column)` APIs;
    authorize before private descriptor lookup and preserve exact inherited
    descriptors on unrelated row updates.

### Proofs

- Staging without row permission exposes no application state.
- Rejected or failed mutations leave only expiring unreachable chunks.
- No direct/core path can publish oversized inline values or handcrafted
  descriptors.
- Row publication never precedes complete root availability.
- Ordinary row conflict/history/branch semantics treat descriptor+tail as one
  atomic cell.
- Crash tests expose either the old or new row root, never a published missing
  tree.

## Phase 9 — authorization, sync and settlement

### Jazz changes

1. An authority may resolve candidate roots internally while evaluating a Jazz
   policy that depends on large content.
2. Only an authorized view discloses the row/root locator to a receiver.
3. Receiver Groove operations use a capability scoped to that authorized view.
4. Keep chunks outside canonical Jazz witness/fact sync; transport them through
   the proxy on demand.
5. Compose Jazz closure/authority settlement with Groove terminal quiescence.
6. Preserve independent publication: one chunk-blocked closure must not stall
   unrelated terminals or sync results.
7. Classify missing, expired, denied, corrupt and retryable backend errors without
   exposing sensitive locator existence.

### Proofs

- Large-value read/write policies match a fully resident authority oracle.
- A client cannot fetch content to self-evaluate its own admission.
- Revocation prevents new locator disclosure/traversal in live sessions.
- Reconnect and capability replacement cannot reuse stale root authority.
- Settlement never reports while required Groove work is blocked.
- Cache eviction after settlement causes reload, not logical retraction/reset.

## Phase 10 — Groove-owned retention and collection

### Groove/backend changes

1. Persist descriptor root-reference deltas with every physical-record
   insert/delete/move; Jazz owns no parallel root ledger.
2. On first immutable-node installation, persist child edges and increment child
   inbound counts exactly once; idempotent restaging is count-neutral.
3. Protect unpublished preparations with persisted expiring staging generations
   and active reads with temporary leases.
4. Drain zero-count nodes through a bounded, durable, restartable cascade that
   decrements children before deleting locator mappings.
5. Keep deduplicated blobs while any locator mapping remains live.
6. Retain authenticated traversal as an audit/rebuild oracle, not the ordinary
   collector.

Jazz edge eviction, rejected-version moves, and any future history thinning use
the same Groove physical-record mutation mechanism; none performs separate
large-value accounting.

### Proofs

- Historical and snapshot reads survive current-root replacement.
- Collection racing active evaluation/result leases is safe.
- Crash/reopen cannot collect a referenced or staged value.
- Eventually unreachable staged locators and deduplicated blobs are reclaimed.
- Refcounts match authenticated tracing in differential audit tests.

## Phase 11 — public API and docs

1. Keep ordinary schema declarations for string, bytes and JSON.
2. Default reads/writes use idiomatic full primitives.
3. Add partial byte, UTF-16 text and JSON-pointer projections.
4. Add append and splice updates; keep whole-row CAS orthogonal.
5. Document which operations may hydrate complete values while preserving exact
   semantics and which are naturally range/stream bounded.
6. Expose native streaming create/update/upsert as one-shot reader-to-mutation
   operations over Groove's bounded builder. Expose the typed TypeScript
   operations through NAPI
   using an async source, bounded host windows, per-chunk backpressure, and
   cancellation; infer the physical kind from the runtime schema and preserve
   ordinary identity, branch-view, and timestamp context. NAPI and WASM use the
   same resumable Groove push preparation with Jazz-metered, awaited per-chunk
   persistence and incremental JSON validation, preserving the contract without
   whole-value buffering or native temporary-file spooling. Keep streaming query
   deferred until the primitive query
   machinery proves insufficient; do not require an object-like mutable handle.
7. Document bearer-locator limitations and operational proxy requirements.

## Canonical adversarial matrix

Before marking implementation ready, independently attack:

- descriptor/inline tag ambiguity;
- forged root metrics and logical hashes;
- branch cycles, depth/fanout and decompression/resource bombs;
- locator guessing, enumeration, logging and cross-capability reuse;
- cached bytes bypassing authorization;
- staging publication bypass through every mutation path;
- policy evaluation cycles and authority/client inversion;
- stale chunk completion after row/root replacement;
- partial arrangement/index publication on suspension or cancellation;
- physical descriptor comparison in every relational operator;
- edit-tail range mapping and consolidation boundaries;
- UTF-8/UTF-16 and JSON parser early-finish mistakes;
- unbounded streaming accumulator, cache pinning and returned-result ownership;
- collection races across history, snapshots, subscriptions and bindings;
- exact maintained-vs-one-shot equality under random eviction and delayed chunks.

Each behavior-changing test receives a planted positive proving that the test
fails when its guarded property is deliberately broken.

## Required performance receipts

- Inline scalar overhead versus base #1661.
- Cold and warm full reads across sizes.
- Narrow random ranges in multi-gigabyte values.
- Repeated small edits before and across consolidation.
- Random insert locality and rewritten chunk/node counts.
- JSON pointer reads and semantic merges.
- Lazy equality/order early-exit request counts.
- Concurrent request deduplication across shared graph nodes.
- Streaming-checksum peak memory, cooperative yielding and cancellation.
- Cache budget, active leases, copied binding-result memory and operator-state
  accounting.
- Memory and RocksDB ordered storage crossed with memory and blob-like chunk
  providers.
- Slow proxy, retryable failure and unavailable backend behavior.

## Landing strategy

Prefer a reviewable stack rather than one implementation PR:

1. specifications and this plan;
2. generic evaluation requests;
3. Groove tree/descriptor format;
4. lazy reader and edit tails;
5. relational operator integration;
6. streaming checksum operator;
7. cache and result ownership;
8. Jazz locator proxy/capability;
9. Jazz write publication;
10. authorization/sync/settlement;
11. retention/collection;
12. bindings, API, docs and benchmarks.

Each PR stays based on the async-core stack until #1661 lands, uses `gh stack`
with the explicit intended base, and independently passes relevant Groove/Jazz
oracles and incremental-delivery canaries.

## Stop conditions

Stop and redesign if any phase introduces:

- Jazz interpretation of tree/tail/content semantics;
- a second evaluator or host-managed missing-chunk retry loop;
- an eager resolver node required by every large-value consumer;
- published partial node/arrangement/index state;
- content hash as authorization;
- a blob backend that evaluates Jazz policy;
- cache presence as permission;
- durable IVM state accidentally pinning input chunks;
- full materialization for an operator whose semantics can conservatively finish
  from bounded evidence;
- unbounded memory proportional to a streamed value without an explicit public
  result or accumulator requiring it;
- a compatibility arm for an unreleased experimental format;
- weakened existing tests solely to accommodate suspension.
