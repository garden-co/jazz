# Merge Design — TODO (This Week)

How objects with concurrent edits converge to a single state.

## Context

Jazz objects use a commit DAG: each commit stores a full snapshot of the object. When two peers edit concurrently, the DAG forks into multiple tips. This spec defines how those tips merge back.

### Open concerns (from Guido)

1. If commits store full snapshots, how do we optimize transmission when peers need only small field updates?
2. How do we support custom merge strategies if full snapshots hide the user's intent?
3. When exactly should tip merge happen?

## Proposed Direction: Three-Way Merge on Snapshots

Keep full-snapshot commits. Derive intent at merge time by diffing against the common ancestor — the same approach Git uses for source code.

### How it works

Given two tips A and B with common ancestor O:

```
       O
      / \
     A   B
      \ /
       M   (merge commit)
```

1. Compute `delta_A = diff(O, A)` — fields A changed
2. Compute `delta_B = diff(O, B)` — fields B changed
3. Apply merge policy per column:
   - **Disjoint changes**: combine both deltas (no conflict)
   - **Same column, deterministic strategy**: apply strategy (LWW, counter-add, etc.)
   - **Same column, no strategy**: conflict — surface to application
4. Produce merge commit M with the resolved snapshot

### Why this works

- **Optimizes for reads**: every commit is a complete snapshot, no chain-walking to reconstruct state
- **Intent is recoverable**: three-way diff reveals what each side changed, even though we don't store explicit deltas
- **Merge overhead is pay-on-conflict**: most objects won't have concurrent edits; those that do pay the diff cost only at merge time
- **Deterministic**: given the same DAG, every peer computes the same merge result (assuming deterministic strategy)

### Relationship to per-column strategies

The three-way merge is the *mechanism*. Per-column merge strategies (LWW, counters, sets, RGA for text) are *policies* plugged into step 3. These are catalogued in `../b_mvp/complex_merge_strategies.md`.

For MVP, LWW (last-writer-wins by timestamp) is the only strategy. The mechanism should be designed so other strategies can slot in later.

## Commit Frequency

How often a client creates a new commit directly affects the cost of snapshots and the granularity of merge.

| Use case | Commit frequency | Snapshot overhead | Merge granularity |
|----------|-----------------|-------------------|-------------------|
| Task board / CRUD | On user action (click, submit) | Low — commits are infrequent | Coarse — rarely concurrent |
| Real-time text editing | Per keystroke or debounced (50–200ms) | High — many snapshots | Fine — frequent concurrency |
| Canvas / drawing | Per stroke or debounced | Medium–High | Medium |
| Presence / cursors | Continuous (throttled) | High | Fine, but conflicts are trivial (LWW) |

Key question: **is snapshot-per-commit affordable for high-frequency use cases?** The storage benchmarking spike (`storage_benchmarking_spike.md`) will provide numbers. If not, options include:

- Debounced/batched commits (reduce frequency, coarser granularity)
- Checkpoint commits (snapshot every N-th, deltas in between) — adds complexity
- Separate commit cadence per table or column type

For MVP, target the task-board / CRUD cadence. Real-time text is a harder problem that may need RGA/Yjs-style CRDTs regardless of snapshot vs. delta choice.

## Multi-Parent Merge

The simple case above has exactly two tips and one common ancestor. Real DAGs can be more complex:

### Three or more concurrent tips

```
       O
      /|\
     A B C
      \|/
       M
```

Options:
- **Pairwise reduction**: merge A+B → AB, then AB+C → M. Simple but order-dependent unless strategies are associative/commutative.
- **N-way diff against O**: compute all deltas simultaneously, apply strategies across all of them. More principled but more complex.

For MVP, pairwise reduction with deterministic ordering (sort tips by commit hash) is sufficient. N-way merge is a later optimization.

### Nested merges (merges of merges)

```
     O
    / \
   A   B
   |   |
   C   D   (C merges O+A, D merges O+B, then C and D diverge further)
    \ /
     M
```

The common ancestor of C and D might be O (not A or B). Finding the correct common ancestor is the classic "lowest common ancestor in a DAG" problem. We need:

- An efficient LCA algorithm for the commit DAG
- Handling of criss-cross merges (multiple LCAs) — Git uses "recursive merge" for this

This is well-studied territory. For MVP, single-LCA with fallback to oldest-common-ancestor is fine.

## Tip-Merge Timing

When does the system produce a merge commit?

### Auto-merge (eager)

The receiving peer merges tips as soon as it has all the commits. This is the default for most data:

- **When** concurrent edits touch disjoint columns → auto-merge, no conflict
- **When** concurrent edits touch the same column and strategy is deterministic (e.g., LWW) → auto-merge using strategy

### Deferred merge

- **When** sync delivers out-of-order commits (missing parents) → buffer until dependencies arrive, then merge
- **When** strategy requires application input → surface conflict to app, merge when resolved

### Conflict artifacts

- **When** concurrent edits touch the same column and no deterministic strategy exists → keep tips diverged, expose all tips to subscribers in deterministic order
- Application resolves by creating an explicit merge commit

### Who creates the merge commit?

Every peer that observes diverged tips computes the same merge (deterministic). The merge commit is created locally and synced like any other commit. Peers that receive a merge commit they've already computed locally recognize it as identical (same parents, same content → same hash) and deduplicate.

## Open Questions

- How expensive is three-way diff in practice? (Benchmarks needed — see `storage_benchmarking_spike.md`)
- LCA algorithm: simple parent-walking vs. indexed ancestor queries?
- Should merge commits be distinguished from regular commits in the DAG? (Metadata flag? Multiple parents is already the signal.)
- How does schema migration interact with merge? (Commits on different schema versions?)
- Wire protocol: can we send only the delta for sync while storing full snapshots? (Derived delta encoding — compress by diffing against a commit the peer already has)

## Related Specs

- `../b_mvp/complex_merge_strategies.md` — per-column merge policies (LWW, counters, sets, RGA)
- `storage_benchmarking_spike.md` — performance numbers that inform snapshot viability
- `../b_mvp/benchmarks_and_performance.md` — broader performance goals
- `../d_later/branching_snapshots.md` — environment-level branching (uses same DAG mechanics)
