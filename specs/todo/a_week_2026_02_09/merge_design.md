# Merge Design — TODO (This Week)

How objects with concurrent edits converge to a single state.

## Status Quo

Jazz objects use a git-like commit DAG. Every piece of data lives here — understanding the current model is prerequisite to designing merge.

### Commit model

Each commit is an immutable, content-addressed node (`commit.rs:33–48`):

```
Commit
├── parents: SmallVec<[CommitId; 2]>   # 0 = root, 1 = linear, 2+ = merge
├── content: Vec<u8>                   # Full binary-encoded row snapshot
├── timestamp: u64                     # Microseconds since epoch
├── author: ObjectId
├── metadata: Option<BTreeMap<String, String>>
└── (runtime-only: stored_state, ack_state — not hashed)
```

**CommitId** = BLAKE3(parents ‖ content ‖ timestamp ‖ author ‖ metadata). Deterministic: two peers creating an identical commit get the same hash.

### Branches and tips

Each object has named branches. A branch tracks its commit DAG and its **tips** — the frontier commits with no children (`object.rs:126–136`):

```
Branch
├── commits: HashMap<CommitId, Commit>
├── tips: SmolSet<[CommitId; 2]>       # Current frontier (≤2 inline)
├── tails: Option<HashSet<CommitId>>   # Truncation boundary
```

Tip management is automatic: when a commit is added, its parents leave tips and it joins tips. When two commits share a parent but neither is the other's ancestor, both are tips — the branch has **diverged**.

```
Linear:       root → c1 → c2              tips = {c2}
Diverged:     root → a                    tips = {a, b}
                   → b
Merged:       root → a ─┐
                   → b ─┴─► merge         tips = {merge}
```

### How writes work today

On `update()` / `delete()`, the QueryManager collects **all current tips** as parents for the new commit (`writes.rs:418–425`):

```rust
let tips = object_manager.get_tip_ids(id, branch)?;
let parents: Vec<_> = tips.into_iter().collect();
// ... add_commit(id, branch, parents, new_data, ...)
```

This means a **local write implicitly merges all diverged twigs** — if there were 2 tips, the new commit has 2 parents and becomes the single tip. But this is a blind merge: the write doesn't inspect the diverged tips or reconcile their content. The new content simply overwrites.

### How reads work today

`load_row_from_object_on_branch()` picks the **newest tip by timestamp** — LWW (`manager.rs:655–668`):

```rust
tips.sort_by_key(|id| branch.commits.get(id).map(|c| c.timestamp).unwrap_or(0));
let tip_id = tips.last()?;   // newest = LWW winner
```

Other tips are invisible to queries. The AllObjectUpdate notification carries `old_content` (previous LWW winner) for index delta computation, but diverged tips that aren't the LWW winner are never surfaced.

### How sync works today

`SyncPayload::ObjectUpdated` sends commits (full snapshots + parents + metadata) in topological order. The receiver calls `receive_commit()` which is idempotent by CommitId. Incremental sync sends only commits not already in `sent_tips` (`sync_logic.rs`).

Key point: **sync transmits full commit content**. No delta encoding on the wire. Each commit is the complete row snapshot.

### Summary of what exists

| Aspect | Status |
|--------|--------|
| Multi-parent commits | Supported (`SmallVec<[CommitId; 2]>`) |
| Diverged tips (multiple heads) | Tracked per branch |
| Automatic merge on local write | Yes — all tips become parents of new commit |
| Three-way merge (diff-based) | **Not implemented** |
| Read resolution | LWW by timestamp (newest tip wins) |
| Merge strategies (counter, set, etc.) | **Not implemented** |
| Delta encoding on wire | **Not implemented** (full snapshots sent) |
| Conflict surfacing to application | **Not implemented** |

### Guido's concerns in this context

1. **Transmission optimization**: today we send full snapshots per commit. For small field changes on large rows, this is wasteful.
2. **User intent for merge strategies**: with LWW, intent doesn't matter — newest wins. But richer strategies (counters, sets, text) need to know *what changed*, not just the final state.
3. **When does merge happen?**: today, implicitly on the next local write (all tips become parents). There's no explicit merge step and no three-way reconciliation.

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

The diff operates on decoded `Value` columns — not raw bytes. The row encoding (`encoding.rs`) supports per-column decode via `decode_column()`, so diffing doesn't require decoding the entire row if only a few columns changed.

### Why this works

- **Optimizes for reads**: every commit is a complete snapshot, no chain-walking to reconstruct state
- **Intent is recoverable**: three-way diff reveals what each side changed, even though we don't store explicit deltas
- **Merge overhead is pay-on-conflict**: most objects won't have concurrent edits; those that do pay the diff cost only at merge time
- **Deterministic**: given the same DAG, every peer computes the same merge result (assuming deterministic strategy + deterministic timestamps)
- **Backwards-compatible**: no change to Commit struct, Storage trait, or wire protocol. Merge commits are just commits with 2+ parents — already supported

### What changes from status quo

1. **Read path**: instead of LWW-picks-one-tip, detect multiple tips → trigger merge → read merged result
2. **Merge trigger**: explicit step when diverged tips are observed, not just a side effect of the next write
3. **LCA computation**: new algorithm to find common ancestor in the commit DAG
4. **Column-level diff**: new function to compare two decoded rows and produce a changeset

The write path (`writes.rs`) already uses all tips as parents — that part stays. But instead of blindly writing new content, the merge step would first reconcile diverged tips before applying the user's edit on top.

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

### Interaction with truncation

Branch truncation (`set_branch_tails()`) deletes old commits. If we truncate too aggressively, the common ancestor needed for three-way merge may be gone. Options:

- **Keep at least one common ancestor** per diverged pair (truncation-aware tail selection)
- **Merge before truncating** — if tips are diverged, merge them first so history is linear before truncation
- **Fall back to LWW** when ancestor is unavailable (safe default, loses intent)

## Multi-Parent Merge

The simple case has exactly two tips and one common ancestor. Real DAGs can be more complex.

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

Note: `SmolSet<[CommitId; 2]>` stores ≤2 tips inline. More than 2 concurrent tips requires heap allocation — this is fine but worth knowing as a performance characteristic.

### Nested merges (merges of merges)

```
     O
    / \
   A   B
   |   |
   C   D   (C and D are further edits on top of A and B)
    \ /
     M
```

The common ancestor of C and D is O (the point where A and B diverged). Finding it requires walking parents upward from both tips until paths converge — the classic "lowest common ancestor in a DAG" problem.

Complications:
- **Criss-cross merges** (multiple LCAs) — Git uses "recursive merge" for this
- **Long histories** — parent-walking can be expensive if divergence happened many commits ago

For MVP, single-LCA with fallback to oldest-common-ancestor is fine. The commit DAG per-object is typically small (not millions of commits like a git repo).

## Tip-Merge Timing

When does the system produce a merge commit?

### Status quo timing

Today, merge happens **implicitly on the next local write** — `update()` uses all tips as parents. There is no merge step between receiving diverged commits via sync and the next user action. During that window, reads use LWW.

### Proposed timing

**Auto-merge (eager)**: merge tips as soon as divergence is observed, during `process()` / `settle()`. This means:

- When `receive_commit()` creates a second tip on a branch, immediately trigger merge
- The merge commit is created locally, becomes the sole tip, and syncs normally
- **When** concurrent edits touch disjoint columns → auto-merge, no conflict
- **When** concurrent edits touch the same column and strategy is deterministic (e.g., LWW) → auto-merge using strategy

**Deferred merge**:

- **When** sync delivers out-of-order commits (missing parents) → `receive_commit()` already handles this (returns error, commit isn't applied). Retry when parents arrive.
- **When** strategy requires application input → keep tips diverged, surface conflict

**Conflict artifacts**:

- **When** concurrent edits touch the same column and no deterministic strategy exists → keep tips diverged, expose all tips to subscribers in deterministic order
- Application resolves by creating an explicit merge commit

### Who creates the merge commit?

Every peer that observes diverged tips computes the same merge (deterministic). The merge commit is created locally and synced like any other commit. Peers that receive a merge commit they've already computed locally recognize it as identical (same parents, same content → same hash via BLAKE3) and deduplicate.

The determinism requirement means: given the same DAG state, every peer must:
- Pick the same LCA
- Apply the same column-level diff
- Use the same merge strategy
- Produce byte-identical content
- Use a deterministic timestamp for the merge commit (e.g., `max(A.timestamp, B.timestamp) + 1`)

## Wire Protocol Considerations

Today, sync sends full commit content. Three-way merge doesn't change this — merge commits are regular commits with full snapshots. But Guido's concern about transmission efficiency is valid independently.

### Possible future optimization: derived delta encoding

When sending a commit to a peer that already has the parent, we could send `diff(parent.content, commit.content)` instead of `commit.content`. The receiver reconstructs the full snapshot locally. This is orthogonal to the merge design — it's a wire optimization, not a storage change.

This is similar to Git's pack protocol (deltified objects for transfer, full objects in storage).

Not MVP — just noting that the full-snapshot storage model doesn't preclude wire-level compression.

## Open Questions

- How expensive is three-way diff in practice? (Benchmarks needed — see `storage_benchmarking_spike.md`)
- LCA algorithm: simple parent-walking vs. indexed ancestor queries? (Per-object DAGs are small, so simple walking is likely fine)
- Deterministic merge timestamp: `max(tips) + 1`? Or use the receiving peer's wall clock? (Former is deterministic but might drift from real time)
- How does schema migration interact with merge? (Commits on different schema versions need lens transformation before diffing)
- Should merge be triggered in `receive_commit()`, in `process()`, or in `settle()`? (`process()` is where QueryManager already handles AllObjectUpdates — natural fit)
- What happens to AllObjectUpdate's `old_content` field after merge? (It currently tracks the previous LWW winner; with merge, the "previous" state is always a single tip)

## Related Specs

- `../status-quo/object_manager.md` — current commit DAG, tip tracking, storage model
- `../status-quo/sync_manager.md` — wire protocol, commit transmission
- `../status-quo/query_manager.md` — row loading, LWW reads, index delta computation
- `../b_mvp/complex_merge_strategies.md` — per-column merge policies (LWW, counters, sets, RGA)
- `storage_benchmarking_spike.md` — performance numbers that inform snapshot viability
- `../b_mvp/benchmarks_and_performance.md` — broader performance goals
- `../d_later/branching_snapshots.md` — environment-level branching (uses same DAG mechanics)
