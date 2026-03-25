---
name: review-current-branch
description: Use when reviewing a branch before merge, after completing feature work, or when asked to review changes. Spawns parallel review agents for correctness, performance, and simplicity focused on crates and packages.
---

# Review Current Branch

Reviews the current branch's diff vs main by spawning parallel agents. Primary focus is `crates/` and `packages/` changes. If `examples/` or `docs/` changes are detected, dedicated agents are spawned for those areas.

## Workflow

### Step 1: Get the diff

Run `git diff main...HEAD` to get the full branch diff. If the diff is empty, tell the user there's nothing to review and stop.

Also collect the list of changed files with `git diff main...HEAD --name-only`.

Classify changed files into buckets:

- **core**: files under `crates/` or `packages/`
- **examples**: files under `examples/`
- **docs**: files under `docs/`
- **other**: everything else (benchmarks, CI, configs)

### Step 2: Spawn core review agents

Launch all 4 core agents **in parallel** using the Agent tool. Each agent receives:

- The full diff
- The list of changed files (so they can read surrounding context with the Read tool)

Each agent MUST format its output as:

```
## {Agent Name} Review

{numbered list of findings, each as:}
{n}. [{severity}] `file/path:line` — {description}
```

Where `{severity}` is one of: `critical`, `warning`, `nitpick`.

#### Agent 1: Bug Hunter

```
You are a bug hunter reviewing a code diff for a local-first relational database (Rust core + TypeScript client layers, WASM + NAPI bindings).

Review this branch diff against main. For each finding, provide a severity (critical/warning/nitpick), the file:line, and a concise explanation.

Flag:
- Logic errors and off-by-one mistakes
- Race conditions or concurrency issues (tick ordering, sync message handling)
- Unhandled edge cases (null, undefined, empty collections, missing commits)
- Unsafe code issues in Rust (unsound lifetimes, incorrect unsafe blocks)
- WASM/NAPI boundary errors (wrong types across FFI, missing error propagation)
- Broken error handling (swallowed errors, wrong error types, panics in library code)
- Type mismatches or incorrect type assertions (both Rust and TypeScript)
- Missing validation at system boundaries
- Commit-graph or CRDT correctness issues (merge, causal ordering)
- Policy/permission logic errors (wrong grant/deny, missing checks)

Do NOT flag:
- Style or readability issues (that's the simplicity agent's job)

Read the changed files for full context when needed. Use the Read tool.

Output format:
## Bug Hunter Review

1. [severity] `file:line` — description
...
```

#### Agent 2: Performance

```
You are a performance reviewer for a local-first relational database. The system has no external database — it IS the database. Performance-critical paths include: storage operations (MemoryStorage, FjallStorage, OpfsBTreeStorage), the reactive query engine (QueryGraph, subscriptions, incremental settle), sync message processing (immediate_tick, batched_tick), and commit-graph traversal.

The project has Criterion micro-benchmarks in crates/jazz-tools/benches/ and a realistic macro-benchmark suite in benchmarks/realistic/. Review with both micro and macro performance in mind.

Review this branch diff against main. For each finding, provide a severity (critical/warning/nitpick), the file:line, and a concise explanation.

Flag:
- Unnecessary allocations in hot paths (cloning where borrowing suffices, Vec where iterator works)
- O(n^2) or worse algorithmic complexity where O(n) or O(n log n) is achievable
- Unnecessary hash map / B-tree lookups in loops (N+1 patterns within the engine itself)
- Missing or broken index usage in the query engine
- Subscription fanout costs that grow worse than linearly with subscriber count
- Sync message volume or size regressions (extra fields, redundant messages, unbounded batches)
- Storage I/O regressions (extra reads/writes per operation, missing batching)
- Commit-graph traversal that doesn't bound depth or visits too many nodes
- Lock contention or unnecessary serialization points
- WASM binary size regressions (pulling in large dependencies, monomorphization bloat)
- Cold-start / cold-load regressions (startup path doing unnecessary work)
- Memory leaks or unbounded growth (subscriptions not cleaned up, caches without eviction)

Do NOT flag:
- Micro-optimizations in code paths that run rarely (migrations, one-time setup)
- Style preferences that don't affect performance

Read the changed files and surrounding context when needed. Use the Read tool. When reviewing storage or query changes, read the relevant bench files in crates/jazz-tools/benches/ for context on what's measured.

Output format:
## Performance Review

1. [severity] `file:line` — description
...
```

#### Agent 3: Simplicity

```
You are a simplicity reviewer for a local-first relational database (Rust core + TypeScript layers). This is a prototype — simplicity and directness are paramount. The project values breaking backcompat freely before launch and reworking code from first principles when designs change.

Review this branch diff against main. For each finding, provide a severity (critical/warning/nitpick), the file:line, and a concise explanation.

Flag:
- Unnecessary abstractions or indirection (trait where concrete type works, generic where specific works)
- Premature generalization (built for hypothetical future needs)
- Code that could be written more simply
- Unnecessary new files or modules when existing ones could be extended
- Patterns that don't match the rest of the codebase
- Dead code or unused exports introduced in the diff
- Backwards-compatibility shims or feature flags that aren't needed yet
- Over-engineered error handling (wrapping errors that should propagate directly)

Do NOT flag:
- Style preferences (formatting, naming conventions) unless they hurt readability
- Things that are genuinely needed for the task

Read the changed files for context when needed. Use the Read tool.

Output format:
## Simplicity Review

1. [severity] `file:line` — description
...
```

#### Agent 4: Integration Test Coverage

```
You are an integration test coverage reviewer for a local-first relational database (Rust core + TypeScript client layers). The project strongly prefers E2E integration tests (exercising SchemaManager, RuntimeCore, sync pipelines) over unit tests. Tests should read like real usage with realistic fixtures and human actor names (alice, bob).

Review this branch diff against main. Identify the features and behaviors introduced or changed, then check whether they have adequate integration test coverage.

For each finding, provide a severity (critical/warning/nitpick), the file:line (of the untested code), and a concise explanation of what's missing.

Approach:
1. Read the diff to understand what new features/behaviors are introduced
2. Read the test files in the diff to see what IS tested
3. Identify gaps: new code paths, edge cases, error conditions, or cross-layer behaviors that lack integration tests

Flag:
- New public APIs or behaviors with no integration test
- New error/validation paths that are never exercised in tests
- Edge cases visible in the implementation but not covered (empty inputs, null defaults, boundary conditions)
- Cross-layer behaviors (Rust <-> TS, WASM boundary, sync round-trips) that aren't tested end-to-end
- Schema migration or encoding changes without round-trip tests
- New permission/policy logic without integration tests covering allow AND deny paths

Do NOT flag:
- Missing unit tests for internal helpers (the project prefers integration tests)
- Test style preferences
- Coverage for code paths that existed before this branch

Read the changed files, test files, and surrounding context when needed. Use the Read tool.

Output format:
## Integration Test Coverage Review

1. [severity] `file:line` — description of untested behavior and what test is missing
...
```

### Step 3: Conditionally spawn peripheral agents

If changed files include `examples/` paths, spawn an **Examples Agent**:

```
You are reviewing example application changes for a local-first relational database framework. Examples serve as both documentation and integration tests.

Review this branch diff against main, focusing only on files under examples/. For each finding, provide a severity (critical/warning/nitpick), the file:line, and a concise explanation.

Flag:
- Examples that don't reflect current API (outdated patterns, deprecated usage)
- Missing error handling that would confuse users trying to learn from the example
- Examples that are more complex than needed to demonstrate the concept
- Broken imports or references to renamed/removed APIs
- Examples that silently fail rather than showing clear errors

Do NOT flag:
- Internal implementation details (reviewed by core agents)
- Style preferences

Read the changed files for context when needed. Use the Read tool.

Output format:
## Examples Review

1. [severity] `file:line` — description
...
```

If changed files include `docs/` paths, spawn a **Docs Agent**:

```
You are reviewing documentation changes for a local-first relational database framework.

Review this branch diff against main, focusing only on files under docs/. For each finding, provide a severity (critical/warning/nitpick), the file:line, and a concise explanation.

Flag:
- Factual inaccuracies (wrong API signatures, incorrect behavior descriptions)
- Code snippets that won't compile/run with current API
- Missing documentation for new public APIs or changed behavior
- Broken links or references
- Contradictions with other docs pages

Do NOT flag:
- Prose style preferences
- Minor formatting issues

Read the changed files for context when needed. Use the Read tool.

Output format:
## Docs Review

1. [severity] `file:line` — description
...
```

### Step 4: Filter the false positives

Spawn a sub-agent to double-check every reported item. Give it the full list of findings and access to the codebase.

Filter out every false-positive. The filter must check two things for each finding:

1. **Factual accuracy** — does the code actually match what the finding claims?
2. **Contextual relevance** — is the issue actually reachable given the surrounding execution context and invariants? A finding that is technically true but impossible to trigger (e.g. "X panics if Y is absent" when Y is guaranteed by an earlier check in the same code path) is a false positive and should be dropped.

### Step 5: Final summary

After all sub-agents complete, synthesize their findings into a final report:

```
### Critical / Must Fix
(bugs, correctness issues, or performance regressions that must be addressed before merge)

### Recommended Changes
(simplicity, performance, coverage gaps worth fixing now)

### Low Priority / FYI
(nitpicks and minor notes that can be deferred)

### Summary
(2-4 sentence overall assessment)
```

Deduplicate overlapping findings across agents. Prefer concrete `file:line` references over vague statements.

## Rules

1. Always run all 4 core agents in parallel — never sequentially
2. If the diff is empty, stop immediately
3. Agents must read changed files for context — don't review the diff blindly
4. Findings must include `file:line` references
5. Every finding must have a severity level
6. Don't flag things outside the diff unless they're directly affected by the changes
7. Examples and docs agents only spawn when those areas have changes
8. Core agents (bug hunter, performance, simplicity) focus on `crates/` and `packages/` — they may read `examples/` or `docs/` for context but should not produce findings for those areas when dedicated agents handle them
