---
name: jazz
description: Spec-first engineering workflow for the Jazz distributed, local-first relational database (Rust core in `crates/jazz-tools`, TypeScript client layers, WASM, RN and NAPI bindings). Use when implementing, refactoring, or testing features in this repository, especially when changes must preserve relational semantics, follow red-then-green TDD, and emphasize end-to-end integration behavior.
---

# Jazz

Treat Jazz as a distributed, local-first relational database. Prioritize relational semantics over SQL surface details or DSL syntax.

## Read Specs First

Read relevant files in `specs/` before coding:

- Use status-quo specs for current behavior.
- Use todo specs for upcoming behavior in `specs/todo/`:
  - `a_mvp/` for MVP and first-adopter requirements
  - `b_launch/` for launch readiness
  - `c_later/` for post-launch work
- Scan filenames first to map scope quickly, then open only relevant topics.

## Canonical Naming

Use `jazz` naming consistently across docs, comments, and user-facing text.

- Rust core crate path: `crates/jazz-tools/`

## Design and Change Strategy

Treat this codebase as pre-launch prototype software:

- Break backward compatibility when needed.
- Rework existing design to incorporate new assumptions from the start.
- Avoid bolting on new layers when a coherent redesign is cleaner.

## Testing Discipline

Follow TDD in strict red-then-green order:

1. Write a test against public contracts first.
2. Run it and confirm failure.
3. Implement until it passes.

Prefer end-to-end integration tests over unit tests:

- Target system-level behavior around `SchemaManager` and `RuntimeCore`.
- Write small unit tests only for isolated pure functions.

Write tests to read like real usage:

- Use realistic fixtures and domain-shaped metadata.
- Use human actor names like `alice` and `bob`.
- Avoid generic placeholders unless testing abstract graph mechanics.

Add short ASCII flow sketches for non-trivial causal flows, especially:

- Multi-client sync
- Branching and merging
- Permission pipelines

## Build and Test Commands

Use Turbo entrypoints:

- `pnpm build`
- `pnpm test`

## Completion Bias Guardrails

Prioritize in this order:

1. Correctness and design fidelity
2. Honesty about breakage and unknowns
3. Clarity in TODOs, red tests, and status summaries
4. Completion

Allow intermediate breakage during complex work. Do not weaken assertions, add test-only backdoors, or explain away failures to force completion.

Ask before moving to a new phase after major milestones. Treat "it compiles" as insufficient evidence of correctness.

## After Hard Problems

When implementation is harder than expected, run a brief 5-whys reflection:

- Identify the root difficulty (wrong assumption, missing spec, architectural gap, or other).
- Record the insight in memory or update a spec to prevent repetition.
