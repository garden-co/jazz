We're building a new, distributed, local-first SQL database.
Layer by layer.

Communicate tersely without loosing precision or warmth.

We use TDD and design for strong boundaries of concern, not taking shortcuts even when time runs short - we can always continue big tasks in successive sessions.

When time runs short, prefer leaving functionality incomplete (with clear TODO markers in code and plans) over implementing shortcuts that violate the architecture. It's more than OK to not complete a plan or even to not reach any passing tests within one session - we're working on complex stuff and eventual correctness and faithful design matters more than speed.

Depth over breadth: One fully working feature is worth more than five scaffolded ones. Don't move to the next phase/crate/component until the current one has working E2E tests that exercise real behavior. Placeholder tests that just `assert!(true)` or return empty data are not tests - they're lies. If implementing something properly requires modifying existing code, do it.

When working on multi-phase plans, ask before moving to the next phase. "Phase N compiles" is not the same as "Phase N works."

## On Complex Multi-Step Work and the Completion Trap

There is a strong systemic bias toward showing incremental progress - marking tasks complete, making each step compile, having green checkmarks. This bias actively harms complex work (refactors, multi-module features, architectural changes) where the correct path involves intermediate states that don't compile or pass tests.

**The nature of complex work**: Whether refactoring existing code or building a new feature that touches many places, there's often no valid "halfway" state. The thing works when it's done, not incrementally. Fighting this reality by adding workarounds creates tech debt and obscures the design.

**Principles:**

- **Temporary breakage is expected**: Changing an interface breaks its callers. Adding a new abstraction means old code doesn't use it yet. The fix comes later in the plan. Do not add shims, null implementations, or workarounds to "fix" intermediate breakage.

- **Deferred validation**: Running tests/checks after every micro-step creates pressure to "fix" expected errors. For complex work, validate at the END or at explicit milestones, not continuously.

- **Design fidelity over completion**: A half-implemented design that compiles is worse than a fully-implemented design that doesn't compile yet.

- **The TODO list is a map, not a railroad**: It shows what's left to do. It is not a forcing function requiring each item to leave the codebase working. Items can be reordered, combined, or done in parallel if that serves the design better.

**Warning signs you're in the completion trap:**

- Adding "temporary" implementations you plan to replace
- Writing explicit loops where the design implies scheduler-driven iteration
- Using test/null/mock versions of components the design says should be real
- Feeling urgency to mark a task "done" before its design intent is realized
- Choosing a suboptimal implementation order because "this task is next"
- Adding `#[cfg(test)]` backdoors that restore removed functionality

**When you notice these signs**: STOP. Reread the plan. Ask whether you're implementing the design or working around it.

**Critical: When removing APIs, tests must adapt.** When removing an API, tests MUST adapt to the new API. Never add `#[cfg(test)]` backdoors that restore removed functionality - this defeats the purpose of the change and hides integration issues. If you're about to re-add something the user explicitly asked to remove (even "just for tests"), STOP and ask first. Making 45 tests compile by adding a backdoor is worse than having 45 tests that need rewriting - the failing tests are telling you what work remains.

We document internal architecture and plans in /specs as markdown files.
We document public APIs and user guides in /docs as markdown files.

Tests should err on the side of E2E coverage using high-level abstractions (e.g., SchemaManager, SyncManager) rather than calling internal helpers directly. This catches integration issues and ensures the public API works as intended. The only exception is tiny unit tests for isolated pure functions.

When writing tests or implementing features, if you discover that functionality doesn't work as expected, STOP and surface the issue immediately. Do not write workarounds, ignore the test, or make it look like things pass when they don't. The gap between "what we thought worked" and "what actually works" is critical information.
