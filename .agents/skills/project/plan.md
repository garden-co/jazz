# Plan Workflow

Write a comprehensive implementation plan from an approved spec. The plan assumes the engineer has zero context for the codebase. Document everything: which files to touch, code, testing, how to verify. Bite-sized tasks. TDD. Frequent commits.

**Save to:** `todo/projects/{feature-name}/plan.md`

## Scope Check

The spec should describe a single-PR change. If it covers multiple independent subsystems, suggest breaking into separate plans — one per subsystem. Each plan should produce working, testable software on its own.

## File Structure

Before defining tasks, map out which files will be created or modified and what each one is responsible for. This is where decomposition decisions get locked in.

- Design units with clear boundaries and well-defined interfaces. Each file should have one clear responsibility.
- Prefer smaller, focused files over large ones that do too much.
- Files that change together should live together. Split by responsibility, not by technical layer.
- Follow established patterns in the codebase.

This structure informs the task decomposition. Each task should produce self-contained changes that make sense independently.

## Bite-Sized Task Granularity

**Each step is one action (2-5 minutes):**

- "Write the failing test" — step
- "Run it to make sure it fails" — step
- "Implement the minimal code to make the test pass" — step
- "Run the tests and make sure they pass" — step
- "Commit" — step

## Plan Document Header

**Every plan MUST start with this header:**

```markdown
# [Feature Name] Implementation Plan

**Goal:** [One sentence describing what this builds]

**Architecture:** [2-3 sentences about approach]

**Tech Stack:** [Key technologies/libraries]

---
```

## Task Structure

````markdown
### Task N: [Component Name]

**Files:**

- Create: `exact/path/to/file`
- Modify: `exact/path/to/existing:123-145`
- Test: `tests/exact/path/to/test`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn test_specific_behavior() {
    let result = function(input);
    assert_eq!(result, expected);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test test_specific_behavior`
Expected: FAIL with "cannot find function"

- [ ] **Step 3: Write minimal implementation**

```rust
fn function(input: Type) -> Type {
    expected
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test test_specific_behavior`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add path/to/files
git commit -m "feat: add specific feature"
```
````

## No Placeholders

Every step must contain the actual content an engineer needs. These are **plan failures** — never write them:

- "TBD", "TODO", "implement later", "fill in details"
- "Add appropriate error handling" / "add validation" / "handle edge cases"
- "Write tests for the above" (without actual test code)
- "Similar to Task N" (repeat the code — the engineer may be reading tasks out of order)
- Steps that describe what to do without showing how (code blocks required for code steps)
- References to types, functions, or methods not defined in any task

## Remember

- Exact file paths always
- Complete code in every step — if a step changes code, show the code
- Exact commands with expected output
- DRY, YAGNI, TDD, frequent commits

## Self-Review

After writing the complete plan, review it against the spec:

**1. Spec coverage:** Skim each section/requirement in the spec. Can you point to a task that implements it? List any gaps.

**2. Placeholder scan:** Search the plan for red flags — any of the patterns from the "No Placeholders" section above. Fix them.

**3. Type consistency:** Do the types, method signatures, and property names used in later tasks match what was defined in earlier tasks? A function called `clear_layers()` in Task 3 but `clear_full_layers()` in Task 7 is a bug.

Fix issues inline. If a spec requirement has no task, add the task.

## Done

After saving the plan, stop. Implementation is a separate activity initiated by the user.
