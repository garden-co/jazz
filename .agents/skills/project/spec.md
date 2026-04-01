# Spec Workflow (Single PR)

A design-then-plan workflow for changes that fit in one PR. Two phases: **spec** (what to build) and **plan** (how to build it). No implementation happens here.

For quick idea or issue capture, follow the workflows in `AGENTS.md` instead.

**After every write to `todo/`**, run `bash scripts/update-todo.sh` to regenerate `TODO.md`.

## Directory Structure

```
todo/projects/{feature-name}/
├── spec.md      <- shaped design; what to build and why
└── plan.md      <- implementation plan; created only after spec approval
```

---

## Phase 1: Spec — `spec.md`

### Step 1: Explore context

Check relevant files, docs, recent commits to understand the current state.

### Step 2: Ask clarifying questions

One question at a time. Prefer multiple choice when possible. Understand purpose, constraints, success criteria.

### Step 3: Propose approaches

Present 2-3 approaches with trade-offs and your recommendation. Lead with the recommended option.

### Step 4: Write the spec

Create `todo/projects/{feature-name}/spec.md`. Include:

- **Problem** _(opening paragraph)_ — what's broken or missing, who is affected, concrete examples
- **Appetite** _(optional)_ — ask: _"Do you want to include an appetite (time budget) in the spec?"_
- **Solution** — described at the right level of abstraction:
  - _Breadboards_: text-based UI/flow descriptions
  - _Fat marker sketches_: high-level architecture, data flow, component interactions
  - Core code snippets following Jazz conventions:
    - Rust for core logic (`crates/groove`)
    - TypeScript for client layers (`packages/`)
    - WASM + NAPI bindings where relevant
    - SQL (Jazz's custom dialect subset) for relational semantics
- **Rabbit Holes** — technical risks and what to avoid; **cannot be empty**
- **No-gos** — explicitly out of scope; **cannot be empty**
- **Testing Strategy** — integration-first (SchemaManager, RuntimeCore level), realistic fixtures with human actor names (alice, bob), not mocks

### Step 5: Spec self-review

Review the spec critically before showing it to the user:

1. **Placeholder scan** — any "TBD", "TODO", vague requirements? Fix them.
2. **Internal consistency** — do sections contradict each other?
3. **Ambiguity check** — could any requirement be interpreted two ways? Pick one and make it explicit.
4. **Challenge every section:**
   - "The solution is still too vague — describe what happens step by step when a user does X?"
   - "Rabbit holes only has one item — what else could derail this?"
   - "No-gos is empty — what are we explicitly not doing?"
   - "Does this solution actually fit the appetite, or are we doing too much?"
   - "Are the breadboards concrete enough to build from?"
   - "Would a new team member understand the intended user experience from this spec alone?"
5. Give the spec a confidence score from 1 to 10. Fix issues inline.

### Step 6: Refinement with user

Present the spec to the user with targeted questions:

> _"This is a first draft. Before we move to approval, I have a few questions to make it more solid: [list]"_

Update based on answers. Run another round if still weak.

**Spec Approval Gate**

> "The spec is looking solid. Does it reflect what you want to build? If so, we'll move on to the implementation plan."

Wait for explicit approval before proceeding.

---

## Phase 2: Plan — `plan.md`

Created only after spec approval. Read `skills/project/plan.md` (in the same directory as this file) and follow it. Pass the approved spec as context.

---

## Rules

1. Never skip phases or steps
2. Spec self-review is mandatory before showing to user
3. At least one refinement round with user before spec approval gate
4. Always wait for explicit approval before advancing to Phase 2
5. No implementation — this workflow is for planning only
6. Rabbit Holes and No-gos sections in `spec.md` cannot be empty
7. `plan.md` is only created after spec approval, via the plan sub-skill
