---
name: project
description: Shape Up spec workflow — use when designing a feature, shaping an idea into a spec, defining a project, writing a pitch, scoping work, or planning what to build. Triggers on "shape", "spec", "pitch", "scope", "design this", "plan this feature", "let's figure out how to build", "project for", or any intent to go from idea to concrete plan.
---

# Shape Up Spec Workflow

Full design workflow producing shaping documents and a pitch. **No implementation is done here.**

For quick idea or issue capture, follow the workflows in `AGENTS.md` instead.

**After every write to `todo/`**, run `bash scripts/update-todo.sh` to regenerate `TODO.md`.

## Directory Structure

```
todo/projects/{feature-name}/
├── 1_problem.md   <- required: raw problem framing
├── 1b_appetite.md <- optional: time budget decision
├── 2_pitch.md     <- shaped deliverable; input to the betting table
└── 3_scopes.md    <- created only after pitch approval
```

## Refinement Loop (mandatory before every approval gate)

After creating each document, run at least one refinement round before moving to the approval gate:

1. Review the document critically — flag weak sections, vague language, missing pieces
2. Ask the user targeted questions:
   > _"This is a first draft. Before we move to approval, I have a few questions to make it more solid: [list]"_
3. Update the document based on answers
4. Confirm it's solid, or run another round if still weak
5. Then present the approval gate

**Never skip the refinement loop.** A document is not ready for approval unless it has been challenged.

## Optional Step Handling

Before any optional step, explicitly ask:

> _"[Step name] is optional. Do you want to include it, or skip to the next step?"_

The user must choose. Never skip silently.

---

### Step 1: Create feature directory

Create `todo/projects/{feature-name}/` using kebab-case for the feature name.

---

### Step 2: Problem Statement — `1_problem.md` _(required)_

Create this file with:

- What's broken or missing?
- Who is affected? (be specific: developers using Jazz, app authors, end users of Jazz-powered apps?)
- Concrete examples — not abstract requirements

**Refinement loop**: After drafting, review to find any gaps or improvement areas.

---

### Step 3: Appetite — `1b_appetite.md` _(optional)_

Ask: _"Appetite is optional. Do you want to define a time budget for this feature, or skip to the pitch?"_

If included, write:

- **Small batch**: 1-2 weeks — what we're committing to
- **Big batch**: 4-6 weeks — what we're committing to
- **Rationale**: why this appetite fits this problem

**Refinement loop**: Challenge the appetite choice:

- "Is this really a big batch? What would we cut to make it small batch?"
- "What's the non-negotiable core if we run out of time?"
- "Does the appetite reflect business priority or just engineering comfort?"

---

### Step 4: Pitch — `2_pitch.md` _(required)_

Create at `todo/projects/{feature-name}/2_pitch.md`. Include:

- **Problem** — 1-3 sentence framing of the raw need
- **Appetite** — restated from `1b_appetite.md` if it was written; omit otherwise
- **Solution** — described at the right level of abstraction:
  - _Breadboards_: text-based UI/flow descriptions — no wireframes needed
  - _Fat marker sketches_: high-level architecture, data flow, component interactions
  - Core code snippets following Jazz conventions:
    - Rust for core logic (`crates/groove`)
    - TypeScript for client layers (`packages/`)
    - WASM + NAPI bindings where relevant
    - SQL (Jazz's custom dialect subset) for relational semantics
- **Rabbit Holes** — technical risks and what to avoid; **this section cannot be empty**
- **No-gos** — explicitly out of scope; **this section cannot be empty**
- **Testing Strategy** — integration-first (SchemaManager, RuntimeCore level), realistic fixtures with human actor names (alice, bob), not mocks

**Refinement loop**: This is the most critical review. Challenge every section:

- "The solution is still too vague — describe what happens step by step when a user does X?"
- "Rabbit holes only has one item — what else could derail this?"
- "No-gos is empty — what are we explicitly not doing?"
- "Does this solution actually fit the appetite, or are we doing too much?"
- "Are the breadboards concrete enough to build from?"
- "Would a new team member understand the intended user experience from this pitch alone?"
- Review to find any gaps or improvement areas. Give the pitch a confidence score from 1 to 10.

**Pitch Approval Gate**

> "The pitch is looking solid. Does it reflect what you want to build? If so, we'll move on to scopes."

Wait for explicit approval before proceeding.

---

### Step 5: Scopes — `3_scopes.md` _(required, created after pitch approval)_

Break work into named, interconnected scopes. Scopes emerge from the solution — they are not a flat numbered task list.

```markdown
# Scopes

## [Scope Name] — [what it solves]

- [ ] Task
- [ ] Task

## [Scope Name] — [what it solves]

- [ ] Task
```

**Refinement loop**: Challenge the scope breakdown:

- "Are these scopes truly independent, or does [A] block [B]?"
- "This scope looks too large — can it be split?"
- "Are all tasks in [scope] actually required by the pitch, or is this gold-plating?"
- "Is there a scope that could be deferred if we run out of time?"
- Review to find any gaps or improvement areas. Give the document a confidence score from 1 to 10.

**Scopes Approval Gate**

> "The scopes look solid. Do they match what you expected? If so, we're done with planning."

Wait for explicit approval before proceeding.

---

### Step 6: Stop

**Do not write any code.** The workflow ends here. Implementation is a separate activity initiated by the user.

### Rules (Spec)

1. Never skip steps (except explicitly optional ones, after asking the user)
2. Refinement loop is mandatory — at least one round before every approval gate
3. Optional steps require an explicit choice — ask "include or skip?" before proceeding
4. Always wait for explicit approval before advancing
5. No implementation — this workflow is for planning only
6. Rabbit holes and No-gos sections in `2_pitch.md` cannot be empty
7. `3_scopes.md` is only created after pitch approval
8. Appetite is a constraint, not an estimate — it shapes what solution is possible
