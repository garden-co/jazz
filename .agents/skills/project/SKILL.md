---
name: project
description: Unified project management — captures ideas, issues, and shapes specs. Auto-routes based on intent. Use for any planning, tracking, or shaping work.
---

# Project Management Skill

Single entry point for all project management. Classify the user's intent and run the matching workflow:

| Intent    | Signal                                                         | Workflow                                 |
| --------- | -------------------------------------------------------------- | ---------------------------------------- |
| **Idea**  | Raw feature thought, opportunity, "what if..."                 | Quick capture → `todo/ideas/{priority}/` |
| **Issue** | Bug, broken behavior, focused problem                          | Quick capture → `todo/issues/`           |
| **Spec**  | "Let's design...", "shape this", mature idea ready for shaping | Full Shape Up → `todo/projects/`         |

If the intent is ambiguous, ask one question to clarify:

> _"Is this a quick capture (idea or bug), or do you want to shape it into a full spec?"_

**After every write to `todo/`**, run `bash scripts/update-todo.sh` to regenerate `TODO.md`.

---

## Idea Workflow

For raw ideas that aren't ready for shaping yet.

### Step 1: Capture

If the user described the idea in the prompt, use that — don't re-ask. If the idea is unclear, ask for:

- A name (1-4 words, kebab-case for the filename)
- A one-liner description
- A priority bucket (see Step 2)

Keep it fast. This is a low-friction capture, not an interview.

### Step 2: Create `todo/ideas/{priority}/{idea-name}.md`

Ideas are organized by priority bucket:

| Directory              | Meaning                      |
| ---------------------- | ---------------------------- |
| `todo/ideas/1_mvp/`    | Must-have for first adopters |
| `todo/ideas/2_launch/` | Needed for public launch     |
| `todo/ideas/3_later/`  | Post-launch, nice to have    |

If the user didn't specify priority, ask:

> _"Which bucket: mvp, launch, or later?"_

```markdown
# {Idea Title}

## What

[One sentence: what this is]

## Why

[Why this matters — what problem it solves or opportunity it opens]

## Who

[Who is affected or who benefits]

## Rough appetite

[small / medium / big / unknown]

## Notes

[Any early thoughts, links, prior art, related ideas]
```

Fields are intentionally loose. An idea doesn't need to be fully formed. Leave `Notes` empty if not provided. Use `unknown` for `Rough appetite` if not stated.

### Step 3: Show the backlog

After saving, list all existing ideas grouped by priority bucket, with their `What` line as a one-liner summary:

```
Current idea backlog:

## MVP
- flame-graph-for-slow-jobs — Show a flame graph for slow CI jobs

## Launch
- webhook-retry-ui — Manual retry button for failed webhook deliveries

## Later
(empty)
```

If the backlog is empty except for the new idea, say so.

Optionally, if the idea seems mature enough: _"When you're ready to shape this, run `/project` again to turn it into a spec."_

### Rules (Idea)

1. Low-friction — ask only what's needed; use what's already in the prompt
2. All fields are optional; `unknown` is always valid
3. No gates, no refinement loops, no approval steps
4. Always show the full backlog after saving
5. Do not start shaping or writing code
6. Ideas are uncommitted — capturing one is not a decision to build it

---

## Issue Workflow

For bugs and focused problems. Small and scoped — not feature ideas.

### Step 1: Capture

If the user described the issue in the prompt, use that — don't re-ask. Only ask for clarification if the issue is too vague to name or describe in one sentence. Keep it fast.

### Step 2: Create `todo/issues/{issue-name}.md`

```markdown
# {Issue Title}

## What

[One sentence: what's broken or wrong]

## Where

[Area / component / file affected]

## Steps to reproduce

[How to trigger it — numbered list, or "N/A" if not applicable]

## Expected

[What should happen]

## Actual

[What actually happens]

## Priority

[critical / high / medium / low / unknown]

## Notes

[Any context, links, related issues, logs]
```

`What` is the only required field. All others are optional — use `unknown` or `N/A` for anything not provided. Derive a kebab-case filename from the issue description.

### Step 3: Show the issue list

After saving, list all existing files in `todo/issues/` with their `What` line:

```
Open issues:
- webhook-job-missing-on-rerun — Webhook re-run events don't create a new job row
- slow-query-on-trace-view — Trace view query takes >5s on large repos
```

If the list is empty except for the new issue, say so.

### Rules (Issue)

1. Low-friction — use what's in the prompt; don't interrogate
2. `What` is required; all other fields are optional — `unknown` / `N/A` always valid
3. No gates, no refinement loops, no approval steps
4. Always show the full issue list after saving
5. Do not investigate, diagnose, or fix — capture only
6. Issues are small and focused — if it sounds like a feature, route to Idea or Spec workflow instead
7. Use the test\_ prefix for any test-related issue

---

## Spec Workflow (Shape Up)

Full design workflow producing shaping documents and a pitch. **No implementation is done here.**

### Directory Structure

```
todo/projects/{feature-name}/
├── 1_problem.md   <- required: raw problem framing
├── 1b_appetite.md <- optional: time budget decision
├── 2_pitch.md     <- shaped deliverable; input to the betting table
└── 3_scopes.md    <- created only after pitch approval
```

### Refinement Loop (mandatory before every approval gate)

After creating each document, run at least one refinement round before moving to the approval gate:

1. Review the document critically — flag weak sections, vague language, missing pieces
2. Ask the user targeted questions:
   > _"This is a first draft. Before we move to approval, I have a few questions to make it more solid: [list]"_
3. Update the document based on answers
4. Confirm it's solid, or run another round if still weak
5. Then present the approval gate

**Never skip the refinement loop.** A document is not ready for approval unless it has been challenged.

### Optional Step Handling

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
