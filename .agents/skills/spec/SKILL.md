---
name: spec
description: "Implement features using a Spec Driven Development workflow with approval gates before any coding. Use when a request should move through planning artifacts in order: requirements (optional), design, and tasks, stored in `specs/todo/{a_mvp,b_launch,c_later}` with explicit user sign-off between phases."
---

# Spec Driven Development

Use this workflow when asked to implement a feature but planning and approvals must happen first.

## Core Rules

1. Follow the steps in order and do not skip steps.
2. Ask for explicit approval before moving past each approval gate.
3. Store files under `specs/todo/<track>/` where `<track>` is one of: `a_mvp`, `b_launch`, `c_later`.
4. Use descriptive `snake_case` topic filenames.
5. Never write planning artifacts under singular `spec/`.
6. Stop after task approval.
7. Do not implement code in this workflow.

## Step 1: Choose Track and Topic Name

Choose a track:

- `a_mvp` for must-have MVP work
- `b_launch` for launch readiness
- `c_later` for post-launch follow-ups

If user does not specify a track, default to `a_mvp`.

Pick a descriptive `snake_case` topic name (e.g., `enum_column_type_end_to_end`).

## Step 2 (Optional): Create Requirements Document

Only perform this step when the user explicitly requests requirements.

Create `specs/todo/<track>/<topic>_requirements.md` with:

- `# Requirements`
- `## Introduction`
- `## User Stories & Acceptance Criteria`

Write user stories with EARS acceptance criteria patterns:

- Ubiquitous: `The [system] shall [action]`
- Event-driven: `When [event], the [system] shall [action]`
- State-driven: `While [state], the [system] shall [action]`
- Optional: `Where [condition], the [system] shall [action]`
- Unwanted behavior: `If [condition], then the [system] shall [action]`

After drafting requirements:

1. Scan for missing requirements (edge cases, errors, scope boundaries, performance constraints, roles/permissions).
2. Identify ambiguities (vague terms, multiple interpretations, non-testable criteria).
3. Ask clarifying questions grouped by topic and explain why each answer matters.

Do not proceed until critical ambiguities are resolved. Note only minor open items as assumptions in design.

If requirements are created, request approval before proceeding to design.

## Step 3: Create Design Document

Create `specs/todo/<track>/<topic>_design.md` with:

- Overview
- Architecture / Components
- Data Models
- Testing Strategy

Include code snippets for core implementation parts.

Prioritize integration testing and include representative integration test snippets in the testing strategy.

After drafting design:

1. Scan for missing requirement coverage.
2. Identify ambiguities in behavior, data flow, or ownership.
3. Ask clarifying questions and explain implementation impact.

Do not proceed until critical gaps are resolved. Note minor open items as assumptions.

## Step 4: Design Approval Gate

Ask exactly:

`Does the design look good? If so, we can move on to the implementation plan.`

Wait for explicit approval.

## Step 5: Create Tasks Document

After design approval, create `specs/todo/<track>/<topic>_tasks.md` with:

- A numbered checklist of coding tasks
- Task references to specific design components
- Only coding tasks (exclude deployment, docs, and non-coding work)

## Step 6: Tasks Approval Gate

Ask exactly:

`Do the tasks look good?`

Wait for explicit approval.

## Step 7: Stop

End the workflow. Do not implement code. Implementation starts only when the user initiates a separate request.
