---
name: spec
description: Implement features using Spec Driven Development (SDD) workflow. Creates design and task documents with approval gates.
---

# Spec Driven Development Skill

Use this skill when the user asks to implement a feature. This workflow ensures proper planning and approval before any code is written.

## Workflow

Follow these steps in order. **Do not skip steps.** Always ask for explicit approval before moving to the next step.

### Step 1: Create Feature Directory

Create a feature directory under `.specs/{feature_name}` using kebab-case for the name.

### Step 2 (Optional): Create Requirements Document

**This step is opt-in and must be explicitly requested by the user.**

If the user requests requirements, create `requirements.md` in the feature directory with:

- **Introduction** - Brief context and purpose of the feature
- **User Stories & Acceptance Criteria** - Written in EARS (Easy Approach to Requirements Syntax) style

Example EARS patterns:
- **Ubiquitous**: "The [system] shall [action]"
- **Event-driven**: "When [event], the [system] shall [action]"
- **State-driven**: "While [state], the [system] shall [action]"
- **Optional**: "Where [condition], the [system] shall [action]"
- **Unwanted behavior**: "If [condition], then the [system] shall [action]"

Example structure:
```markdown
# Requirements

## Introduction

[Brief context about what this feature addresses and why it's needed]

## User Stories & Acceptance Criteria

### US-1: [User Story Title]

**As a** [role], **I want** [goal], **so that** [benefit].

**Acceptance Criteria:**
- When [user action], the system shall [expected behavior]
- While [state], the system shall [maintain condition]
- Where [optional condition], the system shall [handle appropriately]
```

If requirements are created, get approval before proceeding to the design document.

### Step 3: Create Design Document

Create `design.md` in the feature directory with:

- **Overview** - High-level description of the solution
- **Architecture / Components** - System structure and component interactions
- **Data Models** - Schemas, types, and data structure
- **Testing Strategy** - Approach to testing the feature

Show the code snippets of the core parts of the implementation in the design.

We prioritize integration testing, and show a couple of test snippets as example of testing strategy.

### Step 4: Design Approval Gate

Ask the user: **"Does the design look good? If so, we can move on to the implementation plan."**

Wait for explicit approval before proceeding.

### Step 5: Create Tasks Document

Once design is approved, create `tasks.md` in the feature directory with:

- **Numbered checklist** of coding tasks
- Each task should **reference specific design components**
- Include **only coding tasks** - no deployment, documentation, or other non-coding tasks

Example structure:
```markdown
# Implementation Tasks

## Tasks

- [ ] 1. Create data model for [entity]
- [ ] 2. Add API endpoint for [action]
- [ ] 3. Implement validation logic
- [ ] 4. Add unit tests for [component]
- [ ] 5. Add integration tests for [feature]
```

### Step 6: Tasks Approval Gate

Ask the user: **"Do the tasks look good?"**

Wait for explicit approval.

### Step 7: Stop

**Do not implement any code.** The workflow ends here. Implementation should be a separate activity initiated by the user.

## Important Rules

1. **Never skip steps** - Each step builds on the previous one
2. **Always get approval** - Do not proceed without explicit user confirmation
3. **No implementation** - This workflow is for planning only
4. **Kebab-case naming** - Feature directories use kebab-case (e.g., `user-authentication`)
