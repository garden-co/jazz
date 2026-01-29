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

### Step 2: Create Design Document

Create `design.md` in the feature directory with:

- **Overview** - High-level description of the solution
- **Architecture / Components** - System structure and component interactions
- **Data Models** - Schemas, types, and data structure
- **Testing Strategy** - Approach to testing the feature

Show the code snippets of the core parts of the implementation in the design.

We prioritize integration testing, and show a couple of test snippets as example of testing strategy.

### Step 3: First Approval Gate

Ask the user: **"Does the design look good? If so, we can move on to the implementation plan."**

Wait for explicit approval before proceeding.

### Step 4: Create Tasks Document

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

### Step 5: Second Approval Gate

Ask the user: **"Do the tasks look good?"**

Wait for explicit approval.

### Step 6: Stop

**Do not implement any code.** The workflow ends here. Implementation should be a separate activity initiated by the user.

## Important Rules

1. **Never skip steps** - Each step builds on the previous one
2. **Always get approval** - Do not proceed without explicit user confirmation
3. **No implementation** - This workflow is for planning only
4. **Kebab-case naming** - Feature directories use kebab-case (e.g., `user-authentication`)
