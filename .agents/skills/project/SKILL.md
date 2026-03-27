---
name: project
description: Shape Up spec workflow — use when designing a feature, shaping an idea into a spec, defining a project, writing a pitch, scoping work, or planning what to build. Triggers on "shape", "spec", "pitch", "scope", "design this", "plan this feature", "let's figure out how to build", "project for", "write a spec", or any intent to go from idea to concrete plan.
---

# Project / Spec Router

Before any design work, determine the scope of what's being built.

## Step 1: Ask

> _"Before we start, one question: is this a **spec** (fits in a single PR) or a **project** (needs multiple PRs)?"_
>
> - **Spec** — self-contained change, one branch, one review cycle
> - **Project** — multiple interconnected scopes, each delivered as its own PR

Wait for the user's answer. If unclear, help them decide: "Can the whole thing land in one PR without becoming unwieldy? If yes, spec. If it naturally breaks into independent pieces that each need their own review, project."

## Step 2: Load the workflow

Based on the answer, read the corresponding file and follow it:

- **Spec** — read `skills/project/spec.md` (in the same directory as this file) and follow it
- **Project** — read `skills/project/project.md` (in the same directory as this file) and follow it

**Do not proceed without reading the sub-skill file.** The instructions below this point are in those files.
