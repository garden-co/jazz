# Branching & Snapshots — TODO

Point-in-time forks for staging environments and safe experimentation.

## Overview

When forking an environment (e.g., creating "staging" from "prod"), we want a true snapshot:

- "staging" = "prod as of {timestamp}" + subsequent staging-only edits
- Edits in staging do NOT flow back to prod automatically
- Pulling changes back to prod should be an explicit step (cherry-pick / copy commits)

This is conceptually similar to git branching but for live data, not source code.

## Relationship to Existing Work

- Builds on the existing branch infrastructure (`{env}-{schemaHash}-{userBranch}`)
- Related to `magic_filters_time_travel_branches.md` (time-travel reads are a prerequisite for point-in-time snapshots)

## Open Questions

- Snapshot implementation: full copy, or CoW (copy-on-write) with shared history?
- How to "merge" staging back into prod? (Diff-based? Commit replay?)
- Conflict resolution when prod has diverged since the snapshot
- How does this interact with schema migrations? (Staging might have a different schema version)
- Storage cost: CoW snapshots are cheap, full copies are expensive — which to support?
