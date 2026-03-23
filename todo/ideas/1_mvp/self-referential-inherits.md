# Self-Referential INHERITS

## What

Support bounded self-referential INHERITS relationships (e.g., a folder inheriting permissions from its parent folder).

## Why

Hierarchical data (folders, org charts, comment threads) needs permissions to flow up/down the tree. Without self-referential INHERITS, each level requires manual policy duplication.

## Who

App developers modeling hierarchical/tree-structured data with inherited permissions.

## Rough appetite

medium

## Notes

Partially implemented: cycle validation (`validate_no_inherits_cycles`) and `Gather` with `max_depth` exist. What remains: full recursive query execution for self-referential chains, bounded unrolling with configurable max depth, and integration tests proving permission inheritance through multi-level hierarchies.
