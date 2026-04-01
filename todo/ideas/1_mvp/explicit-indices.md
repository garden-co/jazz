# Explicit Indices

## What

Developer-declared indices in the schema language, replacing auto-index-all-columns.

## Why

Auto-indexing every column doesn't scale. Explicit indices align with relational norms and guarantee good performance. Compound indices are critical for permission queries (e.g. `organization_id` + `created_at`).

## Who

App developers writing schemas and queries.

## Rough appetite

big

## Notes

Scope: single-column, compound indices, FK columns indexed by default, index rebuild for existing data, slow query warnings for full table scans. Open questions around syntax, lens interaction, per-branch indices, and rebuild strategy. Smart automatic indices deferred to later.
