# Explicit Indices

## What

Developer-declared indices in the schema language, replacing auto-index-all-columns.

## Notes

- Auto-indexing every column does not scale. Explicit indices align with relational norms and guarantee better performance characteristics.
- Compound indices are especially important for permission queries such as `organization_id + created_at`.
- Main consumers are app developers writing schemas and queries.
- Scope: single-column and compound indices, FK columns indexed by default, index rebuild for existing data, and slow-query warnings for full table scans.
- Open questions: syntax, lens interaction, per-branch indices, and rebuild strategy. Smart automatic indices are deferred.
