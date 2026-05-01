# COUNT Aggregation

## What

Add terminal `.count()` queries for filtered relations, with the MVP limited to reactive `COUNT(*)` returning `{ count: number }`.

## Notes

- Apps need total counts for pagination and filtered views, and this is the smallest aggregation slice that fits Jazz's relational model.
- Main consumers are app developers building list and pagination UIs.
- Count should follow the filtered relation before projection, ordering, and pagination.
- MVP should keep sync object-centric and derive `{ count }` locally from existing query subscriptions rather than adding aggregate-only replication.
- Non-goals: `COUNT(column)`, `COUNT(DISTINCT ...)`, and `GROUP BY`.
