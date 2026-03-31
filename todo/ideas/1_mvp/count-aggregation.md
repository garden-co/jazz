# COUNT Aggregation

## What

Add terminal `.count()` queries for filtered relations, with the MVP limited to reactive `COUNT(*)` returning `{ count: number }`.

## Why

Apps need total counts for pagination and filtered views, and this is the smallest aggregation slice that fits Jazz's relational model.

## Who

App developers building list and pagination UIs.

## Rough appetite

big

## Notes

Count should follow the filtered relation before projection, ordering, and pagination. MVP should keep sync object-centric and derive `{ count }` locally from existing query subscriptions rather than adding aggregate-only replication. Execution should count tuple membership incrementally and avoid row materialization on simple paths. Non-goals: `COUNT(column)`, `COUNT(DISTINCT ...)`, and `GROUP BY`.
