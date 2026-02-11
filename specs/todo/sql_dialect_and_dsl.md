# SQL Dialect & DSL Positioning — TODO

Strategy for the query interface: modern DSL first, SQL as escape hatch.

## Overview

Team consensus: Jazz is **not** an SQL database. It's a local-first relational database with a modern, type-safe DSL. SQL exists as:

1. **Escape hatch** — for queries the DSL doesn't cover yet
2. **Wire protocol** — clients without native Jazz bindings can send SQL over HTTP
3. **LLM interface** — agents can read/write data via SQL strings
4. **Power user tool** — for ad-hoc exploration in the database viewer

The DSL (TypeScript schema + generated query builders) is the primary, recommended interface.

## SQL Dialect Scope

Follow PostgreSQL semantics where choices exist, but explicitly scope what we support:

- Basic CRUD: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- Joins (inner, left)
- Aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
- **Not supported (initially)**: subqueries, CTEs, window functions, stored procedures, triggers, custom functions

### Error Strategy

When users (or LLMs) try unsupported SQL features, return **clear, actionable errors**:

- "SUBSTRING is not supported. Use the DSL's string helpers instead: `col.startsWith(...)`"
- "Window functions are not available. Consider using a reactive query subscription for running totals."

This turns limitations into education opportunities.

## Risks to Mitigate

- **Comparison trap**: exposing SQL invites comparison to Postgres/MySQL. The DSL-first positioning avoids this.
- **Dialect confusion**: LLMs trained on Postgres/MySQL will generate incompatible SQL. Clear errors + DSL docs help.
- **Type safety gap**: SQL strings bypass the type-safe DSL. Parameterized queries prevent injection, but runtime type errors are possible.
- **Feature expectations**: "SQL database" implies feature parity with mature engines. "Local-first relational database" sets better expectations.

## Open Questions

- Should SQL be available in production apps, or only in dev tools / database viewer?
- SQL validation: parse-time (in editor) or runtime (at execution)?
- Can we provide an LSP / editor plugin for our SQL dialect?
- How to document the exact SQL subset we support? (Formal grammar?)
- Query builder libraries (Drizzle, Kysely) — can they target our dialect?
