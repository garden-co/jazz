# Jazz Application

This project uses [Jazz](https://jazz.tools) — a local-first database with real-time sync.

## Looking up Jazz APIs

Jazz is actively developed. Always fetch the live docs before answering Jazz-related questions — do not rely on training data alone.

**Step 1:** Fetch the page index to identify relevant pages:

```
https://jazz.tools/llms.txt
```

**Step 2:** Fetch the relevant page by appending `.mdx` to its path, e.g.:

```
https://jazz.tools/docs/schemas/defining-tables.mdx
```

Only fetch the pages you actually need.

### Topic areas in the docs

- **Getting started** — client and server setup
- **Quickstarts** — client quickstart, TypeScript server quickstart
- **Schemas** — defining tables, column types, relations, migrations
- **Reading** — queries, subscriptions, filters, sorting, pagination
- **Writing** — insert, update, delete, durability tiers, files and blobs
- **Auth** — authentication modes, sessions, row-level permissions
- **Concepts** — local-first data model, how sync works, branches
- **Recipes** — user-owned data, shared access, real-time collaboration, nested data
- **Reference** — column types, operators, framework patterns, FAQ
