# CLI & Developer Workflow — TODO

A single CLI command for the full local development loop.

## Overview

Goal: a Convex-like experience where one command handles everything:

```bash
pnpx jazz dev
```

This should:

- Watch the schema file for changes
- Regenerate the TypeScript client (types, query builders, relations) on save
- Assist with migrations (generate lens, prompt for review)
- Push schema updates to the local sync server
- Start a local sync server if one isn't running

The generated client file should live **inside the project** (not in `node_modules`) for LLM accessibility and developer visibility.

## Related

- `ts_client.md` — the typed client/app surface that `jazz dev` ultimately feeds
- Schema lenses / migrations — `schema_manager.md`

## Additional CLI Commands

- `jazz deploy` — push schema + config to hosted infra
- `jazz migrate` — run pending migrations
- `jazz db` — open the database viewer (see `database_viewer_editor.md`)
- `jazz auth` — configure BetterAuth / WorkOS integration

## Open Questions

- How to handle breaking schema changes in dev mode? (Auto-reset local data? Prompt?)
- Hot module reload integration: can the app pick up new types without a full restart?
- Multi-schema support (multiple schema files in one project)?
- Should `jazz dev` also proxy/tunnel to a cloud sync server for mobile testing?
