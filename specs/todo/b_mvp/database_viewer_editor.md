# Database Viewer & Editor — TODO

Visual tool for inspecting and editing jazz2 database contents.

## Phasing

- **MVP**: Minimal read-only viewer — browse tables, schemas, rows. Enough for developers to see their data.
- **Launch**: Polished, full-featured editor with inline editing, history, sync state, query playground.

## MVP: Minimal Viewer

A basic web UI that connects to a running Jazz instance and shows:

- Table list and schema definitions
- Row data with filtering and sorting
- Enough to answer "what's in my database?" during development

Keep it simple — even a single-page app with a table grid is valuable.

## Launch: Full Editor

- Editing data inline (writes go through the normal sync path)
- Viewing object history and edit provenance
- Inspecting sync state (which peers have which data)
- Schema migration status and lens chains
- Query playground (run SQL, see results live with reactivity)
- Branch visualization (tree/graph of branches)

## Open Questions

- Standalone app vs. embedded in the developer dashboard?
- Built with jazz2 React bindings (dogfooding)?
- How to visualize merge conflicts and resolution?
- Can it connect to both local dev servers and hosted infra?
