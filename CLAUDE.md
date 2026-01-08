# Jazz2 (Groove)

This is a work-in-progress redesign of Jazz, a distributed database that syncs across frontend, backend, and cloud. The core library is called "Groove" and lives in the `groove/` directory.

**Status**: Active development. Many features are implemented but the system is not yet production-ready.

## Project Structure

```
groove/           # Core Rust library
groove-wasm/      # WASM bindings for browser
demo-app/         # Example React app using groove-wasm
specs/            # Design documents
```

## Design Documents

Read these to understand the architecture:

- [new-jazz-no-context.md](new-jazz-no-context.md) - Overview of the new Jazz design principles
- [from-legacy-to-new-jazz.md](from-legacy-to-new-jazz.md) - Why we're redesigning and what's changing
- [specs/sql-layer.md](specs/sql-layer.md) - SQL interface design and status
- [specs/incremental-queries.md](specs/incremental-queries.md) - Incremental query computation graphs
- [specs/streaming-and-persistence.md](specs/streaming-and-persistence.md) - Content storage and streaming
- [specs/rebac-policies.md](specs/rebac-policies.md) - ReBAC permission policies

## Key Concepts

- **Objects**: Fundamental unit, identified by ObjectId (UUIDv7 with Crockford Base32 encoding). Each object has a commit graph (git-like history).
- **Tables/Rows**: Each table row is an Object with its own commit history, enabling fine-grained sync and per-row conflict resolution.
- **SQL Interface**: CREATE TABLE, INSERT, UPDATE, SELECT with JOIN support. Queries can be incremental (only recompute affected parts on change).
- **Incremental Queries**: Computation graphs that propagate deltas efficiently instead of re-evaluating entire queries.

## Building

```bash
# Run all tests
cargo test

# Build WASM package
cd groove-wasm && wasm-pack build --target web

# Run demo app
cd demo-app && npm install && npm run dev
```

## Testing Guidelines

- Always check specific properties of collection items in tests, not just the collection length. For example, after asserting `rows.len() == 2`, also verify the actual row values like names, titles, or IDs.

## Documentation Guidelines

- **All code examples in docs must come from typechecked example apps whenever possible.** Use the `<include>` directive to pull code from `docs/examples/` rather than writing inline code snippets. This ensures examples are always valid and up-to-date with the actual API.
  - Example: `<include>../../../examples/react/src/components/TaskList.tsx#task-list-basic</include>`
  - Mark regions in example files with `// #region <name>` and `// #endregion <name>` comments
  - Exceptions: SQL schema examples, ASCII diagrams, and comparison snippets showing other products' APIs may be inline

## Code Quality Guidelines

- When taking any shortcut or simplification, loudly document it in: (1) code comments at the site, (2) a Linear issue for tracking, and (3) the final summary when completing a task.

## Module Overview

Key modules in `groove/src/`:

- `object.rs` - ObjectId type and object primitives
- `sql/` - SQL parser, database, schema, query execution
- `sql/query_graph/` - Incremental query computation system
- `storage.rs` - Content and commit storage traits
- `node.rs` - LocalNode for managing objects
- `listener.rs` - Synchronous callback subscriptions

## Linear Integration

**IMPORTANT**: All task tracking happens in Linear. When working on this project:
- **Create issues** for new tasks, bugs, and ideas
- **Update issue status** as you make progress
- **Log design decisions** and blockers in issue comments
- **Check existing issues** before starting work to avoid duplicates

Project: [Jazz2 prototype](https://linear.app/garden-co/project/jazz2-prototype-ad7779f29620)

### Setup

To interact with Linear via the GraphQL API, set the `LINEAR_API_KEY` environment variable:

```bash
# Add to ~/.zshrc or ~/.bashrc
export LINEAR_API_KEY="lin_api_..."
```

Get your API key from https://linear.app/settings/api

If a team member tries to interact with Linear but doesn't have this set up, walk them through creating an API key and adding it to their shell profile.

