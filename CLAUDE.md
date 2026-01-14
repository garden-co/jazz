# Jazz2 (Groove)

Distributed database syncing across frontend/backend/cloud. Core "Groove" in Rust, TypeScript bindings via WASM.

**Status**: Active development, not production-ready.

## Structure

```
crates/                # Rust workspace
  groove/              # Core library — see crates/groove/src/lib.rs
  groove-wasm/         # WASM bindings — see crates/groove-wasm/src/lib.rs
  groove-cli/          # CLI tools — see crates/groove-cli/src/main.rs
  groove-server/       # Server — see crates/groove-server/src/lib.rs
packages/              # TS packages
  jazz-client/         # Client — see packages/jazz-client/src/index.ts
  jazz-react/          # React hooks — see packages/jazz-react/src/index.ts
  jazz-schema/         # Schema codegen — see packages/jazz-schema/src/index.ts
examples/              # All example apps
  demo-app/            # React demo app
  sync-demo/           # Sync demonstration
  docs/                # Typechecked doc examples (symlinked to docs/examples)
docs/                  # Fumadocs site
```

Each entry point has module overview in its header comments.

## Building

```bash
cd crates && cargo test                                   # Rust tests
cd crates/groove-wasm && wasm-pack build --target web     # WASM
pnpm install && pnpm build                                # TS packages (required before docs build)
cd examples/demo-app && npm run dev                       # Demo app
```

**Important**: `pnpm build` at repo root before docs build — TS packages must be built for workspace resolution.

## Specs

Source of truth. Linear references specs, not vice versa.

**Architecture**: [new-jazz-no-context.md](new-jazz-no-context.md), [from-legacy-to-new-jazz.md](from-legacy-to-new-jazz.md)

**Deep dives** in [docs/content/docs/internals/](docs/content/docs/internals/):

| Spec | Status |
|------|--------|
| [SQL Layer](docs/content/docs/internals/sql-layer.mdx) | Implemented |
| [Incremental Queries](docs/content/docs/internals/incremental-queries.mdx) | Partial |
| [Streaming & Persistence](docs/content/docs/internals/streaming-and-persistence.mdx) | Partial |
| [ARRAY Subqueries](docs/content/docs/internals/array-subquery.mdx) | Implemented |
| [Binary Data & Blobs](docs/content/docs/internals/binary-data-and-blobs.mdx) | Implemented |
| [Deletes & Truncation](docs/content/docs/internals/deletes-and-truncation.mdx) | Implemented |
| [ReBAC Policies](docs/content/docs/internals/rebac-policies.mdx) | Planned |
| [Sorted Chunk Indices](docs/content/docs/internals/sorted-chunk-indices.mdx) | Future |
| [Multi-Row Transactions](docs/content/docs/internals/multi-row-transactions.mdx) | Future |
| [External Migrations](docs/content/docs/internals/external-data-migrations.mdx) | Future |

## Key Concepts

**Objects**: Fundamental unit, ObjectId (UUIDv7, Crockford Base32). Each has git-like commit graph.

**Tables/Rows**: Each row is an Object. Fine-grained sync, per-row conflict resolution.

**SQL**: CREATE TABLE, INSERT, UPDATE, SELECT with JOINs. Queries can be incremental.

**Incremental Queries**: Computation graphs propagate deltas instead of full re-evaluation.

## Guidelines

**Testing**: Assert concrete values, not just structure. `assert_eq!(user.name, "Alice")` not `assert!(user.name.len() > 0)`. Makes tests rigid and readable.

**Docs**: Code examples from `examples/docs/`. Use `<include>` with `// #region` markers. Exceptions: SQL, ASCII diagrams, comparison snippets.

**Shortcuts**: Document in (1) code comments, (2) Linear issue, (3) task summary.

**Generated code**: Never edit directly. Edit generators in `packages/jazz-schema/src/codegen/`.

## Linear

Project: [Jazz2 prototype](https://linear.app/garden-co/project/jazz2-prototype-ad7779f29620)

Issues reference specs. PRs link to both Linear and spec. Don't duplicate spec content.

**Setup**: `export LINEAR_API_KEY="lin_api_..."` from https://linear.app/settings/api

```bash
curl -s -X POST https://api.linear.app/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: $LINEAR_API_KEY" \
  -d '{"query": "query { issue(id: \"GCO-1071\") { id identifier title description state { name } } }"}'
```
