# Docs MCP Server — TODO (MVP)

An MCP server built into the `jazz-tools` CLI that lets AI agents search and read Jazz documentation via tool calls.

## Motivation

Agents working on Jazz projects need ergonomic access to the docs. Today they either get the full `llms-full.txt` dump (wastes context) or nothing. An MCP server exposes targeted search and retrieval as tool calls — agents get exactly the docs they need.

## Key constraints

Listed up front because they drive every design decision below.

- **Zero npm dependencies.** The MCP protocol handler, search backends, and all glue code are self-contained. No `@modelcontextprotocol/sdk`, no `better-sqlite3`, nothing.
- **`node:sqlite` for search, not speed.** We use the Node built-in SQLite module (`node:sqlite`, stable from 22.13). It's not the fastest SQLite driver — `better-sqlite3` is significantly faster. We chose it anyway because it's zero-install: no native addon compilation, no postinstall scripts, no platform-specific binaries. For a corpus of low-hundreds of docs pages, query performance is irrelevant — even the naive fallback responds in single-digit milliseconds.
- **No Node version bump.** `engines` stays at `>=20`. The SQLite backend is a runtime enhancement, not a requirement. Nodes without `node:sqlite` get the naive text backend automatically.
- **Graceful degradation.** The naive backend is always available. The SQLite backend activates only when `node:sqlite` is importable and FTS5 works. The tool interface is identical either way.
- **Index is pre-built.** No runtime index construction. Artifacts ship ready to query.
- **Stays in Node.** The `mcp` subcommand is handled entirely in TypeScript/JavaScript — it never spawns the Rust binary.

## Invocation

```bash
npx jazz-tools mcp
```

Because `npx` fetches the package on demand, this works outside Jazz projects without a global install. Cold start is a one-time cost per npx cache window; MCP clients keep the server process alive for the session.

### MCP client configuration

```json
{
  "mcpServers": {
    "jazz-docs": {
      "command": "npx",
      "args": ["jazz-tools", "mcp"]
    }
  }
}
```

## Architecture

### Whole-page storage, section-level search

The docs corpus is small — low-hundreds of pages, each a few KB of rendered text. We store every page's full content in the database rather than reading from disk at query time. This keeps the architecture simple (one artifact, no path resolution) and the corpus easily fits in a single SQLite file under 5 MB.

The FTS5 index is built over sections (split on `## ` headings) for ranked search, but `get_doc` returns the full page body from a separate `pages` table. No file I/O at query time.

```
┌─────────────────────────────────────────┐
│  docs-index.db                          │
│                                         │
│  pages (title, slug, description, body) │  ← full page content
│  sections_fts (title, slug,             │  ← FTS5 virtual table
│    section_heading, body)               │     for ranked search
└─────────────────────────────────────────┘
```

### Backend selection at startup

The server probes for `node:sqlite` at startup via dynamic import. If available, it uses FTS5 over the pre-built database. If not, it falls back to naive text search over the same content bundled as plain text.

```
startup
  │
  ├─ try dynamic import("node:sqlite")
  │   ├─ success → probe FTS5 (CREATE VIRTUAL TABLE _probe USING fts5(x))
  │   │   ├─ success → SQLite backend
  │   │   └─ failure → naive backend + warning
  │   └─ failure → naive backend + warning
  │
  └─ naive backend emits to stderr once:
     "node:sqlite not available — using basic text search.
      Upgrade to Node >=22.13 (current LTS) for better results."
```

### SQLite backend

- Opens the pre-built `.db` file (shipped in the npm package)
- `pages` table: `(title TEXT, slug TEXT PRIMARY KEY, description TEXT, body TEXT)` — stores full rendered page content; `description` is from frontmatter or first three sentences of body
- `sections_fts` FTS5 virtual table: `(title, slug, section_heading, body)` — one row per `## ` section, used for ranked search
- Tokenizer: `unicode61` (FTS5 default) — handles Unicode normalisation, ASCII folding, and is sufficient for English-language technical docs. No custom tokeniser config needed.
- Search queries use FTS5 `MATCH` with `bm25()` ranking and `snippet()` for context
- `get_doc` reads directly from the `pages` table — no FTS involved

### Naive backend

- Loads a pre-built text file (the same content that populates the SQLite DB, but as plain text with heading markers)
- Splits into sections on heading boundaries at startup, holds in memory
- Case-insensitive substring matching across all query terms
- Returns sections where all terms appear, ranked by term frequency
- Functional but noisier — no stemming, no ranking sophistication
- `get_doc` returns the full page text between page-level heading markers

### MCP protocol

The server speaks MCP JSON-RPC over stdio. Hand-rolled, no SDK dependency. The protocol surface is small for a read-only tool server:

**Lifecycle messages:**

- `initialize` — respond with server info and capabilities (`tools`)
- `initialized` — no-op notification
- `ping` — respond with empty result

**Tool messages:**

- `tools/list` — return tool definitions
- `tools/call` — dispatch to tool handler

**Transport:** newline-delimited JSON on stdin/stdout. Errors and warnings go to stderr (MCP convention).

### Tools exposed

#### `search_docs`

Search the Jazz documentation.

| Parameter | Type   | Required | Description                                                           |
| --------- | ------ | -------- | --------------------------------------------------------------------- |
| `query`   | string | yes      | Search query (FTS5 syntax when available, plain keywords in fallback) |
| `limit`   | number | no       | Max results (default 10)                                              |

Returns an array of results, each with: `title`, `slug`, `section`, `snippet`, `relevance` (float, SQLite backend only).

#### `get_doc`

Retrieve the full content of a documentation page.

| Parameter | Type   | Required | Description                                              |
| --------- | ------ | -------- | -------------------------------------------------------- |
| `slug`    | string | yes      | Page slug (e.g. `"reading-data"`, `"quickstarts/react"`) |

Returns the full rendered text of the page (with `<include>` directives resolved to actual code), plus a `related` field: an array of slugs for pages that share significant FTS terms with the requested page. Computed at query time via a second FTS5 `MATCH` query against the page's own title and top section headings. Capped at 5 results. In the naive backend, `related` is always an empty array.

#### `list_pages`

List all available documentation pages.

No parameters. Returns an array of `{ title, slug, description }` for every page in the index. Useful for agents that want to browse or discover relevant pages before fetching content.

**`description` field:** populated from MDX frontmatter `description` field if present. Fallback: first three sentences of the rendered page body (trimmed). Three sentences gives enough context for an agent to judge relevance without being too aggressive a trim. Stored in the `pages` table as a `description TEXT` column (populated at build time, not derived at query time).

## Index build pipeline

A build-time script produces the search index artefacts from the docs source.

### Input

The docs MDX content in `docs/content/`, with `<include>` directives resolved to actual code snippets from the example projects. This is the same content pipeline that produces `/llms-full.txt`.

### Processing

1. Render each MDX page to plain text (strip JSX components, resolve includes)
2. Store the full rendered page in the `pages` table
3. Split each page into sections on `## ` heading boundaries
4. Each section becomes a row in `sections_fts`: `(title, slug, section_heading, body)`

### Output

Two artefacts, **committed to the repo**:

- `docs-index.db` — SQLite database with `pages` table and `sections_fts` FTS5 virtual table
- `docs-index.txt` — Plain text file with heading markers for the naive backend

Both ship inside the npm package.

**Decision: commit the artefacts.** The `.db` file will be under 5 MB for our corpus size. Committing keeps local dev simple (clone and go), avoids CI build-order dependencies, and makes the index inspectable in review. The trade-off is binary churn in git history, but at this file size it's negligible. Worth revisiting if the corpus grows past ~1000 pages.

### When to rebuild

The index is rebuilt as part of the docs build pipeline. If the docs content changes, the index must be regenerated before publishing. This can be a script in `packages/jazz-tools/scripts/` invoked from the publish workflow, or integrated into the turbo pipeline.

## File layout

```
packages/jazz-tools/
├── bin/
│   ├── jazz-tools.js          # Add "mcp" command branch (stays in Node)
│   ├── docs-index.db          # Pre-built FTS5 database (pages + sections)
│   └── docs-index.txt         # Plain text fallback index
└── src/
    └── mcp/
        ├── server.ts          # MCP JSON-RPC stdio transport
        ├── tools.ts           # Tool definitions and handlers
        ├── backend-sqlite.ts  # SQLite + FTS5 search backend
        ├── backend-naive.ts   # Plain text search fallback
        └── build-index.ts     # Script to build index artefacts from docs
```

## JS shim change

In `bin/jazz-tools.js`, add a branch before the Rust binary dispatch:

```js
if (command === "mcp") {
  // MCP server runs in Node — don't spawn the Rust binary
  const mcpPath = join(here, "..", "dist", "mcp", "server.js");
  await import(mcpPath);
  // server.js takes over stdin/stdout, process exits when client disconnects
} else {
  // existing: spawn Rust binary
}
```

## Deferred

- **`search_docs` section filter** — skip for v1. Agents can use `list_pages` to discover slug structure and include section-specific terms in their queries. Add later if evidence emerges that agents need it.
