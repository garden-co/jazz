# claude-sessions-backend

Sibling of `codex-sessions-backend`. Watches `~/.claude/projects/` Claude Code
transcripts, indexes session summaries into SQLite, and exposes a Unix-socket
protocol so Lin (and any other consumer) can treat Claude sessions the same
way Codex sessions are treated.

## Phase 1 (this directory)

- fs-watch of `~/.claude/projects/*/<uuid>.jsonl`
- per-session summary: id, cwd, projectRoot, gitBranch, firstUserMessage,
  latestUserMessage, latestAssistantMessage, turn counts, transcriptPath
- SQLite at `~/Library/Caches/Flow/claude-sessions.db`
- Unix socket at `~/Library/Application Support/Flow/claude-sessions.sock`
- JSON-per-line protocol matching the codex-sessions-backend shape:
  - `{ "method": "health" }`
  - `{ "method": "get-session", "sessionId": "<uuid>" }`
  - `{ "method": "list-sessions", "projectRoot": "/repo", "limit": 10 }`
  - `{ "method": "search-sessions", "query": "text", "limit": 20 }`
  - `{ "method": "list-recent", "limit": 20 }`

## Run

```bash
pnpm --dir examples/claude-sessions-backend build
node examples/claude-sessions-backend/dist/src/cli.js serve
```

Or with tsx:

```bash
pnpm --dir examples/claude-sessions-backend serve
```

## Test

```bash
pnpm --dir examples/claude-sessions-backend test
```

## Next phases

- Phase 2: Jazz2 CoValue schema + projector so Claude sessions sync through
  the Jazz2 network the same way Codex sessions do.
- Phase 3: Lin consumer `JazzClaudeSessionCompletionService` mirroring
  `JazzCodexSessionCompletionService`.
- Phase 4: unify into a single `agent-sessions-backend` once both providers
  share the same CoValue shape.
