# Skill Issues Design

## Goal

Replace the tracked Markdown todo system with a Jazz Cloud-backed issue and idea manager for this repo.

Jazz Cloud is the source of truth. Markdown files remain available only as generated import/export compatibility artifacts and must not be tracked.

The primary interaction path is a repo-local Codex skill named `issues`, backed by a CLI. A local UI can be started for visual management, but the CLI must work without the UI server.

## Location

The implementation lives in `examples/skill-issues`.

This example contains:

- Shared Jazz schema and domain code.
- CLI entrypoint for all low-level operations.
- Optional local UI server.
- Trusted backend/verifier endpoint for GitHub identity binding.

The repo-local skill lives in `.agents/skills/issues/SKILL.md` and calls the CLI instead of editing Markdown files directly.

## Architecture

`examples/skill-issues` is CLI-first.

The CLI handles:

- Local-first auth initialization.
- GitHub verification flow.
- Import from current Markdown todo files.
- Export to current Markdown todo format.
- Add, list, show, assign, and status operations.
- Starting the optional local UI.

The UI is optional. It reads and writes the same Jazz Cloud data and enforces the same authorization rules, but the skill and CLI do not depend on the UI server running.

The trusted backend/verifier is required for identity binding. It performs GitHub OAuth, verifies that the caller controls a Jazz local-first principal, and writes the corresponding `User` row as a backend/trusted writer.

For the first implementation, the verifier runs as part of `issues serve` under dedicated auth routes. The CLI still does not depend on the UI being available for normal issue operations, but `issues auth github` requires a configured verifier URL.

## Data Model

Core items stay deliberately small:

```ts
Item {
  kind: "idea" | "issue";
  title: string;
  description: string;
  slug: string; // globally unique across ideas and issues
}
```

Workflow state is separate:

```ts
ItemState {
  itemSlug: string;
  status: "open" | "in_progress" | "done";
  assigneeUserId?: string;
}
```

Verified users are represented by a general `User` table:

```ts
User {
  id: string; // Jazz local-first principal id
  githubUserId: string; // immutable GitHub id
  githubLogin: string;
  verifiedAt: string;
}
```

`ItemState.assigneeUserId` references `User.id`.

## Authorization

All mutations require a verified GitHub identity.

A client is allowed to mutate `Item` and `ItemState` only when:

- It authenticates with a local-first Jazz identity.
- A `User` row exists whose `id` equals the current Jazz session user id.

Only the trusted backend/verifier can create or update `User` rows. Regular local-first users can read `User` rows but cannot create, update, or delete them.

The verifier writes `User.id = provedJazzUserId` only after both checks pass:

- GitHub OAuth resolves to a real GitHub account.
- The caller proves control of the Jazz local-first principal.

Import is a mutation and therefore requires a verified `User`.

Read/list/export operations can be available without verified identity if Jazz Cloud read access permits it, but any command that writes must fail with a clear instruction to run GitHub verification first.

## GitHub Verification Flow

The CLI command `issues auth github` starts the verification flow.

The intended flow is:

1. CLI has or creates a local-first secret.
2. CLI starts GitHub device authorization.
3. CLI sends the GitHub OAuth result plus a Jazz local-first proof to the verifier.
4. Verifier calls GitHub server-side to fetch the immutable GitHub user id and current login.
5. Verifier validates the Jazz proof and writes the `User` row through a backend Jazz context.
6. CLI confirms that the current Jazz user is now verified.

GitHub login is display data. Authorization must use the immutable GitHub id and Jazz user id.

## CLI

The CLI is the stable low-level interface:

```sh
issues auth init
issues auth github --verifier-url <url>
issues import todo/
issues export todo/
issues add idea <slug> --title ... --description ...
issues add issue <slug> --title ... --description ...
issues list [--kind issue|idea] [--status open|in_progress|done]
issues show <slug>
issues assign <slug> --me
issues status <slug> <open|in_progress|done>
issues serve
```

`issues assign <slug> --me` assigns the item to the current verified `User`. If the item is `open`, assignment should move it to `in_progress`.

`issues status <slug> done` clears no data by default. It only changes status.

All commands should return concise, scriptable output and useful human text. The skill may rely on command output rather than directly reading Jazz data.

## Skill Behavior

The `issues` skill replaces the current Markdown capture workflow.

The skill must:

- Use the CLI for all issue and idea operations.
- Never create or edit `todo/ideas/**/*.md` or `todo/issues/**/*.md` as source files.
- Recommend `issues export todo/` only when Markdown compatibility output is requested.
- Treat exported Markdown as generated/untracked.
- For capture, create a Jazz `Item` with status `open`.
- For self-assignment, call the CLI assignment command.
- For status updates, call the CLI status command.

## Markdown Import And Export

Import supports the current repo format:

- Ideas from `todo/ideas/{priority}/{slug}.md`.
- Issues from `todo/issues/{slug}.md`.

Current Markdown fields map as:

- Filename becomes `slug`.
- H1 becomes `title`.
- `## What` becomes `description`.
- Issue priority is ignored by the canonical model.
- Notes are not canonical in the KISS model and are not imported.

Export writes the same broad shape for compatibility:

- Ideas export under `todo/ideas/1_mvp/{slug}.md`.
- Issues export under `todo/issues/{slug}.md`.
- `description` exports under `## What`.
- Issue `## Priority` exports as `unknown`.
- `## Notes` exports empty.

The repo must ignore generated Markdown export paths so Jazz Cloud remains the source of truth.

## Cutover Plan

The implementation must remove the current tracked Markdown todo system, not run beside it.

Artifact fates:

- `todo/ideas/`: one-time imported into Jazz Cloud as `Item.kind = "idea"`, then removed from tracked source. Future Markdown under this path is generated export output only.
- `todo/issues/`: one-time imported into Jazz Cloud as `Item.kind = "issue"`, then removed from tracked source. Future Markdown under this path is generated export output only.
- `todo/projects/`: not part of the KISS item model. During cutover, each project directory must be reviewed explicitly. Active project intent becomes one or more Jazz `idea` or `issue` items; long-form project docs that still matter move out of `todo/` into an appropriate committed docs/spec location. After that review, `todo/projects/` is removed from tracked source.
- `TODO.md`: removed as a generated tracked summary. Listing is provided by `issues list`; Markdown export is on demand.
- `scripts/update-todo.sh`: removed. The replacement is `issues export todo/` for compatibility output and `issues list` for summaries.
- `CLAUDE.md` and `AGENTS.md` Quick Capture instructions: replaced with instructions to use the `issues` skill or CLI. They must no longer instruct agents to write Markdown under `todo/` or run `scripts/update-todo.sh`.
- `.gitignore`: updated so generated Markdown export output and local skill-issues auth/config/data are ignored.

The migration should happen in this order:

1. Implement enough schema and CLI to import current Markdown.
2. Verify import against the existing `todo/ideas/` and `todo/issues/` files.
3. Review `todo/projects/` and either convert each project to Jazz items or move durable docs out of `todo/`.
4. Update repo instructions and `.gitignore`.
5. Remove tracked `todo/`, `TODO.md`, and `scripts/update-todo.sh`.
6. Run `issues export todo/` once locally to verify compatibility output, confirm the exported files are ignored, then delete the local export unless needed for inspection.

## Configuration

Configuration is read from environment variables first, then from a local ignored config file created by `issues auth init`.

Required values:

- `SKILL_ISSUES_APP_ID`
- `SKILL_ISSUES_SERVER_URL`
- `SKILL_ISSUES_VERIFIER_URL` for `issues auth github`
- `GITHUB_CLIENT_ID` and `GITHUB_CLIENT_SECRET` for the verifier
- `SKILL_ISSUES_BACKEND_SECRET` for trusted backend writes

Local generated auth/config state must live under ignored local data paths, not tracked source files.

## Local UI

`issues serve` starts a local server for browsing and managing issues and ideas.

The UI should support:

- Listing ideas and issues.
- Filtering by kind and status.
- Viewing an item.
- Creating an item.
- Assigning the current verified user.
- Updating status.
- Export trigger.

The UI uses local-first auth and the same GitHub verification requirement as the CLI for writes.

## Testing

Prefer integration tests over helper-only unit tests.

Coverage should include:

- CLI import from realistic current Markdown fixtures.
- CLI export to the current Markdown shape.
- Cutover behavior for ignored generated export output.
- Global slug uniqueness across ideas and issues.
- Mutation commands fail before GitHub verification.
- Verified users can create items, assign themselves, and update status.
- Regular clients cannot create or update `User` rows.
- The verifier can create or update a `User` row after mocked GitHub OAuth and Jazz proof verification.
- UI smoke test for list, create, assign, and status update.
