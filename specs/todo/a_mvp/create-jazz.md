# `create-jazz` Scaffolder + Starters — TODO (MVP)

Add a guided entry point for new Jazz projects: `npm create jazz` scaffolds a runnable app with a working todo UI, wired-up auth, permissions, and zero-config local sync.

- Ship a published `create-jazz` npm package powering `npm create jazz`.
- Ship six starter templates across two frameworks (Next.js, SvelteKit) and three auth modes (local-first, hybrid, BetterAuth).
- Keep each starter small enough that the developer can see every file and understand what it does.
- Use the framework-appropriate dev plugin (`withJazz` for Next, `jazzSvelteKit()` for SvelteKit) to spawn a managed local sync server and inject runtime config automatically on dev. No manual instrumentation needed.
- Resolve `workspace:*` and `catalog:` dependency references to published versions at clone time, not at `create-jazz` publish time. Workspace packages may live under `packages/` or `crates/` (e.g. `jazz-napi`).

The reserved `create-jazz` npm package name means `npm create jazz` / `pnpm create jazz` / `yarn create jazz` all resolve to this package automatically.

## Goals

- Make first-run friction as close to zero as possible: one command, a framework and auth prompt, one `npm install && npm run dev` away from a working Jazz app.
- Give developers a known-good starting shape that they can build on, not a demo they have to reverse-engineer.
- Keep each starter small enough that it's obvious where every file comes from and what it does.
- Decouple the scaffolder's release cadence from the starter's dependency versions by resolving deps at clone time.
- Provide a clean swap point for the Jazz-backed BetterAuth adapter (`feat/better-auth-adapter`) when it lands: a single line change, not a rewrite.

## Non-Goals

- Tailwind, styling, UI libraries, or component kits. Each starter ships plain CSS sufficient to make the demo legible.
- Roles, seeded users, or any demo data beyond a single `todos` table.
- Subsuming or overlapping with the `jazz-tools` CLI or the planned `jazz dev` / `jazz deploy` commands. `create-jazz` is purely a scaffolder.
- Shipping documentation for the BetterAuth adapter's schema-gen and migration lifecycle interop. That belongs to `feat/better-auth-adapter`.
- Offline template fallbacks or bundled-in-package templates. The scaffolder fetches from `garden-co/jazz2` main over the network.

## File Layout

Two new top-level directories in the monorepo:

```text
packages/
  create-jazz/
    bin/create-jazz.js
    src/
      index.ts          # CLI entry: interactive prompts, arg parsing
      scaffold.ts       # Core scaffolding logic (fetch, resolve, git init)
      deps.ts           # workspace:/catalog: dep resolution (remote + local)
      detect-pm.ts      # Package manager detection from npm_config_user_agent
    package.json
    tsconfig.json

starters/
  next-betterauth/      # Next.js + BetterAuth (email + password, sign-up required)
  next-localfirst/      # Next.js + local-first (anonymous)
  next-hybrid/          # Next.js + local-first with optional BetterAuth upgrade
  sveltekit-betterauth/ # SvelteKit + BetterAuth
  sveltekit-localfirst/ # SvelteKit + local-first
  sveltekit-hybrid/     # SvelteKit + local-first with optional BetterAuth upgrade

scripts/
  check-starters-parity.mjs  # Drift detector (horizontal, cross-framework, README)
```

## The Starters

All six starters are real apps that run in place inside the monorepo using `workspace:*` and `catalog:` references. Running `pnpm dev` inside any starter produces a working app end-to-end with no additional setup. When fetched by `create-jazz`, workspace references are rewritten to published versions before the files land in the user's target directory.

All starters share a common shape:

- A single `todos` table in `schema.ts` and row-level permissions in `permissions.ts`. These files are byte-identical across all six starters (enforced by the parity check script).
- A `TodoWidget` component that queries, inserts, toggles, and deletes todos. Plain CSS, no UI framework. Byte-identical within each framework family.
- For BetterAuth and hybrid starters: a `scripts/init-env.mjs` wired to `postinstall` that idempotently generates `.env.local` with a fresh `BETTER_AUTH_SECRET`.
- `.env.local` is gitignored. The scaffolder's copy path also excludes it.

### Auth Modes

**Local-first** — every visitor gets a stable identity from their first page load. No sign-in, no sign-up. Data lives on-device only.

**Hybrid** — starts as local-first, but the user can optionally upgrade to a BetterAuth-managed account. Anonymous data carries over after sign-up.

**BetterAuth** — email + password sign-up required before access. BetterAuth configured with `memoryAdapter`, bearer tokens, and JWKS.

### Parity Enforcement

`scripts/check-starters-parity.mjs` runs as a lefthook pre-commit hook on any change under `starters/`. It checks:

- **Horizontal parity**: files that must be identical within each framework family (schema, permissions, todo widget).
- **Cross-framework parity**: schema and permissions must be identical across Next and SvelteKit.
- **README structure**: all six READMEs must contain the same required sections in the same order.
- **Shared README sections**: the "Extending the schema" section must be byte-identical across all six.

## The Scaffolder: `packages/create-jazz`

### CLI Flow

```text
$ npm create jazz
♪ Jazz
◇ What's your app called?  my-app
◇ Framework              React (Next.js) / Svelte (SvelteKit)
◇ Auth                   Local-first / Hybrid / BetterAuth
◇ Done.
│
│ Next steps:
│   cd my-app
│   pnpm dev
```

Flags:

- `--starter <name>` — skip the interactive picker entirely.
- `--no-git` — skip `git init` and the initial commit.

Non-TTY mode (piped stdin) defaults to `next-betterauth`.

### Dependency Resolution at Clone Time

Two protocols are handled:

**Workspace protocol** (`workspace:*`, `workspace:^`, `workspace:~`) — fetch the package's `package.json` from `packages/{name}/` or `crates/{name}/` in the repo and read its `version` field. Rewrite to a caret or tilde range.

**Catalog protocol** (`catalog:<name>`) — resolve against `pnpm-workspace.yaml`'s `catalogs` section.

Unknown forms fail loudly.

### Package Manager Detection

Detected via `process.env.npm_config_user_agent`. Recognised: `pnpm`, `yarn`, `npm`, `bun`. When detected, the scaffolder runs `{pm} install` and prints `{pm} dev` in the outro. When not detected, install is skipped and the generic `npm install && npm run dev` sequence is printed.

### Error Surfaces

- Network failure / stall — `fetchWithTimeout` aborts after 8 seconds; `fetchOnce` retries once with a warning on transient failure.
- Invalid app name — rejected before any filesystem work.
- Target directory exists — rejected before fetching.
- Unknown starter — fails fast with a list of valid starters.
- Corrupt `pnpm-workspace.yaml` — shape-validated, refuses non-object parses.
- Missing workspace package — named error after checking both `packages/` and `crates/`.
- Install failure — target directory preserved so the user can retry manually.
