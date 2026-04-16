# `create-jazz` Scaffolder + Next Starters — TODO (MVP)

Add a guided entry point for new Jazz projects: `npm create jazz` scaffolds a runnable Next.js app with a working todo UI, wired-up auth, permissions, and zero-config local sync.

- Ship a published `create-jazz` npm package powering `npm create jazz`.
- Ship two starter templates — `next-betterauth` (BetterAuth email/password accounts) and `next-localfirst` (anonymous-first local-first identities, with an optional BetterAuth upgrade path behind an env var flag).
- Keep each starter small enough that the developer can see every file and understand what it does.
- Use the `withJazz` Next plugin from `jazz-tools/dev/next` to spawn a managed local sync server and inject runtime config automatically on `next dev`. No `instrumentation.ts` needed.
- Resolve `workspace:*` and `catalog:` dependency references to published versions at clone time, not at `create-jazz` publish time. Workspace packages may live under `packages/` or `crates/` (e.g. `jazz-napi`).

The reserved `create-jazz` npm package name means `npm create jazz` / `pnpm create jazz` / `yarn create jazz` all resolve to this package automatically.

## Goals

- Make first-run friction as close to zero as possible: one command, one prompt, one `npm install && npm run dev` away from a working Jazz app.
- Give developers a known-good starting shape for Next + BetterAuth that they can build on, not a demo they have to reverse-engineer.
- Keep the starter small enough that it's obvious where every file comes from and what it does.
- Decouple the scaffolder's release cadence from the starter's dependency versions by resolving deps at clone time.
- Provide a clean swap point for the Jazz-backed BetterAuth adapter (`feat/better-auth-adapter`) when it lands: a single line change, not a rewrite.

## Non-Goals

- Framework variety beyond Next.js. A SvelteKit retrofit of the env-bootstrap pattern is tracked on a separate branch.
- Tailwind, styling, UI libraries, or component kits. Each starter ships plain CSS sufficient to make the demo legible.
- Roles, seeded users, or any demo data beyond a single `todos` table.
- Subsuming or overlapping with the `jazz-tools` CLI or the planned `jazz dev` / `jazz deploy` commands. `create-jazz` is purely a scaffolder.
- Changes to the existing `examples/auth-betterauth-chat` example. It stays as-is.
- Shipping documentation for the BetterAuth adapter's schema-gen and migration lifecycle interop. That belongs to `feat/better-auth-adapter`.
- Offline template fallbacks or bundled-in-package templates. The scaffolder fetches from `garden-co/jazz2` main over the network.

## File Layout

Two new top-level directories in the monorepo:

```text
packages/
  create-jazz/
    bin/create-jazz.js
    src/
      index.ts
      scaffold.ts
      resolve-deps.ts
      resolve-remote-deps.ts
      detect-pm.ts
    package.json
    tsconfig.json

starters/
  next-betterauth/
    app/
      layout.tsx
      page.tsx
      dashboard/layout.tsx
      dashboard/page.tsx
      api/auth/[...all]/route.ts
    src/
      components/todo-widget.tsx
      lib/auth.ts
      lib/auth-client.ts
    scripts/init-env.mjs
    schema.ts
    permissions.ts
    proxy.ts
    next.config.ts
    package.json
    .gitignore
    README.md

  next-localfirst/
    app/
      layout.tsx
      page.tsx
      providers.tsx
      signin/page.tsx
      signin/sign-in-form.tsx
      signup/page.tsx
      signup/sign-up-form.tsx
      api/auth/[...all]/route.ts
    src/
      components/todo-widget.tsx
      lib/auth.ts
      lib/auth-client.ts
    scripts/init-env.mjs
    schema.ts
    permissions.ts
    next.config.ts
    package.json
    .gitignore
    README.md
```

The `starters/` directory is a workspace root. Both starters are workspace members and runnable in place with `pnpm dev`. The scaffolder's local-fetch copy path filters out `node_modules`, `.next`, `.jazz`, `.turbo`, and `.env.local` so contributor dev artefacts never leak into locally-scaffolded projects.

## The Starters

Both starters are real Next.js apps that run in place inside the monorepo using `workspace:*` and `catalog:` references. Running `pnpm dev` inside either starter produces a working app end-to-end with no additional setup. When fetched by `create-jazz`, workspace references are rewritten to published versions before the files land in the user's target directory. The user never sees `workspace:*` in their scaffolded `package.json`.

Both starters share a common shape:

- A single `todos` table in `schema.ts` and `$createdBy: session.user_id` row-level permissions in `permissions.ts`. This is deliberate pedagogy — developers see a working example of the data model the moment they open the code — not a non-goal to defer to "larger starters".
- A `TodoWidget` React component under `src/components/` that queries, inserts, toggles, and deletes todos using `jazz-tools/react`. Plain CSS, no UI framework.
- A `scripts/init-env.mjs` wired to `postinstall` in `package.json`. The script idempotently generates a `.env.local` with a fresh `BETTER_AUTH_SECRET` (32 random bytes, base64url via `node:crypto`). It exits without touching the file if it already exists.
- The `withJazz` Next.js plugin from `jazz-tools/dev/next`, wired in `next.config.ts`. This plugin spawns a managed local Jazz sync server via NAPI `DevServer.start` on first `next dev`, and injects `NEXT_PUBLIC_JAZZ_APP_ID` and `NEXT_PUBLIC_JAZZ_SERVER_URL` into `process.env` at runtime. There is no `instrumentation.ts` file; the plugin does all the boot/push/watch work the old spec expected an instrumentation hook to do.
- `.env.local` is gitignored. The scaffolder's copy path also excludes it so a contributor's locally-generated secret can never leak into a scaffolded project.

### `starters/next-betterauth`

The BetterAuth-gated flavour. Homepage is a sign-in / sign-up form (`app/page.tsx`); successful auth redirects to `/dashboard`, which mounts `JazzProvider` with a JWT fetched from BetterAuth's `jwtClient()` and renders `TodoWidget`. The `proxy.ts` middleware gates both directions (redirect to `/dashboard` when signed in, to `/` when not).

BetterAuth is configured with `memoryAdapter`, `nextCookies()`, `bearer()`, and `jwt({ jwks: { keyPairConfig: { alg: "ES256" } } })`. The JWKS endpoint at `/api/auth/jwks` is what the local Jazz sync server polls to verify tokens. No `admin` plugin, no roles, no seeded users.

### `starters/next-localfirst`

The local-first flavour. Every visitor gets a stable Ed25519 identity from their first page load — no sign-in wall, no sign-up ceremony. Homepage (`app/page.tsx`) mounts `JazzAuthProvider` (in `app/providers.tsx`) and renders `TodoWidget` directly. `JazzAuthProvider` resolves its `DbConfig` via `BrowserAuthSecretStore.getOrCreateSecret()` and passes `auth: { localFirstSecret }` to `JazzProvider`.

A single environment variable, `NEXT_PUBLIC_ENABLE_BETTERAUTH=1`, flips the starter into a "real accounts with identity continuity" mode:

- `JazzAuthProvider` splits into two components at module load; the flag-on branch consults `authClient.useSession()` and fetches a JWT when a BetterAuth session is present.
- `/signup` and `/signin` routes render forms (server component calls `notFound()` when the flag is off).
- Sign-up mints a short-lived proof token (`db.getLocalFirstIdentityProof({ audience: "next-localfirst-signup" })`) and passes it to `authClient.signUp.email`. The server verifies it via `verifyLocalFirstIdentityProof` from `jazz-napi` and pins the new BetterAuth `user.id` to the proven Jazz user id via `databaseHooks.user.create.before`, so the user's anonymous todos carry over after sign-up with no data migration.
- `/api/auth/[...all]/route.ts` short-circuits with a 404 when the flag is off; BetterAuth code is always present but dormant.
- Header gains "Sign up to access from any device" and "Sign in" links when the flag is on and the user is anonymous; they become user email + sign-out button when signed in. Sign-out calls `authClient.signOut()` then clears the `BrowserAuthSecretStore` so the next render mounts a fresh anonymous identity.

`better-auth` is listed as a regular dependency even when the flag is off so flipping it requires only a dev-server restart, not a second install. `jazz-napi` is also a workspace dep (it lives under `crates/`, not `packages/` — see [Dependency Resolution](#dependency-resolution-at-clone-time)).

### Env Bootstrap (both starters)

`scripts/init-env.mjs` is wired to `postinstall` in each starter's `package.json`. On first install it writes `.env.local`:

- `next-betterauth`: just `BETTER_AUTH_SECRET`.
- `next-localfirst`: `BETTER_AUTH_SECRET` plus `NEXT_PUBLIC_ENABLE_BETTERAUTH=0`.

Re-running `pnpm install` with `.env.local` already present is a no-op — the script exits without touching the file so existing secrets are never overwritten. Both starters' `src/lib/auth.ts` reads `BETTER_AUTH_SECRET` from `process.env` and throws at module-load time with a helpful error if missing, pointing the developer at `pnpm install`.

There is no `JAZZ_APP_ID` or `JAZZ_ADMIN_SECRET` in `.env.local`. The `withJazz` plugin injects `NEXT_PUBLIC_JAZZ_APP_ID` and `NEXT_PUBLIC_JAZZ_SERVER_URL` at runtime from its managed dev server, so the user does not have to generate or manage these values.

### Dependencies

In the workspace form (`next-betterauth` shown; `next-localfirst` is the same plus `better-auth` and `jazz-napi`):

```json
{
  "dependencies": {
    "better-auth": "^1.5.5",
    "jazz-tools": "workspace:*",
    "next": "16.2.1",
    "react": "19.2.4",
    "react-dom": "19.2.4"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "@types/react": "^19.0.0",
    "@types/react-dom": "^19.0.0",
    "typescript": "catalog:default"
  }
}
```

At clone time the scaffolder rewrites `workspace:*` and `catalog:default` entries to real published versions. See [Dependency Resolution](#dependency-resolution-at-clone-time).

## The Scaffolder: `packages/create-jazz`

### Package Shape

- Name: `create-jazz` (matches the reserved npm package name).
- Version: `0.0.1`.
- Published (not private) — `npm create jazz` requires a real package on the registry.
- Bin entry: `bin/create-jazz.js` → compiled `dist/index.js`.
- TypeScript, compiled to `dist/` via the standard monorepo build pipeline.
- Runtime dependencies: `@clack/prompts`, `tiged`, `picocolors`, `yaml`.

Pragmatic dependency philosophy: a few well-chosen libraries rather than zero-deps hand-rolled. These four libraries are lightweight, maintained, and what most modern scaffolders use.

### CLI Flow

```text
$ npm create jazz
◇ What's your app called?
│ my-app
◇ Scaffolding into ./my-app ...
◇ Fetching starter template ...
◇ Resolving workspace dependencies ...
◇ Generating dev secrets ...
◇ Initialising git repository ...
◇ Installing dependencies with pnpm ...
◇ Done.
│
│ Next steps:
│   cd my-app
│   pnpm dev
```

Concretely:

1. Parse the app name from `argv`, or prompt via `@clack/prompts` if not provided. Validate the app name against npm's valid package-name rules; reject whitespace, slashes, and other invalid characters before any fs work.
2. Parse `--starter <name>` from `argv`. If omitted and running in an interactive TTY, prompt the user to pick from `KNOWN_STARTERS`. If omitted and not interactive, default to `next-betterauth`. Unknown names fail fast with a clear error before any fs work.
3. Validate target directory: bail if it exists and is non-empty.
4. `tiged` fetch `garden-co/jazz2/starters/<selected-starter>` from `main` into the target directory.
5. Fetch and shape-validate `pnpm-workspace.yaml` from the repo root to build a catalog lookup. An HTML 404 page served as 200 is rejected with a clear source-named error.
6. For each `workspace:*` / `workspace:^` / `workspace:~` dep in the fetched `package.json`, fetch that package's `package.json` from the repo and read its `version` field. Try `packages/<name>/package.json` first, then `crates/<name>/package.json`. See [Dependency Resolution](#dependency-resolution-at-clone-time).
7. Rewrite workspace and catalog references in the scaffolded `package.json` to real published versions.
8. Set the `name` field in `package.json` to the user-chosen app name.
9. `git init` in the target directory, create an initial commit with the scaffolded files. `.env.local` is not yet present at this point, so the initial commit is free of any generated secret.
10. Detect the invoking package manager (see below). If detected, run its install command in the target directory. If not, skip install. The install step runs the starter's `postinstall` hook, which invokes `scripts/init-env.mjs` to generate `.env.local` with a fresh `BETTER_AUTH_SECRET`. The secret is never written to the initial commit.
11. Print next steps, using the detected package manager's command form (`{pm} dev`). If install was skipped, include the install step in the printed next steps along with a note that `.env.local` will be generated at that time.

### Package Manager Detection and Install

Detect the invoking package manager via `process.env.npm_config_user_agent`, which pnpm, yarn, npm, and bun all populate with a string like `pnpm/9.15.0 npm/? node/v22.0.0 darwin x64`. Parse the prefix to get the name.

Detection outcomes and behaviour:

- Detected `pnpm` → run `pnpm install`, print `cd my-app && pnpm dev`.
- Detected `yarn` → run `yarn install`, print `cd my-app && yarn dev`.
- Detected `bun` → run `bun install`, print `cd my-app && bun dev`.
- Detected `npm` → run `npm install`, print `cd my-app && npm run dev`.
- Not detected (missing or unrecognised user agent) → skip install entirely, print the full `cd my-app && npm install && npm run dev` sequence with a note that the user can substitute their own package manager.

The install step is the only reason detection matters. The printed next steps in the detected case are a convenience; users can always use a different package manager for the dev step if they want.

### Starter Selection

Two starters are registered in `KNOWN_STARTERS`: `next-betterauth` (the default) and `next-localfirst`. Selection happens via:

- `--starter <name>` CLI argument, when provided.
- An interactive `select()` prompt from `@clack/prompts` when no argument is passed on a TTY.
- Silent default to `next-betterauth` when no argument is passed and there is no TTY (CI / scripted usage).

An unknown starter name fails with a clear error before any filesystem work. Adding a future starter is a two-line change: add the name to `KNOWN_STARTERS` and ensure the starter directory exists under `starters/`.

### Dependency Resolution at Clone Time

The starter's `package.json` uses `workspace:*` and `catalog:` references because it's a workspace member. The user's scaffolded project cannot, because they're not in a pnpm workspace. The scaffolder rewrites these at clone time by fetching repo metadata directly from `garden-co/jazz2`.

Two protocols must be handled. Anything else in the dep value is an unknown form and should fail loudly.

**Workspace protocol** — any value starting with `workspace:`. Supported forms:

- `workspace:*` → `^{resolvedVersion}`
- `workspace:^` → `^{resolvedVersion}`
- `workspace:~` → `~{resolvedVersion}`
- `workspace:<exact>` → `{exact}` (no rewrite of the version part, just strip the `workspace:` prefix)

For each workspace-protocol dep, fetch `packages/{name}/package.json` from the repo and read its `version` field. If the package is not present under `packages/`, fall back to `crates/{name}/package.json`. This second location matters for `jazz-napi`, which lives in `crates/` because it ships a Rust NAPI binding, but is published to npm like any other workspace package.

**Catalog protocol** — any value starting with `catalog:`. Supported forms:

- `catalog:default` → the version from `pnpm-workspace.yaml`'s `catalogs.default.{depName}`
- `catalog:<name>` → the version from `pnpm-workspace.yaml`'s `catalogs.<name>.{depName}`

Parse `pnpm-workspace.yaml` once at clone start and use it to resolve all catalog references. For this MVP the monorepo only uses `catalog:default`, but the scaffolder should handle arbitrary catalog names if they exist.

**Unknown forms** — any dep value starting with `workspace:` or `catalog:` whose form doesn't match the above, or any other protocol-looking value the scaffolder doesn't recognise, fails loudly with a message naming the dep and the unrecognised value. Better to stop and fix the scaffolder than silently produce a broken `package.json`.

Resolution happens at clone time, so `create-jazz` never ships stale version metadata. Old releases of `create-jazz` work as long as the starter still exists at `starters/next-betterauth` on `main`.

Use caret ranges by default for `workspace:*` and `workspace:^`. This matches `create-next-app` and lets users pick up compatible patches on install.

### Error Surfaces

- Network failure during `tiged` fetch — wrap with a clear message pointing at connectivity or GitHub rate limits, not a raw stack trace.
- Network stall during remote dependency resolution — `resolveRemoteDeps` aborts any `fetch` that does not return within 30 seconds via `AbortSignal.timeout`, surfacing the stall as an explicit error rather than hanging the CLI.
- Invalid app name — reject whitespace, slashes, leading dots, and anything else that would produce an invalid `package.json` `name` field. Check before any filesystem work.
- Target directory exists and is non-empty — bail before fetching.
- Unknown starter passed to `--starter` — fail fast with a clear error listing `KNOWN_STARTERS`, before any filesystem work.
- `pnpm-workspace.yaml` parse failure — a corrupt upstream (or a GitHub HTML 404 page served as 200) is unrecoverable; the shape-validator refuses non-object parses and surfaces a source-named error.
- A `workspace:*` dep whose referenced package is missing from both `packages/` and `crates/` in the repo — fail with a message naming the missing package.
- Unrestricted `pm` values — `scaffold()` validates `pm` against a fixed allow-list (`npm`, `pnpm`, `yarn`, `bun`) before spawning the install step, so external callers cannot inject shell commands via the pipeline API.
- Install-step failure — the `pm install` invocation is wrapped in its own try-block so a failing install surfaces a friendly error message rather than leaking a raw `execSync` stack trace. The target directory is intentionally preserved on install failure so the user can retry manually; this is asymmetric with the pre-install pipeline which rolls back on any failure.

### The Swap-Point Comment

The `memoryAdapter` line in `src/lib/auth.ts` has a `// TODO` comment that points at the future Jazz-backed BetterAuth adapter. The wording is for Joe's own use pre-launch and will be updated before the package ships, so the final text does not need to be finalised in this spec. A placeholder comment is sufficient.

## Relationship to Other Work

- `feat/better-auth-adapter` (Matteo): provides the Jazz-backed BetterAuth adapter. This spec is deliberately unblocked from that branch by using `memoryAdapter` and a single swap-point comment. When the adapter lands, the starter's `src/lib/auth.ts` gets a one-line change.
- `packages/jazz-tools/src/dev/next.ts` + `packages/jazz-tools/src/dev/vite.ts`: provide the zero-config dev server plugins for Next and Vite. Both starters use the Next plugin via `withJazz` in `next.config.ts`.
- `specs/todo/b_launch/cli_and_dev_workflow.md`: reserves the `jazz` binary namespace (`jazz dev`, `jazz deploy`, `jazz migrate`, etc.). `create-jazz` is intentionally separate and narrower — scaffolding only, no ongoing dev loop commands.
- `examples/auth-betterauth-chat`: the existing Next + BetterAuth example showcasing roles, permissions, and multi-channel chat. It remains as a showcase. The starter is a new, much smaller artefact — not a trimmed fork.
