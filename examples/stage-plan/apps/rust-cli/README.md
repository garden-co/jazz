# StagePlan: Rust CLI

StagePlan from the command line, in Rust, with the `jazz-app-support` crate. It is a **backend client** of StagePlan's TypeScript main backend (`createJazz`): it connects to the main backend's `/jazz` with a machine credential (an OIDC token) that the main backend trusts through `trustWorkload`. As a **scoped** client it acts as one user (`session.user` in the permissions). As a **trusted** client it acts as the system, and for any user with `for_user`. It can:

- create the user's show, as its crew chief;
- list a show's tasks by rank;
- add a task to a stage;
- move a task between stages (`todo`, `doing`, `done`, `blocked`).

## Files

- `schema.ts`: re-exports [`../react-localfirst`](../react-localfirst)'s schema and permissions, so this is StagePlan's app.
- `jazz.app.json` and `jazz.permissions.json`: what `npx jazz-tools generate` writes from `schema.ts`, committed. Don't edit them; `npx jazz-tools generate --check` refuses stale ones.
- `src/jazz_app.rs`: the typed bindings `jazz-tools generate` writes from `jazz.app.json`, committed. Don't edit it. The `bindings_are_current` test and `npx jazz-tools generate --check` refuse a stale copy.
- `src/lib.rs`: the slice: `ensure_show`, `tasks_of`, `add_task` and `move_task`.
- `src/main.rs`: the `stage-plan` CLI over it.

## Running it

The main backend trusts the CLI's token in `createJazz`'s `backendClients`. See [Backend Client](https://jazz.tools/docs/setup/backend-client):

```ts
backendClients: [
  // scoped: acts as one user, under StagePlan's permissions
  trustWorkload({ issuer, audience, claims: { sub: "stage-plan-cli" }, as: () => ({ userId: "alice" }) }),
  // trusted: the system's authority, and for_user
  trustWorkload({ issuer, audience, claims: { sub: "stage-plan-ops" }, as: "backend" }),
],
```

```bash
export JAZZ_SERVER_URL="https://stage-plan.example/jazz"   # the main backend's /jazz
export JAZZ_APP_ID="<jazz.appId>"                           # the main backend's app id
export JAZZ_TOKEN_FILE=/path/to/token   # or JAZZ_TOKEN=<jwt>, or JAZZ_TOKEN_COMMAND='<prints a jwt>'

cargo run -p stage-plan-rust-cli -- show "Spring tour" --venue "The Roundhouse" --date 2026-05-01 --doors 19:00
cargo run -p stage-plan-rust-cli -- add <show-id> "Book the van"
cargo run -p stage-plan-rust-cli -- tasks <show-id>
cargo run -p stage-plan-rust-cli -- move <task-id> doing
```

The token file or command is read again for every token the client needs, so short-lived tokens work. Writes wait for the main backend to accept them, so a refused write is an error. The client's store is in memory, or in `STAGE_PLAN_DATA_DIR` if that is set.

To try it locally, `pnpm --filter rust-backend-clients main-backend ../stage-plan/apps/rust-cli` starts a stock `createJazz` serving StagePlan, with a stand-in OIDC issuer. It prints the env to export.

### `stage-plan gate`

`gate` checks the backend client end to end and prints one JSON line per step. It exits 0 only if every step is `"ok": true`.

- `STAGE_PLAN_MODE=scoped` (the default):
  - `open`, `write` (the user's show and a task), then `read` (a fresh client reads them from the server).
  - With `STAGE_PLAN_FOREIGN_SHOW=<id>`, it adds `foreign_read` (`"visible": false`) and `foreign_write` (`"refused": true`).
- `STAGE_PLAN_MODE=trusted`:
  - `open`, then `write` and `read` as the system.
  - `for_user_write` and `for_user_read` as `STAGE_PLAN_USER_A`.
  - `for_user_read` and `for_user_write` as `STAGE_PLAN_USER_B`, who must not see or add to A's show.
  - Both users need accounts (`jazz.createAccountFor`): `for_user` never creates one.

The open fails when the main backend trusts the token as the other kind.

## What the slice leaves out

- **Shows:** the TS apps also make the chief a `showCrew` member, write a first invite, and log moves in `activity`. The permissions let a show's chief work on its tasks without those rows, so the slice writes only shows and tasks.
- **Mergeable `tasks.notes`:** the column is in the bindings as `Option<String>`. Its text merge works through the core, but the slice doesn't edit notes.
- **Relations** become typed queries (`shows::tasks().of(show)`). Includes, which load related rows in one result, aren't supported by `jazz-app-support` yet.

## Tests

```bash
cargo test -p stage-plan-rust-cli
```

They run the slice against an in-process Jazz server, under StagePlan's permissions: one user plans a show, and another user sees none of its tasks. They also check that both copies equal the canonical files and that the bindings are current. The crate is a workspace member, so CI runs them.

[`../../../rust-backend-clients`](../../../rust-backend-clients) runs the binary against a real TypeScript main backend: `stage-plan gate`, scoped and trusted. CI runs it with the TypeScript tests (`pnpm --filter rust-backend-clients test`).
