# StagePlan: Rust CLI

StagePlan from the command line, in Rust, with the `jazz-app` crate. It runs as the app's backend, acting as one signed-in user (`session.user` in the permissions). It can:

- create the user's show, as its crew chief;
- list a show's tasks by rank;
- add a task to a stage;
- move a task between stages (`todo`, `doing`, `done`, `blocked`).

## Files

- `jazz.app.json` and `jazz.permissions.json`: byte-identical copies of [`../react-localfirst`](../react-localfirst)'s. Never edit them here: copy them from there. The `app_files_are_stage_plans` test fails while they differ.
- `src/jazz_app.rs`: the typed bindings `jazz-tools generate` writes from `jazz.app.json`, committed. Don't edit it. The `bindings_are_current` test and `npx jazz-tools generate --check` refuse a stale copy.
- `src/lib.rs`: the slice: `ensure_show`, `tasks_of`, `add_task` and `move_task`.
- `src/main.rs`: the `stage-plan` CLI over it.

## Running it

```bash
# A Jazz server for the app, with its admin and backend secrets:
export JAZZ_APP_ID="<APP_UUID>" JAZZ_SERVER_URL="http://localhost:1625"
export JAZZ_ADMIN_SECRET="<ADMIN_SECRET>" JAZZ_BACKEND_SECRET="<BACKEND_SECRET>"
export STAGE_PLAN_USER=alice   # the user it acts as (default: stage-plan-cli)

cargo run -p stage-plan-rust-cli -- show "Spring tour" --venue "The Roundhouse" --date 2026-05-01 --doors 19:00
cargo run -p stage-plan-rust-cli -- add <show-id> "Book the van"
cargo run -p stage-plan-rust-cli -- tasks <show-id>
cargo run -p stage-plan-rust-cli -- move <task-id> doing
```

On start it publishes the app's permissions bundle and admits the user it acts as. Its local store is in `STAGE_PLAN_DATA_DIR` (default `./stage-plan-data`).

## What the slice leaves out

- **Shows:** the TS apps also make the chief a `showCrew` member, write a first invite, and log moves in `activity`. The permissions let a show's chief work on its tasks without those rows, so the slice writes only shows and tasks.
- **Mergeable `tasks.notes`:** the column is in the bindings as `Option<String>`. Its text merge works through the core, but the slice doesn't edit notes.
- **Relations** become typed queries (`shows::tasks().of(show)`). Includes, which load related rows in one result, aren't supported by `jazz-app` yet.

## Tests

```bash
cargo test -p stage-plan-rust-cli
```

They run the slice against an in-process Jazz server, under StagePlan's permissions: one user plans a show, and another user sees none of its tasks. They also check that both copies equal the canonical files and that the bindings are current. The crate is a workspace member, so CI runs them.
