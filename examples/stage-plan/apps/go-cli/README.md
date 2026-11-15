# StagePlan: Go CLI

StagePlan's stage-prep board in a terminal, written in Go with
[`jazz-go`](../../../../packages/jazz-go). It's a backend that acts as one
user. It opens StagePlan's own `jazz.app.json` and `jazz.permissions.json`,
which are byte-identical copies of `../react-localfirst`'s, and a test checks
that they still match.

It syncs with a Jazz server, which the variables `JAZZ_SERVER_URL`,
`JAZZ_APP_ID`, `JAZZ_BACKEND_SECRET` and `JAZZ_ADMIN_SECRET` point at.

```sh
# from the repository root
cargo build -p jazz-ffi -p jazz-cli
export JAZZ_APP_ID=$(uuidgen) JAZZ_BACKEND_SECRET=dev-backend JAZZ_ADMIN_SECRET=dev-admin
export JAZZ_SERVER_URL=http://127.0.0.1:1625
target/debug/jazz-tools server "$JAZZ_APP_ID" --in-memory \
  --backend-secret "$JAZZ_BACKEND_SECRET" --admin-secret "$JAZZ_ADMIN_SECRET" &
cd examples/stage-plan/apps/go-cli
go run . show                    # your show; you're its chief (created if needed)
go run . add "Line check"        # add a task to To do
go run . tasks                   # the show's tasks by stage, ordered by rank
go run . move <task-id> doing    # todo, doing, done or blocked
go test ./...                    # the board flow and the docs snippets (starts its own server)
```

The flags `-user` (whom the CLI acts as) and `-data` (where rows are kept,
by default `.jazz`) go before the command.

`jazzapp/jazz_app.go` is generated: after changing `jazz.app.json`, run
`jazz-tools generate`. In CI, `jazz-tools generate --check` fails when it
is stale.

The example leaves out what the Go bindings don't support yet: `tasks.notes`
is a text-merge column, which the CLI never writes, and there are no
includes (relations), comments or activity feed. See
[Go Backend](https://jazz.tools/docs/setup/go#not-supported-yet).
