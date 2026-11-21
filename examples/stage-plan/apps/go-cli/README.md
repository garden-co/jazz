# StagePlan: Go CLI

> **Experimental:** client only, backends are TypeScript. A Go program is an app client
> (an end user) or a backend client of a TypeScript main backend (`createJazz`); Go servers
> come later. See [Language Support](https://jazz.tools/docs/reference/language-support).

StagePlan's stage-prep board in a terminal, written in Go with
[`jazz-go`](../../../../packages/jazz-go), as one of the app's users. It
signs in through the app's Jazz endpoint, its main backend's `/jazz`, with a
token from the app's auth provider, exactly as a browser client does. It
opens StagePlan's own `jazz.app.json` (and keeps `jazz.permissions.json`, the
main backend's): its `schema.ts` re-exports `../react-localfirst`'s, and
`npx jazz-tools generate` writes both files from it.

```sh
export JAZZ_SERVER_URL=https://my-stageplan.example/jazz   # the main backend's /jazz
export JAZZ_TOKEN=…                                        # the user's token
# JAZZ_APP_ID defaults to the id TypeScript clients derive from the URL
go run . show                    # your show; you're its chief (created if needed)
go run . add "Line check"        # add a task to To do
go run . tasks                   # the show's tasks by stage, ordered by rank
go run . move <task-id> doing    # todo, doing, done or blocked
```

`-data` (before the command) is where rows are kept, by default `.jazz`.

`go test ./...` runs the board flow, the docs snippets and every feature
against a real Jazz core (`jazz-tools server`) behind jazz-app-support's test relay,
the stand-in for a main backend's `/jazz`: from the repository root, build
them with `cargo build -p jazz-ffi -p jazz-cli` and
`cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay`.

`JAZZ_MAIN_BACKEND=ts go test ./...` runs the same suite with StagePlan's
real main backend in TypeScript as the server (`ts-main/jazz.server.ts`,
`createJazz` with a loopback OIDC issuer for its backend clients), plus
`TestBackendClient`: a trusted Go backend client acting for an end user
with `ForUser`, and a scoped one. It needs the repository's jazz-tools
build (`pnpm install`, then build jazz-tools) and runs from
`../react-localfirst` (`JAZZ_TS_MAIN_DIR`, `JAZZ_TSX` to override).

`jazzapp/jazz_app.go`, `jazz.app.json` and `jazz.permissions.json` are
generated from `schema.ts` by `npx jazz-tools generate`; in CI,
`jazz-tools generate --check` fails when any of them is stale.
