# StagePlan from Swift

> **Experimental: client only, backends are TypeScript.** Jazz for Swift is
> an end-user client; StagePlan's backend runs in TypeScript. See
> `packages/jazz-swift/README.md` for what Swift supports and doesn't yet, and
> [Language Support](https://jazz.tools/docs/reference/language-support).

A command-line tool over StagePlan's app, written in Swift. Its `schema.ts`
re-exports `../react-localfirst`'s, `npx jazz-tools generate` writes
`jazz.app.json` and `jazz.permissions.json` from it, and `JazzApp.swift` is
what `jazz-tools generate` writes from those.

An end-user client of StagePlan (Swift apps are clients; StagePlan's backend
runs in TypeScript). It signs in to the app's Jazz endpoint
(`JAZZ_ENDPOINT`, the backend's `createJazz` handler, e.g.
`http://127.0.0.1:8780/jazz`; `JAZZ_APP_ID`) with a device account, whose
key it keeps in `--key-file` (made on first use, mode 0600), or with a bearer
token from `$JAZZ_BEARER` (or `--bearer-stdin`). As that user it makes sure a
show exists with them as its chief (`--show <name>`) or targets one by id
(`--show-id <id>`), lists the show's tasks by `rank`, adds tasks and moves
them between stages.

This example lives in the Jazz repository and depends on the Swift package
from source (`.package(name: "jazz", path: "../../../..")`), so it builds the
Jazz core first. Your app depends on the published package instead,
`.package(url: "https://github.com/garden-co/jazz-swift", exact: "2.0.0-alpha.66")`,
which brings the core prebuilt on macOS and iOS (see the Swift setup page).

```bash
../../../../packages/jazz-swift/scripts/build-ffi.sh --lib-only   # once, and after jazz-ffi changes
export PKG_CONFIG_PATH=../../../../target/release/jazz-ffi-static
export JAZZ_ENDPOINT=http://127.0.0.1:8780/jazz JAZZ_APP_ID=<the app's id>
swift run stage-plan-swift --key-file /tmp/alice.key demo
swift run stage-plan-swift --key-file /tmp/alice.key add "Load in"    # prints the task's id, then "show <show id>"
swift run stage-plan-swift --key-file /tmp/alice.key list             # <id>  todo    Load in
swift run stage-plan-swift --key-file /tmp/alice.key move <task-id> doing
swift run stage-plan-swift --key-file /tmp/bob.key --show-id <alice's show id> list   # nothing
swift run stage-plan-swift --key-file /tmp/bob.key --show-id <alice's show id> add x  # exit 1: permission denied
swift test   # the slice and the docs snippets, through jazz-app-test-relay
```
