# Docs snippets in Swift

The Swift snippets the docs show, over the docs todo app (`todos` and
`projects`): one function per `// #region <topic>-swift` in
`Sources/TodoDocs/DocsSnippets.swift`, written to
[`docs/lib/languages/SNIPPETS.md`](../../../docs/lib/languages/SNIPPETS.md).
`schema.ts` re-exports `../todo-client-localfirst-ts`'s; `npx jazz-tools generate`
writes `jazz.app.json`, `jazz.permissions.json` and `JazzApp.swift` from it.

The tests run every snippet as an end user signed in through a
`jazz-app-test-relay` in front of a `jazz-tools server`:

```bash
../../../packages/jazz-swift/scripts/build-ffi.sh --lib-only   # once, and after jazz-ffi changes
export PKG_CONFIG_PATH=../../../target/release/jazz-ffi-static
cargo build -p jazz-tools --bin jazz-tools
cargo build -p jazz-app-support --features test-relay --bin jazz-app-test-relay
swift test
```

`dev/gates/jazz-swift.sh` runs them in CI.
