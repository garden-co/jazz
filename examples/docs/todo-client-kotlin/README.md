# Docs todo app: Kotlin client snippets

> **Experimental:** client only. See [Language Support](https://jazz.tools/docs/reference/language-support#jazz-environment-kotlin).

The Kotlin snippets of the docs' reading and writing pages, for the docs todo app (`todos` and
`projects`), over `jazz-kotlin`. Each region of `src/main/kotlin/todoclient/DocsSnippets.kt` is one
function, as the other languages' are (`docs/lib/languages/SNIPPETS.md`).

`schema.ts` re-exports `../todo-client-localfirst-ts`'s, and `npx jazz-tools generate` writes
`jazz.app.json`, `jazz.permissions.json` and `src/main/kotlin/JazzApp.kt` from it (CI checks they're
current).

`gradle test` (JDK 17+, Gradle 8.14) runs every snippet against a backend the test starts, a **test
harness** and not a backend to copy: `jazz-tools server` and `jazz-app-test-relay`, which publishes
the app's permissions and stands in for its Jazz endpoint. `dev/scripts/test-kotlin.sh` is the CI
entry point.
