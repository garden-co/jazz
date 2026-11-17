# StagePlan: Kotlin CLI

> **Experimental:** client only, backends are TypeScript. See [Language Support](https://jazz.tools/docs/reference/language-support#jazz-environment-kotlin).

StagePlan as a Kotlin/JVM command-line app: an **end user** of StagePlan's backend, through the
backend's Jazz endpoint, over `jazz-kotlin`. (Kotlin is for apps: StagePlan's main backend runs in TypeScript.) Its `schema.ts` re-exports
`../react-localfirst`'s, and `npx jazz-tools generate` writes `jazz.app.json` and
`jazz.permissions.json` from it (CI checks they're current). `src/main/kotlin/JazzApp.kt` is what the same
`npx jazz-tools generate` writes for them (and `generate --check` checks).

Signed in with a device account (its key in `~/.stage-plan/device-key`), or with your backend's
bearer token, it creates a show (with the user as its chief) if there is none of that name, lists
its tasks by `rank`, adds a task, and moves a task between stages:

```bash
gradle installDist        # JDK 17+, Gradle 8.14; builds jazz-ffi with cargo
cli=build/install/stage-plan-kotlin-cli/bin/stage-plan-kotlin-cli
export JAZZ_ENDPOINT=https://stageplan.example/jazz JAZZ_APP_ID=<app id>
export JAZZ_TOKEN=<your backend's bearer token>   # or leave unset: a device account
$cli "Opening night" add Load in      # prints "added <task id>", then the show and its tasks
$cli "Opening night" move <task id> doing
$cli --show <show id> tasks           # any show by id, if this user may read it
```

Writes wait for the server (`WriteTier.Global`): a write its permissions refuse prints
`refused: …` and exits 4. A show this user may not read prints `no show <id> visible to you` and
exits 3. Each line of a listing is `  <task id>  [<status>] <title>` under `<show name> (<show id>)`.

`gradle test` runs the slice and every docs snippet in `DocsSnippets.kt` against a StagePlan
backend the test starts, a **test harness** and not a backend to copy: `jazz-tools server` and
`jazz-app-test-relay`, which publishes StagePlan's permissions and stands in for its Jazz endpoint.
`dev/scripts/test-kotlin.sh` is the CI entry point.
