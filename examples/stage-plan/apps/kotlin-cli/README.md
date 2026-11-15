# StagePlan: Kotlin CLI

StagePlan as a Kotlin/JVM command-line app: an **end user** of StagePlan's backend, through the
backend's Jazz endpoint, over `jazz-kotlin`. (Kotlin is a client language: StagePlan's backend runs
in TypeScript, Rust or Go.) `jazz.app.json` and `jazz.permissions.json` are byte-identical copies
of `../react-localfirst`'s (CI checks it; never edit them here). `src/main/kotlin/JazzApp.kt` is what
jazz-codegen's Kotlin back end writes for them; regenerate it with
`JAZZ_UPDATE_FIXTURES=1 cargo test -p jazz-codegen --test golden the_stage_plan_kotlin_cli` (a golden
test checks it in CI) until Kotlin is switched on in `jazz-tools generate`.

Signed in with a device account (its key in `~/.stage-plan/device-key`), or with your backend's
bearer token, it creates a show (with the user as its chief) if there is none of that name, lists
its tasks by `rank`, adds a task, and moves a task between stages:

```bash
export JAZZ_ENDPOINT=https://stageplan.example/jazz JAZZ_APP_ID=<app id>
# export JAZZ_TOKEN=<your backend's token>   # to sign in as a backend user instead
gradle run --args='"Opening night" add Load in'
gradle run --args='"Opening night" move <task id> doing'
```

`gradle test` runs the slice and every docs snippet in `DocsSnippets.kt` against a StagePlan
backend started by the test: `crates/jazz-app/examples/test_app_backend.rs`, a **test harness** (a
Jazz server, the app's backend and a stand-in for its endpoint), not a backend to copy.
`dev/scripts/test-kotlin.sh` is the CI entry point.
