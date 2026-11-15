# StagePlan: Kotlin CLI

StagePlan as a Kotlin/JVM command-line backend over `jazz-kotlin`. `jazz.app.json` and
`jazz.permissions.json` are byte-identical copies of `../react-localfirst`'s (CI checks it; never
edit them here). `src/main/kotlin/JazzApp.kt` is what `jazz-tools generate` writes for them.

As the user `JAZZ_USER`, it creates a show (with them as its chief) if there is none of that
name, lists its tasks by `rank`, adds a task, and moves a task between stages:

```bash
cargo build -p jazz-ffi -p jazz-cli --bin jazz-tools          # from the repository root
../../../../target/debug/jazz-tools server my-app --in-memory --port 4200 \
  --backend-secret b --admin-secret a &
export JAZZ_SERVER_URL=http://127.0.0.1:4200 JAZZ_APP_ID=my-app \
  JAZZ_BACKEND_SECRET=b JAZZ_ADMIN_SECRET=a JAZZ_USER=alice
gradle run --args='"Opening night" add Load in'
gradle run --args='"Opening night" move <task id> doing'
```

`gradle test` runs the same slice, and every docs snippet in `DocsSnippets.kt`, against an
in-memory `jazz-tools server` (`dev/scripts/test-kotlin.sh` is the CI entry point). Android UI
apps are out of scope for .65.
