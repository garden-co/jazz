# jazz-kotlin

The Kotlin/JVM runtime for Jazz apps: `suspend` calls and `Flow` subscriptions over
[`jazz-ffi`](../../crates/jazz-ffi) (UniFFI, through JNA), for the typed bindings
jazz-codegen's Kotlin back end writes to `src/main/kotlin/JazzApp.kt` beside a `build.gradle.kts`
(through `jazz-tools generate` once Kotlin is switched on in the CLI).

```kotlin
val jazz = jazzapp.open(JazzConfig(appId = …, serverUrl = …, permissions = …, backendSecret = …, adminSecret = …, sessionSubject = "user-1"))
val tasks = jazz.all(Tasks.where(Tasks.showId eq show.id).orderBy(Tasks.rank.asc()))
jazz.update(Tasks, task.id) { status(TaskStatus.Done) }
jazz.watch(Tasks.where(Tasks.showId eq show.id)).collect { println(it) }
```

`gradle build` builds `crates/jazz-ffi` with cargo and generates its Kotlin bindings with
`uniffi-bindgen` first; `-PjazzFfi=skip` builds without them (the tests use a fake native core).
JVM only for .65: Android and Kotlin Multiplatform aren't supported yet. See
`docs/content/docs/setup/kotlin.mdx`.
