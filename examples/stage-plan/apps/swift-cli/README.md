# StagePlan from Swift

A command-line tool over StagePlan's app, written in Swift. `jazz.app.json`
and `jazz.permissions.json` are byte-identical copies of
`../react-localfirst`'s (CI checks this: copy them, never edit them here), and
`JazzApp.swift` is what `jazz-tools generate` writes from them.

As the signed-in user (a backend acting for `--user`), it makes sure a show
exists with that user as its chief, lists the show's tasks by `rank`, adds
tasks and moves them between stages.

```bash
../../../../packages/jazz-swift/scripts/build-ffi.sh   # once, and after jazz-ffi changes
swift run stage-plan-swift demo
swift run stage-plan-swift --data-dir /tmp/stage-plan add "Load in"
swift run stage-plan-swift --data-dir /tmp/stage-plan list
swift run stage-plan-swift --data-dir /tmp/stage-plan move <task-id> doing
swift test                                              # the slice and the docs snippets
```

Left out because the Swift bindings don't support them yet: `tasks.notes`
(mergeable text), and relations and includes (tasks are queried by `showId`). The generated types are inside `JazzApp`: `JazzApp.Task`, `JazzApp.tasks.query()`..
